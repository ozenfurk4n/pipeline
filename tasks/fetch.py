

import pandas as pd
import requests
import time
from typing import Dict, Any, List, Optional

from prefect import task, get_run_logger

# Kendi veritabanı erişim modülümüzü ve loglama fonksiyonlarımızı import ediyoruz.
import DB_postgre.DB_postgre as DB
from utils.utils import log_task_start, log_task_success, log_task_error, tr_upper_ascii

# --- YENİ IMPORT ---
# Otomatik IP değiştirme fonksiyonunu import ediyoruz.
from utils.vpn_manager import switch_ip

# ==========================================================================
# === BÖLÜM 1: TKGM API İLETİŞİMİ - GÜNCELLENDİ
# ==========================================================================

URL_IL   = "https://parselsorgu.tkgm.gov.tr/app/modules/administrativeQuery/data/ilListe.json"
URL_ILCE = "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/ilceListe/{il_id}"
URL_MAH  = "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/mahalleListe/{ilce_id}"

# Geometri çekme için yeni URL'ler
URL_MAH_GEOM = "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/mahalleGeometri/{mahalle_id}"
URL_MAH_BOUNDS = "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/mahalleSinir/{mahalle_id}"

TIMEOUT = 30
SLEEP_BETWEEN = 0.2
# Deneme sayısını artıralım, çünkü bazı denemeler IP değiştirmek için kullanılacak.
MAX_ATTEMPTS = 8

def get_json(url: str) -> Dict:
    """
    API'den JSON verisi çeker. Artık IP bloklaması veya bağlantı hatası
    tespit ettiğinde otomatik olarak IP değiştirir ve tekrar dener.
    """
    logger = get_run_logger()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://parselsorgu.tkgm.gov.tr/",
    }
    last_err = None
    
    for i in range(MAX_ATTEMPTS):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)

            # --- YENİ MANTIK: IP BLOKLAMASI TESPİTİ ---
            if r.status_code in {403, 429}:
                logger.warning(f"IP bloklaması tespit edildi (Status: {r.status_code}). IP değiştiriliyor... (Deneme {i+1}/{MAX_ATTEMPTS})")
                switch_ip()
                continue # Bu deneme başarısız oldu, döngünün bir sonraki adımında yeni IP ile tekrar dene.

            r.raise_for_status() # Diğer HTTP hatalarını (404, 500 vb.) yakala
            return r.json()

        # --- YENİ MANTIK: BAĞLANTI HATASI TESPİTİ ---
        except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logger.warning(f"Bağlantı hatası tespit edildi ({type(e).__name__}). IP değiştiriliyor... (Deneme {i+1}/{MAX_ATTEMPTS})")
            switch_ip()
            continue # Bu deneme de başarısız oldu, tekrar dene.

        except Exception as e:
            last_err = e
            logger.error(f"Beklenmedik bir hata oluştu: {e}. Kısa bir süre bekleniyor...")
            time.sleep(SLEEP_BETWEEN * (i + 1)) # Hata durumunda bekleme süresini artır
    
    # Eğer tüm denemeler başarısız olursa, görevin hata vermesini sağla.
    raise RuntimeError(f"GET {url} tüm denemelere rağmen başarısız: {last_err}")

# ==========================================================================
# === BÖLÜM 2: GEOMETRİ ÇEKME FONKSİYONLARI
# ==========================================================================

def fetch_mahalle_geometry(mahalle_id: int) -> Optional[Dict[str, Any]]:
    """Belirtilen mahalle ID için geometri verisini çeker."""
    logger = get_run_logger()
    
    # YENİ VE GÜVENİLİR MANTIK:
    # Mahalle geometrisini çekmek için daha güvenilir yöntemler kullanıyoruz
    
    # 1. Önce ana geometri endpoint'ini dene
    try:
        geom_data = get_json(URL_MAH_GEOM.format(mahalle_id=mahalle_id))
        if geom_data and "geometry" in geom_data:
            return geom_data
    except Exception as e:
        logger.warning(f"Mahalle {mahalle_id} için ana geometri endpoint'i başarısız: {e}")
    
    # 2. Alternatif olarak sınır endpoint'ini dene
    try:
        bounds_data = get_json(URL_MAH_BOUNDS.format(mahalle_id=mahalle_id))
        if bounds_data and "geometry" in bounds_data:
            return bounds_data
    except Exception as e:
        logger.warning(f"Mahalle {mahalle_id} için sınır endpoint'i başarısız: {e}")
    
    # 3. Son çare olarak, mahalle listesi endpoint'inden geometri bilgisini almaya çalış
    try:
        # Mahalle listesi endpoint'i genellikle daha güvenilir
        mahalle_liste_url = f"https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/mahalleListe/{mahalle_id}"
        mahalle_data = get_json(mahalle_liste_url)
        
        if mahalle_data and "features" in mahalle_data:
            for feature in mahalle_data["features"]:
                if feature.get("properties", {}).get("id") == mahalle_id:
                    if "geometry" in feature:
                        return feature
    except Exception as e:
        logger.warning(f"Mahalle {mahalle_id} için liste endpoint'i başarısız: {e}")
    
    return None

def convert_geometry_to_wkb(geometry_data: Dict[str, Any]) -> Optional[str]:
    """GeoJSON geometrisini WKB (Well-Known Binary) formatına çevirir."""
    try:
        from shapely.geometry import shape
        from shapely.wkb import dumps
        
        # GeoJSON'dan Shapely geometrisi oluştur
        geom = shape(geometry_data["geometry"])
        
        # WKB formatına çevir
        wkb_bytes = dumps(geom)
        
        # Hex string'e çevir
        wkb_hex = wkb_bytes.hex()
        
        return wkb_hex
        
    except ImportError:
        logger = get_run_logger()
        logger.error("Shapely kütüphanesi bulunamadı. Geometri dönüşümü yapılamıyor.")
        return None
    except Exception as e:
        logger = get_run_logger()
        logger.error(f"Geometri dönüşümü sırasında hata: {e}")
        return None

# ==========================================================================
# === BÖLÜM 3: PREFECT GÖREVİ (TASK)
# ==========================================================================

@task(name="TKGM İdari Veri Çekme Görevi", retries=2, retry_delay_seconds=120)
def fetch_tkgm_data_task(il_name: str, ilce_name: Optional[str] = None) -> Dict[str, Any]:
    logger = get_run_logger()
    log_id = None
    try:
        log_id = log_task_start(script_name="fetch.py", params={"il_name": il_name})
        logger.info(f"'{il_name}' için TKGM veri çekme görevi başladı. Log ID: {log_id}")

        if DB.tkgm_has_il(il_name):
            success_message = f"'{il_name}' için TKGM verisi zaten mevcut. API isteği atlanıyor."
            logger.info(success_message)
            log_task_success(log_id, success_message)
            return {"status": "SKIPPED", "reason": "Veri zaten var."}

        logger.info(f"'{il_name}' için yerel veri bulunamadı. TKGM API'den çekilecek.")
        j_il = get_json(URL_IL)
        il_map = {tr_upper_ascii(f["properties"]["text"]): int(f["properties"]["id"]) for f in j_il.get("features", [])}
        key = tr_upper_ascii(il_name)
        if key not in il_map:
            raise ValueError(f"TKGM il listesinde '{il_name}' bulunamadı.")
        il_id = il_map[key]

        j_ilce = get_json(URL_ILCE.format(il_id=il_id))
        ilceler = [{"ilce_id": int(p["id"]), "ilce_ad": p["text"]} for f in j_ilce.get("features", []) if (p := f.get("properties"))]

        # İlçe filtresi uygula
        if ilce_name:
            ilce_key = tr_upper_ascii(ilce_name)
            ilceler = [ic for ic in ilceler if tr_upper_ascii(ic["ilce_ad"]) == ilce_key]
            if not ilceler:
                raise ValueError(f"'{il_name}' ilinde '{ilce_name}' ilçesi bulunamadı.")
            logger.info(f"'{ilce_name}' ilçesi için filtrelendi. {len(ilceler)} ilçe bulundu.")

        rows: List[Dict[str, Any]] = []
        geom_rows: List[Dict[str, Any]] = []
        
        for ic in ilceler:
            time.sleep(SLEEP_BETWEEN)
            j_mah = get_json(URL_MAH.format(ilce_id=ic["ilce_id"]))
            
            # İlçe bazında geometri verilerini toplu çek
            ilce_geom_rows = []
            
            for mf in j_mah.get("features", []) or []:
                pp = mf.get("properties", {}) or {}
                if "id" in pp and "text" in pp:
                    mahalle_id = int(pp["id"])
                    
                    # Ana veri satırı
                    rows.append({
                        "il_id": il_id, "il_ad": il_name,
                        "ilce_id": int(ic["ilce_id"]), "ilce_ad": ic["ilce_ad"],
                        "mahalle_id": mahalle_id, "mahalle_ad": pp["text"],
                    })
                    
                    # Geometri verisi için hazırlık (henüz çekme)
                    ilce_geom_rows.append({
                        "ilce_id": int(ic["ilce_id"]),
                        "mahalle_id": mahalle_id,
                        "mahalle_ad": pp["text"]
                    })
            
            # İlçe bazında toplu geometri çekimi
            if ilce_geom_rows:
                logger.info(f"İlçe {ic['ilce_ad']} için {len(ilce_geom_rows)} mahalle geometrisi çekiliyor...")
                
                # İlçe mahalle listesi endpoint'inden geometri bilgilerini al
                try:
                    ilce_mahalle_url = f"https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/mahalleListe/{ic['ilce_id']}"
                    ilce_mahalle_data = get_json(ilce_mahalle_url)
                    
                    if ilce_mahalle_data and "features" in ilce_mahalle_data:
                        for feature in ilce_mahalle_data["features"]:
                            if "geometry" in feature and "properties" in feature:
                                props = feature["properties"]
                                mahalle_id = props.get("id") or props.get("mahalleId")
                                
                                if mahalle_id:
                                    # Geometri verisini WKB'ye çevir
                                    wkb_hex = convert_geometry_to_wkb(feature)
                                    if wkb_hex:
                                        # İlçe geometri listesinde bul ve güncelle
                                        for geom_row in ilce_geom_rows:
                                            if geom_row["mahalle_id"] == mahalle_id:
                                                geom_row["geom"] = wkb_hex
                                                break
                    
                    # Başarılı geometri çekimlerini ana listeye ekle
                    for geom_row in ilce_geom_rows:
                        if "geom" in geom_row:
                            geom_rows.append(geom_row)
                            logger.info(f"Mahalle {geom_row['mahalle_id']} geometrisi başarıyla çekildi")
                        else:
                            logger.warning(f"Mahalle {geom_row['mahalle_id']} için geometri verisi bulunamadı")
                            
                except Exception as e:
                    logger.error(f"İlçe {ic['ilce_ad']} geometri çekimi sırasında hata: {e}")
                    # Hata durumunda sadece ana veri ile devam et

        if not rows:
            raise RuntimeError(f"'{il_name}' için TKGM'den hiç mahalle verisi alınamadı.")

        # Ana veri tablosuna yaz
        df = pd.DataFrame(rows)
        DB.wipe_tkgm_by_ilid(il_id)
        DB.insert_tkgm_rows(df)
        
        # Geometri verilerini yaz (eğer varsa)
        if geom_rows:
            logger.info(f"{len(geom_rows)} adet mahalle geometrisi bulundu. Geometri tablosuna yazılıyor...")
            df_geom = pd.DataFrame(geom_rows)
            DB.insert_mahalle_geometries(df_geom)
        else:
            logger.warning("Hiç geometri verisi bulunamadı.")
        
        success_message = f"TKGM verileri başarıyla çekildi ve güncellendi: {il_name} → {len(df)} satır."
        log_task_success(log_id, success_message)
        logger.info(success_message)
        
        return {"status": "SUCCESS", "il": il_name, "fetched_rows": len(df)}

    except Exception as e:
        error_message = f"Veri çekme görevi sırasında hata oluştu: {e}"
        logger.error(error_message, exc_info=True)
        if log_id:
            log_task_error(log_id, str(e))
        raise