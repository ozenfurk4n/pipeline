# tasks/eslesmedi_catch.py

import pandas as pd
import requests
import time
import json
import re
import math
from typing import Any, Dict, List, Optional, Tuple

from prefect import task, get_run_logger
from sqlalchemy import text

# Gerekli importları düzenliyoruz
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB
from utils.utils import log_task_start, log_task_success, log_task_error, coerce_int

# ==========================================================================
# === BÖLÜM 1: API İLETİŞİMİ VE GEOMETRİ MANTIĞI - GÜNCELLENDİ
# ==========================================================================

# Sadece ihtiyaç duyduğumuz API endpoint'lerini bırakıyoruz.
URL_MAH_LISTE = "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/idariYapi/mahalleListe/{ilce_id}"
URL_PARSEL = "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/parsel/{mahalle_id}/{ada}/{parsel}"

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Referer": "https://parselsorgu.tkgm.gov.tr/",
}

def http_json(url: str) -> Dict[str, Any]:
    """Basit bir HTTP GET isteği yapar ve JSON döndürür."""
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        get_run_logger().error(f"HTTP isteği başarısız: {url} - Hata: {e}")
        raise

def ensure_mahalle_geoms_for_ilce(ilce_id: int):
    """
    YENİ VE GÜVENİLİR MANTIK:
    Bir ilçedeki TÜM mahallelerin geometrilerini tek bir API isteği ile çeker
    ve veritabanındaki önbellek (cache) tablosuna yazar.
    Bu yöntem, 404 hatalarını tamamen ortadan kaldırır.
    """
    logger = get_run_logger()
    logger.info(f"İlçe ID {ilce_id} için mahalle geometrileri kontrol ediliyor/çekiliyor...")
    
    try:
        # Tek ve güvenilir endpoint'i çağır
        mahalle_liste_data = http_json(URL_MAH_LISTE.format(ilce_id=ilce_id))
        features = mahalle_liste_data.get("features", [])
        if not features:
            logger.warning(f"İlçe ID {ilce_id} için hiç mahalle bulunamadı.")
            return

        eng = DB.engine()
        with eng.begin() as con:
            for f in features:
                props = (f or {}).get("properties") or {}
                geom  = (f or {}).get("geometry") or {}
                mh_id = coerce_int(props.get("mahalleId") or props.get("id"))
                mh_ad = props.get("mahalleAd") or props.get("text") or ""
                
                # Eğer geometri verisi varsa, veritabanına kaydet (UPSERT)
                if mh_id and geom and geom.get("coordinates"):
                    geojson = json.dumps(geom, ensure_ascii=False)
                    sql = text(f"""
                        INSERT INTO {DB.T_MAH_GEOM} (ilce_id, mahalle_id, mahalle_ad, geom)
                        VALUES (:ilce_id, :mahalle_id, :mahalle_ad, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(:geojson), 4326)))
                        ON CONFLICT (ilce_id, mahalle_id) DO UPDATE
                        SET mahalle_ad = EXCLUDED.mahalle_ad, geom = EXCLUDED.geom;
                    """)
                    con.execute(sql, {"ilce_id": ilce_id, "mahalle_id": mh_id, "mahalle_ad": mh_ad, "geojson": geojson})
        logger.info(f"İlçe ID {ilce_id} için {len(features)} mahalle geometrisi başarıyla işlendi.")
    except Exception as e:
        logger.error(f"İlçe ID {ilce_id} için mahalle geometrileri çekilirken hata oluştu: {e}")


def find_mahalle_by_geometry(polygon_hex: str, ilce_id: int) -> Optional[Tuple[int, str]]:
    logger = get_run_logger()
    if not polygon_hex or not ilce_id: return None
    q = text(f"""
        WITH input_geom AS (SELECT ST_SetSRID(ST_GeomFromWKB(:wkb_hex), 4326) AS geom)
        SELECT m.mahalle_id, m.mahalle_ad FROM {DB.T_MAH_GEOM} m, input_geom
        WHERE m.ilce_id = :ilce_id AND ST_Intersects(m.geom, input_geom.geom)
        ORDER BY ST_Area(ST_Intersection(m.geom, input_geom.geom)) DESC LIMIT 1;
    """)
    try:
        eng = DB.engine()
        with eng.connect() as con:
            result = con.execute(q, {"wkb_hex": polygon_hex, "ilce_id": ilce_id}).fetchone()
        if result:
            return int(result[0]), str(result[1])
    except Exception as e:
        logger.error(f"PostGIS sorgusu sırasında hata oluştu: {e}")
    return None

def fetch_parcel_from_tkgm(mahalle_id: int, ada: int, parsel: int) -> Optional[Dict[str, Any]]:
    try:
        return http_json(URL_PARSEL.format(mahalle_id=mahalle_id, ada=ada, parsel=parsel))
    except Exception:
        return None

# ==========================================================================
# === BÖLÜM 2: PREFECT GÖREVİ (TASK)
# ==========================================================================

@task(name="Eşleşmeyenleri Geometri ile Kurtarma Görevi", retries=2, retry_delay_seconds=120)
def process_unmatched_task(filters: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    log_id = None
    try:
        log_id = log_task_start(script_name="eslesmedi_catch.py", params=filters)
        logger.info(f"Eşleşmeyenleri kurtarma görevi başladı. Filtreler: {filters}. Log ID: {log_id}")

        # filters sözlüğünden il adını çıkar
        il_name = filters.get("il")
        if not il_name:
            raise ValueError("Filtrelerde 'il' parametresi bulunamadı.")
        
        df_miss = DB.load_unmatched_for_il(il_name)
        if df_miss.empty:
            success_message = "Filtrelerle eşleşen işlenecek kayıt bulunamadı."
            log_task_success(log_id, success_message)
            return {"status": "SKIPPED", "reason": success_message}

        # Geometri verilerini önbelleğe almak için ilçeleri grupla
        processed_ilce_ids = set()
        for ilce_id in df_miss['ilce_id'].dropna().unique():
            ensure_mahalle_geoms_for_ilce(coerce_int(ilce_id))
            processed_ilce_ids.add(coerce_int(ilce_id))

        kurtarilan_kayitlar = []
        for index, row in df_miss.iterrows():
            polygon_hex, ilce_id = row.get("polygon_hex"), coerce_int(row.get("ilce_id"))
            ada, parsel = coerce_int(row.get("AdaBilgisi")), coerce_int(row.get("ParselBilgisi"))

            if not all([polygon_hex, ilce_id, ada is not None, parsel is not None]):
                continue

            found_mahalle = find_mahalle_by_geometry(polygon_hex, ilce_id)
            if not found_mahalle:
                continue
                
            yeni_mahalle_id, yeni_mahalle_ad = found_mahalle
            tkgm_feature = fetch_parcel_from_tkgm(yeni_mahalle_id, ada, parsel)

            if tkgm_feature and isinstance(tkgm_feature, dict):
                input_wkb_bytes = None
                if polygon_hex:
                    try: input_wkb_bytes = bytes.fromhex(str(polygon_hex).strip().replace("\\x", "").replace("0x", ""))
                    except Exception: pass
                
                kurtarilan_kayitlar.append({
                    "orig_id": row.get("Id"), "il": row.get("IlBilgisi"), "ilce": row.get("IlceBilgisi"),
                    "mahalle_txt": yeni_mahalle_ad, "mahalle_id": yeni_mahalle_id,
                    "ada": ada, "parsel": parsel,
                    "tkgm_properties": json.dumps(tkgm_feature.get("properties", {})),
                    "tkgm_geometry": json.dumps(tkgm_feature.get("geometry", {})),
                    "input_polygon_wkb": input_wkb_bytes,
                })

        if kurtarilan_kayitlar:
            DB.insert_poly_ok(pd.DataFrame(kurtarilan_kayitlar))
        
        success_message = f"Görev tamamlandı. {len(df_miss)} kayıt işlendi, {len(kurtarilan_kayitlar)} kayıt kurtarıldı."
        log_task_success(log_id, success_message)
        return {"status": "SUCCESS", "filters": filters, "processed_count": len(df_miss), "rescued_count": len(kurtarilan_kayitlar)}

    except Exception as e:
        error_message = f"Görev sırasında beklenmedik bir hata oluştu: {e}"
        if log_id: log_task_error(log_id, str(e))
        raise
