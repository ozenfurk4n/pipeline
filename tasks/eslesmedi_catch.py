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

# Kendi veritabanı erişim modülümüzü import ediyoruz.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB
# Merkezi yardımcı modülümüzü import ediyoruz.
from utils.log_utils import (
    log_task_start, log_task_success, log_task_error
)

# ==========================================================================
# === BÖLÜM 1: YARDIMCI FONKSİYONLAR
# ==========================================================================

def coerce_int(x: Any) -> Optional[int]:
    """Güvenli bir şekilde değeri integer'a çevirir."""
    if x is None or (isinstance(x, float) and math.isnan(x)): 
        return None
    if isinstance(x, int): 
        return x
    s = str(x).strip().replace(",", "")
    if s.endswith(".0"): 
        s = s[:-2]
    try:
        return int(s) if re.fullmatch(r"\d+", s) else None
    except (ValueError, TypeError):
        return None

# ==========================================================================
# === BÖLÜM 2: API İLETİŞİMİ VE GEOMETRİ MANTIĞI
# ==========================================================================

API_PARCEL_BASES = [
    "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3.1/api/parsel",
    "https://cbsapi.tkgm.gov.tr/megsiswebapi.v3/api/parsel",
]
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Referer": "https://parselsorgu.tkgm.gov.tr/",
}

def fetch_parcel_from_tkgm(mahalle_id: int, ada: int, parsel: int) -> Optional[Dict[str, Any]]:
    logger = get_run_logger()
    for base in API_PARCEL_BASES:
        url = f"{base}/{int(mahalle_id)}/{int(ada)}/{int(parsel)}"
        try:
            r = requests.get(url, headers=HTTP_HEADERS, timeout=25)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException as e:
            logger.error(f"TKGM API isteği sırasında ağ hatası: {e}")
            time.sleep(1)
    return None

def find_mahalle_by_geometry(polygon_hex: str, ilce_id: int) -> Optional[Tuple[int, str]]:
    logger = get_run_logger()
    if not polygon_hex or not ilce_id: return None
    q = text(f"""
        WITH input_geom AS (SELECT ST_SetSRID(ST_GeomFromWKB(:wkb_hex), 4326) AS geom)
        SELECT m.mahalle_id, m.mahalle_ad
        FROM {DB.T_MAH_GEOM} m, input_geom
        WHERE m.ilce_id = :ilce_id AND ST_Intersects(m.geom, input_geom.geom)
        ORDER BY ST_Area(ST_Intersection(m.geom, input_geom.geom)) DESC
        LIMIT 1;
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

# ==========================================================================
# === BÖLÜM 2: PREFECT GÖREVİ (TASK) - GÜNCELLENDİ
# ==========================================================================

@task(name="Eşleşmeyenleri Geometri ile Kurtarma Görevi", retries=2, retry_delay_seconds=120)
def process_unmatched_task(filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Belirtilen filtrelere uyan eşleşmemiş kayıtları geometri ile kurtarmayı dener.
    """
    logger = get_run_logger()
    log_id = None

    try:
        log_id = log_task_start(script_name="eslesmedi_catch.py", params=filters)
        logger.info(f"Eşleşmeyenleri kurtarma görevi başladı. Filtreler: {filters}. Log ID: {log_id}")

        # Veritabanından verileri il adına göre çekiyoruz.
        il_name = filters.get("il")
        if not il_name:
            raise ValueError("Filtrelerde 'il' bilgisi bulunamadı.")
        df_miss = DB.load_unmatched_for_il(il_name)
        
        if df_miss.empty:
            success_message = "Filtrelerle eşleşen işlenecek kayıt bulunamadı."
            logger.info(success_message)
            log_task_success(log_id, success_message)
            return {"status": "SKIPPED", "reason": success_message}

        logger.info(f"İşlenecek {len(df_miss)} adet eşleşmeyen kayıt bulundu.")
        kurtarilan_kayitlar = []

        for index, row in df_miss.iterrows():
            polygon_hex = row.get("polygon_hex")
            ilce_id = coerce_int(row.get("ilce_id"))
            ada = coerce_int(row.get("AdaBilgisi"))
            parsel = coerce_int(row.get("ParselBilgisi"))

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
                    clean_hex = str(polygon_hex).strip().replace("\\x", "").replace("0x", "")
                    try: input_wkb_bytes = bytes.fromhex(clean_hex)
                    except Exception: pass
                
                yeni_kayit = {
                    "orig_id": row.get("Id"), "il": row.get("IlBilgisi"), "ilce": row.get("IlceBilgisi"),
                    "mahalle_txt": yeni_mahalle_ad, "mahalle_id": yeni_mahalle_id,
                    "ada": ada, "parsel": parsel,
                    "tkgm_properties": json.dumps(tkgm_feature.get("properties", {})),
                    "tkgm_geometry": json.dumps(tkgm_feature.get("geometry", {})),
                    "input_polygon_wkb": input_wkb_bytes,
                }
                kurtarilan_kayitlar.append(yeni_kayit)

        if kurtarilan_kayitlar:
            df_ok = pd.DataFrame(kurtarilan_kayitlar)
            DB.insert_poly_ok(df_ok)
        
        success_message = f"Görev tamamlandı. {len(df_miss)} kayıt işlendi, {len(kurtarilan_kayitlar)} kayıt kurtarıldı."
        log_task_success(log_id, success_message)
        logger.info(success_message)

        return {"status": "SUCCESS", "filters": filters, "processed_count": len(df_miss), "rescued_count": len(kurtarilan_kayitlar)}

    except Exception as e:
        error_message = f"Görev sırasında beklenmedik bir hata oluştu: {e}"
        logger.error(error_message, exc_info=True)
        if log_id:
            log_task_error(log_id, str(e))
        raise
