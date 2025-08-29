# tasks/1n_funct.py

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
from utils.utils import (
    log_task_start, log_task_success, log_task_error, coerce_int
)

# ==========================================================================
# === BÖLÜM 1: YARDIMCI FONKSİYONLAR
# ==========================================================================

# coerce_int fonksiyonu artık utils.utils modülünden import ediliyor

# ==========================================================================
# === BÖLÜM 2: API İLETİŞİMİ VE ÖZYİNELEMELİ TAKİP
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
            if r.status_code == 200: return r.json()
        except requests.RequestException as e:
            logger.error(f"TKGM API isteği sırasında ağ hatası: {e}")
            time.sleep(1)
    return None

def parse_gittigi_list(feature: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not feature: return []
    props = feature.get("properties", {}) or {}
    gittigi_str = props.get("gittigiParselListe")
    if not gittigi_str: return []
    try:
        data = json.loads(gittigi_str) if isinstance(gittigi_str, str) else gittigi_str
        return data.get("features", []) or []
    except (json.JSONDecodeError, TypeError):
        return []

def collect_active_features_recursive(
    root_feature: Dict[str, Any], visited_ids: set, max_depth: int = 10
) -> List[Dict[str, Any]]:
    if max_depth <= 0: return []
    props = (root_feature or {}).get("properties", {}) or {}
    current_id = props.get("id")
    if current_id in visited_ids: return []
    visited_ids.add(current_id)
    child_features = parse_gittigi_list(root_feature)
    if not child_features:
        return [root_feature] if str(props.get("durum", "")).strip() == "1" else []
    final_features = []
    for child in child_features:
        final_features.extend(collect_active_features_recursive(child, visited_ids, max_depth - 1))
    return final_features

# ==========================================================================
# === BÖLÜM 2: PREFECT GÖREVİ (TASK) - GÜNCELLENDİ
# ==========================================================================

@task(name="Parsel Evrim (1-N) Analizi Görevi", retries=2, retry_delay_seconds=180)
def analyze_1n_task(filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Belirtilen filtrelere uyan eşleşmiş kayıtların zaman içindeki değişimini analiz eder.
    """
    logger = get_run_logger()
    log_id = None

    try:
        log_id = log_task_start(script_name="1n_funct.py", params=filters)
        logger.info(f"1-N analizi görevi başladı. Filtreler: {filters}. Log ID: {log_id}")

        # Veritabanından verileri il adına göre çekiyoruz.
        il_name = filters.get("il")
        if not il_name:
            raise ValueError("Filtrelerde 'il' bilgisi bulunamadı.")
        df_match = DB.load_matched_for_il(il_name)
        
        if df_match.empty:
            success_message = "Filtrelerle eşleşen işlenecek kayıt bulunamadı."
            logger.info(success_message)
            log_task_success(log_id, success_message)
            return {"status": "SKIPPED", "reason": success_message}

        logger.info(f"İşlenecek {len(df_match)} adet eşleşmiş kayıt bulundu.")
        yeni_kayitlar = []
        
        for index, row in df_match.iterrows():
            mahalle_id = coerce_int(row.get("mahalle_id"))
            ada = coerce_int(row.get("AdaBilgisi"))
            parsel = coerce_int(row.get("ParselBilgisi"))

            if not all([mahalle_id, ada is not None, parsel is not None]):
                continue
            
            root_feature = fetch_parcel_from_tkgm(mahalle_id, ada, parsel)
            if not root_feature: continue

            final_features = collect_active_features_recursive(root_feature, visited_ids=set())
            
            bucket = "bilinmiyor"
            if not final_features:
                final_features, bucket = [root_feature], "degismemis"
            elif len(final_features) == 1:
                props = (final_features[0] or {}).get("properties", {}) or {}
                is_same = (coerce_int(props.get("mahalleId")) == mahalle_id and
                           coerce_int(props.get("adaNo")) == ada and
                           coerce_int(props.get("parselNo")) == parsel)
                bucket = "degismemis" if is_same else "1-1"
            else:
                bucket = "1-n"

            for feature in final_features:
                props = (feature or {}).get("properties", {}) or {}
                yeni_kayitlar.append({
                    "orig_id": row.get("Id"), "il": row.get("IlBilgisi"), "ilce": row.get("IlceBilgisi"),
                    "mahalle": props.get("mahalleAd"), "mahalle_id": props.get("mahalleId"),
                    "ada": props.get("adaNo"), "parsel": props.get("parselNo"), "bucket": bucket,
                    "tkgm_properties": json.dumps(props),
                })

        if yeni_kayitlar:
            df_new = pd.DataFrame(yeni_kayitlar)
            DB.insert_after_1n(df_new)

        success_message = f"Görev tamamlandı. {len(df_match)} kayıt işlendi, {len(yeni_kayitlar)} yeni/güncel kayıt oluşturuldu."
        log_task_success(log_id, success_message)
        logger.info(success_message)
        
        return {"status": "SUCCESS", "filters": filters, "processed_count": len(df_match), "new_records_created": len(yeni_kayitlar)}

    except Exception as e:
        error_message = f"Görev sırasında beklenmedik bir hata oluştu: {e}"
        logger.error(error_message, exc_info=True)
        if log_id:
            log_task_error(log_id, str(e))
        raise
