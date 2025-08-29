# compare.py

# -*- coding: utf-8 -*-
"""
TKGM <> tum_clean karşılaştırma (Prefect + DB + merkezi utils log)
- İl/ilçe: TR→ASCII UPPER normalize + tam eşitlik
- Mahalle: 4 normal formdan (raw/clean × punct-collapsed) herhangi biri tam eşitse eşleşme
- mahalle_id kontrolü: safe_int ile güvenli; iki tarafta da int var ve farklıysa eşleşmeyi reddet
"""

import re
import math
import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from prefect import task, get_run_logger
from sqlalchemy import text

# Proje içi modüller
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB
from utils.utils import log_task_start, log_task_success, log_task_error

# ==========================================================================
# === BÖLÜM 1: METİN NORMALLEŞTİRME VE YARDIMCI FONKSİYONLAR
# ==========================================================================

_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WS_RE    = re.compile(r"\s+")

def tr_upper_ascii(s: Any) -> str:
    """İl/ilçe eşleştirmesi için TR→ASCII uppercase + whitespace collapse."""
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return ""
    t = str(s).strip().upper()
    t = (t.replace("İ", "I").replace("I", "I")
           .replace("Ş", "S").replace("Ğ", "G")
           .replace("Ü", "U").replace("Ö", "O").replace("Ç", "C"))
    return _WS_RE.sub(" ", t).strip()

def normalize_string(x: Any) -> str:
    """Mahalle adlarında kullanılan TR→ASCII uppercase normalizasyonu."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    t = str(x).strip().upper()
    return (t.replace("İ", "I")
             .replace("Ğ", "G")
             .replace("Ü", "U")
             .replace("Ş", "S")
             .replace("Ö", "O")
             .replace("Ç", "C"))

def clean_mahalle_name(x: Any) -> str:
    """Sondaki 'MAH.', 'MAHALLESI/İ', 'KÖY/Ü' gibi ekleri düşürür."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    name = str(x).strip()
    suffixes = [
        " KÖYÜ"," MAH."," MAHALLESI"," MAHALLESİ"," KÖY"," MAH",
        " Köyü"," Mah."," Mahallesi"," Köy"," Mah",
        " köyü"," mah."," mahallesi"," köy"," mah",
        "  MAH","  MAH.","  KÖYÜ","  KÖY",
    ]
    for s in sorted(suffixes, key=len, reverse=True):
        if name.endswith(s):
            name = name[: -len(s)]
            break
    return name.strip()

def remove_punct_and_collapse(text: str) -> str:
    """Noktalama işaretlerini boşluk ile değiştirip boşlukları tekilleştirir."""
    if not text:
        return ""
    return _WS_RE.sub(" ", _PUNCT_RE.sub(" ", text)).strip()

def sstr(x: Any) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    return str(x)

def safe_int(x: Any) -> Optional[int]:
    """'169260', '169260.0', numpy int, None, '' ... güvenle int'e çevirir."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if isinstance(x, int):
        return x
    try:
        import numpy as np  # varsa int64 vb. için
        if isinstance(x, np.integer):
            return int(x)
        if isinstance(x, float):
            if math.isfinite(x) and x.is_integer():
                return int(round(x))
            return None
    except Exception:
        pass
    s = str(x).strip().replace(",", "")
    if s.endswith(".0"):
        s = s[:-2]
    return int(s) if re.fullmatch(r"\d+", s) else None

# ==========================================================================
# === BÖLÜM 2: VERİ YÜKLEME
# ==========================================================================

def load_tum_full(il_name: str) -> Tuple[pd.DataFrame, List[str]]:
    eng = DB.engine()
    df = pd.read_sql(
        text('SELECT * FROM public."tum_clean" WHERE "IlBilgisi" = :il'),
        eng,
        params={"il": il_name},
    )
    base_cols = list(df.columns)
    # güvenli id + alias kolonlar
    df["mahalle_id_safe"] = df.get("mahalle_id", pd.Series([None]*len(df))).apply(safe_int)
    df["il"] = df.get("IlBilgisi", "")
    df["ilce"] = df.get("IlceBilgisi", "")
    df["mahalle"] = df.get("MahalleBilgisi", "")
    return df, base_cols

# ==========================================================================
# === BÖLÜM 3: AKILLI EŞLEŞTİRME İNDEKSİ VE FONKSİYONLAR
# ==========================================================================

def build_tkgm_index(df_tkgm: pd.DataFrame) -> pd.DataFrame:
    """
    TKGM tarafı için normalize alanlar:
      - _IL_N, _ILCE_N : il/ilçe (TR→ASCII UPPER)
      - _MAH_*         : mahalle için 4 normal form
    """
    out = df_tkgm.copy()
    out["_IL_N"]   = out["tkgm_il"].apply(tr_upper_ascii)
    out["_ILCE_N"] = out["tkgm_ilce"].apply(tr_upper_ascii)

    out["_MAH_N"]        = out["tkgm_mahalle"].apply(normalize_string)
    out["_MAH_CLEAN_N"]  = out["tkgm_mahalle"].apply(lambda x: normalize_string(clean_mahalle_name(x)))
    out["_MAH_NP"]       = out["_MAH_N"].apply(remove_punct_and_collapse)
    out["_MAH_CLEAN_NP"] = out["_MAH_CLEAN_N"].apply(remove_punct_and_collapse)
    return out

def _row_norms(row: pd.Series) -> Dict[str, str]:
    """tum_clean satırı için 4 mahalle normal formunu üret."""
    il_n   = tr_upper_ascii(row.get("il"))
    ilce_n = tr_upper_ascii(row.get("ilce"))

    mah_raw   = sstr(row.get("mahalle"))
    mah_clean = clean_mahalle_name(mah_raw)

    mah_raw_n    = normalize_string(mah_raw)
    mah_clean_n  = normalize_string(mah_clean)
    mah_raw_np   = remove_punct_and_collapse(mah_raw_n)
    mah_clean_np = remove_punct_and_collapse(mah_clean_n)

    return {
        "IL_N": il_n,
        "ILCE_N": ilce_n,
        "MAH_RAW_N": mah_raw_n,
        "MAH_CLEAN_N": mah_clean_n,
        "MAH_RAW_NP": mah_raw_np,
        "MAH_CLEAN_NP": mah_clean_np,
    }

def find_tkgm_match_for_row(row: pd.Series, idx: pd.DataFrame) -> bool:
    """
    True/False döndürür:
      - İl/ilçe normalize tam eşitlik
      - Mahalle 4 normal formdan herhangi biri eşit
      - Eğer iki tarafta da mahalle_id int ise ve farklıysa → False
    """
    norms = _row_norms(row)

    sub = idx[(idx["_IL_N"] == norms["IL_N"]) & (idx["_ILCE_N"] == norms["ILCE_N"])]
    if sub.empty:
        return False

    cond = (
        (sub["_MAH_N"]        == norms["MAH_RAW_N"])   |
        (sub["_MAH_CLEAN_N"]  == norms["MAH_RAW_N"])   |
        (sub["_MAH_N"]        == norms["MAH_CLEAN_N"]) |
        (sub["_MAH_CLEAN_N"]  == norms["MAH_CLEAN_N"]) |
        (sub["_MAH_NP"]       == norms["MAH_RAW_NP"])  |
        (sub["_MAH_CLEAN_NP"] == norms["MAH_RAW_NP"])  |
        (sub["_MAH_NP"]       == norms["MAH_CLEAN_NP"])|
        (sub["_MAH_CLEAN_NP"] == norms["MAH_CLEAN_NP"])
    )
    sub2 = sub[cond]
    if sub2.empty:
        return False

    # mahalle_id tutarlılığı (varsa)
    tum_id = safe_int(row.get("mahalle_id_safe"))
    if tum_id is not None and "tkgm_mahalle_id" in sub2.columns:
        tkgm_ids = sub2["tkgm_mahalle_id"].apply(safe_int).dropna().unique()
        if len(tkgm_ids) > 0 and tum_id not in tkgm_ids:
            return False

    return True

# ==========================================================================
# === BÖLÜM 4: PREFECT GÖREVİ
# ==========================================================================

@task(name="Veri Karşılaştırma ve Ayıklama Görevi", retries=2, retry_delay_seconds=60)
def compare_for_il_task(il_name: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Belirtilen il için verileri karşılaştırır. Tüm adımlarını,
    başarısını ve hatalarını pipeline_logs tablosuna kaydeder.
    """
    logger = get_run_logger()
    log_id = None

    try:
        log_id = log_task_start(script_name="compare.py", params={"il_name": il_name})
        logger.info(f"'{il_name}' için karşılaştırma görevi başladı. Log ID: {log_id}")

        # Güvenlik kontrolü: yerelde TKGM verisi var mı?
        if not DB.tkgm_has_il(il_name):
            raise ValueError(f"'{il_name}' için yerel TKGM verisi bulunamadı.")

        logger.info("Veri kaynakları yükleniyor...")
        df_tum, base_cols = load_tum_full(il_name)
        df_tkgm = DB.tkgm_rows_for_il(il_name)

        if df_tum.empty:
            msg = "Kaynak veri (tum_clean) boş olduğu için işlem atlandı."
            logger.warning(msg)
            log_task_success(log_id, msg)
            return {"status": "SKIPPED", "reason": msg}

        if df_tkgm.empty:
            raise RuntimeError("TKGM veri tablosu boş görünüyor.")

        logger.info("TKGM arama indeksi oluşturuluyor...")
        tkgm_index = build_tkgm_index(df_tkgm)

        logger.info(f"{len(df_tum)} satır için eşleştirme yapılıyor...")
        matched_mask = df_tum.apply(lambda row: find_tkgm_match_for_row(row, tkgm_index), axis=1)

        df_match = df_tum.loc[matched_mask, base_cols].copy()
        df_miss  = df_tum.loc[~matched_mask, base_cols].copy()

        logger.info(f"'{il_name}' için eski karşılaştırma sonuçları veritabanından siliniyor...")
        DB.clear_compare_results_for_il(il_name)

        logger.info("Yeni sonuçlar veritabanına yazılıyor...")
        if not df_match.empty:
            DB.insert_matches(df_match)
        if not df_miss.empty:
            DB.insert_misses(df_miss)

        msg = f"Karşılaştırma tamamlandı. Eşleşen: {len(df_match)}, Eşleşmeyen: {len(df_miss)}"
        log_task_success(log_id, msg)
        logger.info(msg)

        return {
            "status": "SUCCESS",
            "il": il_name,
            "matched_count": int(len(df_match)),
            "unmatched_count": int(len(df_miss)),
        }

    except Exception as e:
        logger.error(f"Hata: {e}", exc_info=True)
        if log_id:
            log_task_error(log_id, str(e))
        raise

# ==========================================================================
# === BÖLÜM 5: DOĞRUDAN ÇALIŞTIRMA (TEST)
# ==========================================================================
if __name__ == "__main__":
    TEST_IL_NAME = "ANKARA"
    print(f"--- BAĞIMSIZ TEST: '{TEST_IL_NAME}' ---")
    DB.ensure_tables()
    compare_for_il_task.fn(il_name=TEST_IL_NAME)