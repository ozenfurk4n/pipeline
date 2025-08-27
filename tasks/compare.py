# compare.py

import re
import math
import time
import json
from datetime import datetime
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

from prefect import task, get_run_logger
from sqlalchemy import text, exc

# Kendi yazdığımız veritabanı erişim modülünü import ediyoruz.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB

# --- GÜNCELLENEN BÖLÜM ---
# Artık loglama fonksiyonlarını merkezi `utils` klasöründen alıyoruz.
# Bu, kod tekrarını önler ve import hatalarını çözer.
from utils.log_utils import log_task_start, log_task_success, log_task_error

# ==========================================================================
# === BÖLÜM 1: METİN NORMALLEŞTİRME VE YARDIMCI FONKSİYONLAR
# ==========================================================================

_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WS_RE    = re.compile(r"\s+")

def tr_upper_ascii(s: Any) -> str:
    if s is None or (isinstance(s, float) and math.isnan(s)): return ""
    t = str(s).strip().upper()
    t = (t.replace("İ","I").replace("I","I").replace("Ş","S").replace("Ğ","G").replace("Ü","U").replace("Ö","O").replace("Ç","C"))
    return _WS_RE.sub(" ", t).strip()

def normalize_string(x: Any) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)): return ""
    t = str(x).strip().upper()
    return (t.replace("İ","I").replace("Ğ","G").replace("Ü","U").replace("Ş","S").replace("Ö","O").replace("Ç","C"))

def clean_mahalle_name(x: Any) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)): return ""
    name = str(x).strip()
    suffixes = [" KÖYÜ"," MAH."," MAHALLESI"," MAHALLESİ"," KÖY"," MAH", " Köyü"," Mah."," Mahallesi"," Köy"," Mah", " köyü"," mah."," mahallesi"," köy"," mah", "  MAH","  MAH.","  KÖYÜ","  KÖY"]
    for s in sorted(suffixes, key=len, reverse=True):
        if name.endswith(s):
            name = name[: -len(s)]
            break
    return name.strip()

def remove_punct_and_collapse(text: str) -> str:
    if not text: return ""
    return _WS_RE.sub(" ", _PUNCT_RE.sub(" ", text)).strip()

def sstr(x: Any) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)): return ""
    return str(x)

def safe_int(x: Any) -> Optional[int]:
    if x is None or (isinstance(x, float) and math.isnan(x)): return None
    if isinstance(x, int): return x
    s = str(x).strip().replace(",", "")
    if s.endswith(".0"): s = s[:-2]
    return int(s) if re.fullmatch(r"\d+", s) else None

# ==========================================================================
# === BÖLÜM 2: VERİ YÜKLEME VE EŞLEŞTİRME MANTIĞI
# ==========================================================================

def load_tum_full(il_name: str) -> Tuple[pd.DataFrame, List[str]]:
    eng = DB.engine()
    df = pd.read_sql(text('SELECT * FROM public."tum_clean" WHERE "IlBilgisi" = :il'), eng, params={"il": il_name})
    base_cols = list(df.columns)
    df["mahalle_id_safe"] = df.get("mahalle_id", pd.Series([None]*len(df))).apply(safe_int)
    df["il"] = df.get("IlBilgisi", "")
    df["ilce"] = df.get("IlceBilgisi", "")
    df["mahalle"] = df.get("MahalleBilgisi", "")
    return df, base_cols

def build_tkgm_index(df_tkgm: pd.DataFrame) -> pd.DataFrame:
    out = df_tkgm.copy()
    out["_IL_N"] = out["tkgm_il"].apply(tr_upper_ascii)
    out["_ILCE_N"] = out["tkgm_ilce"].apply(tr_upper_ascii)
    out["_MAH_N"] = out["tkgm_mahalle"].apply(normalize_string)
    out["_MAH_CLEAN_N"] = out["tkgm_mahalle"].apply(lambda x: normalize_string(clean_mahalle_name(x)))
    out["_MAH_NP"] = out["_MAH_N"].apply(remove_punct_and_collapse)
    out["_MAH_CLEAN_NP"] = out["_MAH_CLEAN_N"].apply(remove_punct_and_collapse)
    return out

def find_tkgm_match_for_row(row: pd.Series, idx: pd.DataFrame) -> bool:
    il_n = tr_upper_ascii(row.get("il"))
    ilce_n = tr_upper_ascii(row.get("ilce"))
    mah_raw, mah_clean = sstr(row.get("mahalle")), clean_mahalle_name(sstr(row.get("mahalle")))
    mah_raw_n, mah_clean_n = normalize_string(mah_raw), normalize_string(mah_clean)
    mah_raw_np, mah_clean_np = remove_punct_and_collapse(mah_raw_n), remove_punct_and_collapse(mah_clean_n)
    sub = idx[(idx["_IL_N"] == il_n) & (idx["_ILCE_N"] == ilce_n)]
    if sub.empty: return False
    cond = ((sub["_MAH_N"] == mah_raw_n) | (sub["_MAH_CLEAN_N"] == mah_raw_n) | (sub["_MAH_N"] == mah_clean_n) | (sub["_MAH_CLEAN_N"] == mah_clean_n) | (sub["_MAH_NP"] == mah_raw_np) | (sub["_MAH_CLEAN_NP"] == mah_raw_np) | (sub["_MAH_NP"] == mah_clean_np) | (sub["_MAH_CLEAN_NP"] == mah_clean_np))
    sub2 = sub[cond]
    if sub2.empty: return False
    tum_id = safe_int(row.get("mahalle_id_safe"))
    if tum_id is not None:
        tkgm_ids = sub2["tkgm_mahalle_id"].apply(safe_int).dropna().unique()
        if len(tkgm_ids) > 0 and tum_id not in tkgm_ids: return False
    return True

# ==========================================================================
# === BÖLÜM 3: PREFECT GÖREVİ (TASK)
# ==========================================================================

@task(name="Veri Karşılaştırma ve Ayıklama Görevi", retries=2, retry_delay_seconds=60)
def compare_for_il_task(il_name: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Belirtilen il için verileri karşılaştırır. Artık tüm adımlarını,
    başarısını ve hatalarını `pipeline_logs` tablosuna kaydeder.
    """
    logger = get_run_logger()
    log_id = None
    
    try:
        log_id = log_task_start(script_name="compare.py", params={"il_name": il_name})
        logger.info(f"'{il_name}' için karşılaştırma görevi başladı. Log ID: {log_id}")

        # Çekirdek iş mantığı
        if not DB.tkgm_has_il(il_name):
            # Bu kontrol artık fetch.py'de yapıldığı için burası bir güvenlik ağıdır.
            # Normalde bu hatayı görmememiz gerekir.
            raise ValueError(f"'{il_name}' için yerel TKGM verisi bulunamadı.")

        logger.info("Veri kaynakları yükleniyor...")
        df_tum, base_cols = load_tum_full(il_name)
        df_tkgm = DB.tkgm_rows_for_il(il_name)

        if df_tum.empty:
            success_message = "Kaynak veri (tum_clean) boş olduğu için işlem atlandı."
            logger.warning(success_message)
            log_task_success(log_id, success_message)
            return {"status": "SKIPPED", "reason": success_message}
        
        logger.info("TKGM arama indeksi oluşturuluyor...")
        tkgm_index = build_tkgm_index(df_tkgm)

        logger.info(f"{len(df_tum)} satır için eşleştirme işlemi yapılıyor...")
        matched_mask = df_tum.apply(lambda row: find_tkgm_match_for_row(row, tkgm_index), axis=1)

        df_match = df_tum.loc[matched_mask, base_cols].copy()
        df_miss  = df_tum.loc[~matched_mask, base_cols].copy()
        
        logger.info(f"'{il_name}' için eski karşılaştırma sonuçları veritabanından siliniyor...")
        DB.clear_compare_results_for_il(il_name)

        logger.info("Yeni sonuçlar veritabanına yazılıyor...")
        if not df_match.empty: DB.insert_matches(df_match)
        if not df_miss.empty: DB.insert_misses(df_miss)

        success_message = f"Karşılaştırma tamamlandı. Eşleşen: {len(df_match)}, Eşleşmeyen: {len(df_miss)}"
        log_task_success(log_id, success_message)
        logger.info(success_message)
        
        return {"status": "SUCCESS", "il": il_name, "matched_count": len(df_match), "unmatched_count": len(df_miss)}

    except Exception as e:
        error_message = f"Görev sırasında beklenmedik bir hata oluştu: {e}"
        logger.error(error_message, exc_info=True)
        if log_id:
            log_task_error(log_id, str(e))
        raise

# ==========================================================================
# === BÖLÜM 4: DOĞRUDAN ÇALIŞTIRMA (TEST İÇİN)
# ==========================================================================
if __name__ == "__main__":
    TEST_IL_NAME = "ANKARA"
    print(f"--- BAĞIMSIZ TEST MODU: '{TEST_IL_NAME}' ---")
    DB.ensure_tables()
    compare_for_il_task.fn(il_name=TEST_IL_NAME)
