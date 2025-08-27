# utils/utils.py

import json
import re
import math
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text

# Veritabanı modülünü import ediyoruz.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB

# ==========================================================================
# === LOGLAMA YARDIMCI FONKSİYONLARI
# ==========================================================================

def log_task_start(script_name: str, params: dict) -> int:
    """Bir görevin başladığını loglar ve log ID'sini döndürür."""
    eng = DB.engine()
    q = text(f"""
        INSERT INTO {DB.T_LOGS} (script_adi, parametreler, baslangic_zamani, durum)
        VALUES (:script, :params, :start_time, 'BASLADI')
        RETURNING log_id;
    """)
    with eng.begin() as con:
        result = con.execute(q, {
            "script": script_name,
            "params": json.dumps(params),
            "start_time": datetime.now()
        }).scalar_one()
    return result

def log_task_success(log_id: int, message: str):
    """Bir görevin başarıyla tamamlandığını loglar."""
    eng = DB.engine()
    q = text(f"""
        UPDATE {DB.T_LOGS}
        SET durum = 'BASARILI', bitis_zamani = :end_time, mesaj = :msg
        WHERE log_id = :log_id;
    """)
    with eng.begin() as con:
        con.execute(q, {"end_time": datetime.now(), "msg": message, "log_id": log_id})

def log_task_error(log_id: int, error_message: str):
    """Bir görevde hata oluştuğunu loglar."""
    eng = DB.engine()
    q = text(f"""
        UPDATE {DB.T_LOGS}
        SET durum = 'HATA', bitis_zamani = :end_time, mesaj = :msg
        WHERE log_id = :log_id;
    """)
    with eng.begin() as con:
        con.execute(q, {"end_time": datetime.now(), "msg": error_message, "log_id": log_id})

# ==========================================================================
# === METİN NORMALLEŞTİRME VE YARDIMCI FONKSİYONLAR
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
