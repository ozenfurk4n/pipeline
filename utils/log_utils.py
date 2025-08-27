# utils/log_utils.py

import json
from datetime import datetime
from sqlalchemy import text

# Veritabanı modülünü import ediyoruz.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB

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
