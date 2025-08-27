# error_management.py

import json
from datetime import datetime
from prefect import flow, task, get_run_logger
from sqlalchemy import text

# Kendi veritabanı erişim modülümüzü import ediyoruz.
# Log tablolarına erişmek ve görevleri yeniden çalıştırmak için gereklidir.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import DB_postgre.DB_postgre as DB

# Yeniden çalıştırılacak görevleri import ediyoruz.
# Bu sayede bu script, diğer script'lerin fonksiyonlarını çağırabilir.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tasks.compare import compare_for_il_task
from tasks.eslesmedi_catch import process_unmatched_task
from tasks.funct_1n import analyze_1n_task

# ==========================================================================
# === BÖLÜM 1: KONFİGÜRASYON
# ==========================================================================

# Bir görevin en fazla kaç kez yeniden deneneceğini belirleyen limit.
# Bu, sonsuz döngüleri ve kaynak israfını önler.
MAX_RETRIES = 3

# Hangi script adının hangi Prefect görev fonksiyonuna karşılık geldiğini
# belirten bir harita. Bu, dinamik olarak doğru görevi çağırmamızı sağlar.
TASK_MAP = {
    "compare.py": compare_for_il_task,
    "eslesmedi_catch.py": process_unmatched_task,
    "1n_funct.py": analyze_1n_task,
    # Gelecekte eklenecek yeni görevler buraya yazılabilir.
}

# ==========================================================================
# === BÖLÜM 2: HATA YÖNETİMİ MANTIĞI
# ==========================================================================

@task(name="Hatalı Görevleri Tespit Etme")
def find_failed_tasks():
    """
    `pipeline_logs` tablosunu sorgulayarak, 'HATA' durumunda olan ve
    yeniden deneme hakkı (MAX_RETRIES) dolmamış olan görevleri bulur.
    """
    logger = get_run_logger()
    logger.info("Yeniden denenebilecek hatalı görevler aranıyor...")
    
    eng = DB.engine()
    q = text(f"""
        SELECT log_id, script_adi, parametreler, retry_count, mesaj
        FROM {DB.T_LOGS}
        WHERE durum = 'HATA' AND retry_count < :max_retries
    """)
    
    df_failed = pd.read_sql(q, eng, params={"max_retries": MAX_RETRIES})
    logger.info(f"{len(df_failed)} adet yeniden denenebilir görev bulundu.")
    
    # DataFrame'i, her satırı bir dictionary olan bir listeye çevirerek döndür.
    # Bu, sonraki adımlarda işlemeyi kolaylaştırır.
    return df_failed.to_dict('records')

@task(name="Görevi Kalıcı Hatalara Taşıma")
def move_to_permanent_errors(task_info: dict):
    """
    Yeniden deneme hakkı dolmuş bir görevi, manuel inceleme için
    `second_check_still_error` tablosuna kaydeder.
    """
    logger = get_run_logger()
    log_id = task_info['log_id']
    logger.warning(f"Log ID {log_id}: Görev deneme limitine ulaştı. Kalıcı hatalara taşınıyor.")
    
    eng = DB.engine()
    with eng.begin() as con:
        # 1. `second_check_still_error` tablosuna yeni bir kayıt ekle.
        insert_q = text(f"""
            INSERT INTO {DB.T_ERRORS} 
            (original_log_id, script_adi, parametreler, son_hata_mesaji, deneme_sayisi)
            VALUES (:log_id, :script, :params, :msg, :retries)
        """)
        con.execute(insert_q, {
            "log_id": log_id,
            "script": task_info['script_adi'],
            "params": json.dumps(task_info['parametreler']),
            "msg": task_info['mesaj'],
            "retries": task_info['retry_count']
        })
        
        # 2. Orijinal log kaydının durumunu 'HATA_KALICI' olarak güncelle.
        # Bu, bu görevin bir daha bu script tarafından ele alınmasını engeller.
        update_q = text(f"UPDATE {DB.T_LOGS} SET durum = 'HATA_KALICI' WHERE log_id = :log_id")
        con.execute(update_q, {"log_id": log_id})

@task(name="Görevi Yeniden Çalıştırma")
def rerun_task(task_info: dict):
    """
    Hatalı bir görevi, log tablosundan okuduğu orijinal parametrelerle
    yeniden tetikler.
    """
    logger = get_run_logger()
    log_id = task_info['log_id']
    script_name = task_info['script_adi']
    params = task_info['parametreler']
    
    logger.info(f"Log ID {log_id}: '{script_name}' görevi yeniden çalıştırılıyor. Parametreler: {params}")
    
    # 1. Log tablosundaki deneme sayısını artır ve durumu güncelle.
    eng = DB.engine()
    with eng.begin() as con:
        update_q = text(f"""
            UPDATE {DB.T_LOGS}
            SET durum = 'YENIDEN_DENENIYOR', retry_count = retry_count + 1
            WHERE log_id = :log_id
        """)
        con.execute(update_q, {"log_id": log_id})

    # 2. Doğru görev fonksiyonunu `TASK_MAP` üzerinden bul.
    task_function = TASK_MAP.get(script_name)
    if not task_function:
        logger.error(f"Log ID {log_id}: '{script_name}' için bir görev fonksiyonu bulunamadı!")
        # Bu durumu logda hata olarak işaretle.
        with eng.begin() as con:
            text(f"UPDATE {DB.T_LOGS} SET durum = 'HATA', mesaj = 'Geçersiz script adı' WHERE log_id = :log_id")
        return

    # 3. Görevi, orijinal parametreleriyle yeniden çalıştır.
    try:
        # `params` bir dictionary olduğu için, `**params` kullanarak
        # fonksiyonu anahtar-değer çiftleriyle çağırıyoruz.
        # Örn: `compare_for_il_task(il_name="ANKARA")`
        task_function(**params)
        
        # Eğer görev başarılı olursa, kendi içindeki `try...except` bloğu
        # logu 'BASARILI' olarak güncelleyecektir. Bizim burada bir şey yapmamıza gerek yok.
        
    except Exception as e:
        # Eğer yeniden deneme de başarısız olursa, görev fonksiyonu kendi içindeki
        # `try...except` bloğu ile logu tekrar 'HATA' olarak güncelleyecektir.
        # Bu sayede bir sonraki `error_management` çalıştırmasında tekrar ele alınabilir.
        logger.error(f"Log ID {log_id}: Görevin yeniden çalıştırılması sırasında bir hata oluştu: {e}")


# ==========================================================================
# === BÖLÜM 3: ANA İŞ AKIŞI (FLOW)
# ==========================================================================

@flow(name="Otomatik Hata Yönetimi ve Yeniden Deneme Akışı")
def error_manager_flow():
    """
    Periyodik olarak çalışarak başarısız olan görevleri tespit eder ve
    onları yönetir.
    """
    logger = get_run_logger()
    logger.info("Hata yönetimi akışı başladı.")
    
    # Adım 1: Yeniden denenebilecek tüm hatalı görevleri bul.
    failed_tasks = find_failed_tasks()
    
    if not failed_tasks:
        logger.info("İşlem gerektiren hatalı görev bulunamadı. Akış sonlandırılıyor.")
        return

    # Adım 2: Her bir hatalı görev için karar ver ve işlem yap.
    for task_info in failed_tasks:
        # Bu döngü, her bir görev için ayrı ayrı çalışır.
        # Prefect, bu görevleri potansiyel olarak paralel çalıştırabilir.
        rerun_task(task_info)
        
    logger.info("Hata yönetimi akışı tamamlandı.")

# ==========================================================================
# === BÖLÜM 4: DOĞRUDAN ÇALIŞTIRMA (TEST İÇİN)
# ==========================================================================
if __name__ == "__main__":
    print("--- BAĞIMSIZ TEST MODU: Hata Yönetimi ---")
    # Bu akışı çalıştırdığınızda, `pipeline_logs` tablosunu kontrol eder
    # ve 'HATA' durumundaki görevleri yeniden çalıştırmaya çalışır.
    error_manager_flow()
