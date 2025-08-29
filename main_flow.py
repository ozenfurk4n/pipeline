# main_flow.py

from prefect import flow, get_run_logger
from typing import Optional, List, Dict, Any

# ==========================================================================
# === BÖLÜM 1: GÖREVLERİ (TASK) İÇERİ AKTARMA
# ==========================================================================

from tasks.fetch import fetch_tkgm_data_task
from tasks.compare import compare_for_il_task
from tasks.eslesmedi_catch import process_unmatched_task
from tasks.funct_1n import analyze_1n_task

import DB_postgre.DB_postgre as DB

# ==========================================================================
# === BÖLÜM 2: ANA İŞ AKIŞI (FLOW)
# ==========================================================================

@flow(name="TKGM Veri Senkronizasyon Akışı", log_prints=True)
def tkgm_pipeline_flow(
    il: Optional[str] = None,
    ilce: Optional[str] = None,
    mahalle_id: Optional[int] = None,
    ada: Optional[int] = None,
    parsel: Optional[int] = None
):
    """
    Tüm TKGM veri işleme adımlarını yöneten ana Prefect akışı.
    Artık il, ilçe, mahalle, ada ve parsel bazında filtrelenebilir.
    """
    logger = get_run_logger()

    logger.info("Veritabanı tabloları kontrol ediliyor ve gerekirse oluşturuluyor...")
    DB.ensure_tables()
    logger.info("Tablo kontrolü tamamlandı.")

    filters = {k: v for k, v in locals().items() if v is not None}
    logger.info(f"Akış aktif filtrelerle çalıştırılıyor: {filters}")

    iller_to_process: List[str] = []
    if il:
        iller_to_process = [il]
    else:
        logger.info("İl filtresi yok. Veritabanındaki tüm iller hedefleniyor.")
        df_iller = DB.list_unique_iller()
        if not df_iller.empty:
            iller_to_process = df_iller["il"].tolist()

    if not iller_to_process:
        logger.warning("İşlenecek hiç il bulunamadı. Akış sonlandırılıyor.")
        return

    for current_il in iller_to_process:
        logger.info(f"===== '{current_il}' İÇİN İŞLEM BAŞLADI =====")
        
        current_filters = filters.copy()
        current_filters["il"] = current_il

        fetch_tkgm_data_task(
            il_name=current_il, 
            ilce_name=current_filters.get("ilce"),
            mahalle_id=current_filters.get("mahalle_id"),
            ada=current_filters.get("ada"),
            parsel=current_filters.get("parsel")
        )
        compare_for_il_task(il_name=current_il, filters=current_filters)
        process_unmatched_task(filters=current_filters)
        analyze_1n_task(filters=current_filters)
        
        logger.info(f"===== '{current_il}' İÇİN İŞLEM TAMAMLANDI =====")

    logger.info("Tüm hedeflenen iller için pipeline akışı başarıyla tamamlandı.")

# ==========================================================================
# === BÖLÜM 3: AKIŞI HİZMETE ALMA (SERVING) - YENİ YÖNTEM
# ==========================================================================
if __name__ == "__main__":
    # Bu blok, `python3 main_flow.py` komutunu çalıştırdığınızda devreye girer.
    # Akışı Prefect sunucusuna kaydeder ve aynı zamanda bir "worker" gibi
    # arayüzden gelecek iş emirlerini dinlemeye başlar.
    tkgm_pipeline_flow.serve(
        name="TKGM Pipeline Deployment (Served)",
        
        # (Opsiyonel) Akışın otomatik olarak zamanlanmasını isterseniz:
        # schedule=(CronSchedule(cron="0 3 * * *", timezone="Europe/Istanbul"))
    )
