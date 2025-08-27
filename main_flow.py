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

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import DB_postgre.DB_postgre as DB

# ==========================================================================
# === BÖLÜM 2: ANA İŞ AKIŞI (FLOW) - TAM PARAMETRELİ
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

    # Prefect arayüzünden gelen parametreleri tek bir filtre sözlüğünde topla.
    filters = {
        "il": il, "ilce": ilce, "mahalle_id": mahalle_id,
        "ada": ada, "parsel": parsel
    }
    # Değeri olmayan (None) filtreleri temizle.
    active_filters = {k: v for k, v in filters.items() if v is not None}
    logger.info(f"Akış aktif filtrelerle çalıştırılıyor: {active_filters}")

    # İşlenecek illeri belirle. Eğer il filtresi varsa sadece o il, yoksa tüm iller.
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

    # Belirlenen her bir il için pipeline adımlarını çalıştır.
    for current_il in iller_to_process:
        logger.info(f"===== '{current_il}' İÇİN İŞLEM BAŞLADI =====")
        
        # O anki il için geçerli olan filtreleri hazırla.
        # Eğer kullanıcı ilçe gibi daha alt bir filtre girdiyse,
        # o an işlenen ilin bu filtreyle eşleştiğinden emin olmalıyız.
        current_filters = active_filters.copy()
        current_filters["il"] = current_il

        # Adım 1: Veri Çekme/Kontrol (İlçe filtresi varsa uygula)
        fetch_tkgm_data_task(il_name=current_il, ilce_name=current_filters.get("ilce"))

        # Adım 2: Karşılaştırma (Filtrelerle çalışır)
        compare_for_il_task(il_name=current_il, filters=current_filters)
        
        # Adım 3: Eşleşmeyenleri Kurtarma (Filtrelerle çalışır)
        process_unmatched_task(filters=current_filters)
        
        # Adım 4: 1-N Analizi (Filtrelerle çalışır)
        analyze_1n_task(filters=current_filters)
        
        logger.info(f"===== '{current_il}' İÇİN İŞLEM TAMAMLANDI =====")

    logger.info("Tüm hedeflenen iller için pipeline akışı başarıyla tamamlandı.")

# ==========================================================================
# === BÖLÜM 3: DOĞRUDAN ÇALIŞTIRMA (TEST İÇİN)
# ==========================================================================
if __name__ == "__main__":
    # Örnek 1: Sadece "ANKARA" ilinin "ÇANKAYA" ilçesi için çalıştır
    tkgm_pipeline_flow(il="İSTANBUL", ilce="ARNAVUTKÖY")

    # Örnek 2: Tüm iller için çalıştır
    # tkgm_pipeline_flow()
