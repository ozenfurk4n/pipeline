# deploy.py

from prefect.server.schemas.schedules import CronSchedule

# Ana akışımızı (flow) main_flow.py dosyasından import ediyoruz.
from main_flow import tkgm_pipeline_flow

# Prefect'e akışımızı tanıtacak olan bir "Deployment" nesnesi oluşturuyoruz.
deployment = tkgm_pipeline_flow.to_deployment(
    # Bu deployment'a arayüzde görünecek bir isim veriyoruz.
    name="TKGM Pipeline - Python Deployment",
    
    # Bu 'parameters' sözlüğü, Prefect arayüzüne her bir parametre için
    # bir metin kutusu oluşturmasını ve varsayılan değerinin boş olmasını söyler.
    parameters={
        "il": None,
        "ilce": None,
        "mahalle_id": None,
        "ada": None,
        "parsel": None
    },
    
    # --- YENİ EKLENEN SATIR ---
    # Arayüzün bu değişikliği fark etmesini sağlamak için bir versiyon numarası ekliyoruz.
    # Bir sonraki güncellemede bunu "1.2" olarak değiştirebilirsiniz.
    version="1.1",
    
    # (Opsiyonel) Akışın otomatik olarak zamanlanmasını isterseniz:
    # schedule=(CronSchedule(cron="0 3 * * *", timezone="Europe/Istanbul"))
)

if __name__ == "__main__":
    # Oluşturduğumuz deployment'ı Prefect sunucusuna uyguluyoruz (kaydediyoruz).
    deployment.apply()
    
    print("Deployment başarıyla oluşturuldu ve Prefect sunucusuna uygulandı!")
    print("Şimdi 'prefect worker start --pool default-agent-pool' komutuyla bir worker başlatabilirsiniz.")

