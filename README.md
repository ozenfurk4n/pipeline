TKGM Veri Senkronizasyon Pipeline'ı
Projeye Genel Bakış
Bu proje, yerel veritabanımızdaki (tum_clean) parsel verilerini, Tapu ve Kadastro Genel Müdürlüğü'nün (TKGM) güncel verileriyle senkronize etmek için tasarlanmış otonom bir veri işleme hattıdır (pipeline).

Pipeline'ın ana görevleri şunlardır:

Karşılaştırma: Yerel verilerle TKGM'nin idari yapı verilerini karşılaştırarak eşleşen ve eşleşmeyen kayıtları ayıklar.

Geometrik Kurtarma: Eşleşmeyen kayıtları, coğrafi poligon verilerini kullanarak PostGIS ile analiz eder ve doğru mahalleyi bularak kurtarmaya çalışır.

Evrim Analizi (1-N): Eşleşen parsellerin zaman içinde birleşme, bölünme veya bilgi değişikliği gibi durumlara uğrayıp uğramadığını TKGM API'si üzerinden takip eder.

Otomasyon ve Yönetim: Tüm bu süreci, durum takibi, loglama ve hata yönetimi sağlayan Prefect aracıyla yönetir.

IP Yönetimi: TKGM API'sinin uyguladığı sorgu limitlerini aşmak için Tunnelblick ve VPNBook kullanarak IP adresini otomatik olarak değiştirir.

Proje Yapısı
Proje, görevleri mantıksal olarak ayıran modüler bir yapıya sahiptir:

/
├── DB_postgre/
│   └── DB_postgre.py       # Veritabanı erişim katmanı
├── tasks/
│   ├── compare.py          # 1. Adım: Karşılaştırma görevi
│   ├── eslesmedi_catch.py  # 2. Adım: Geometrik kurtarma görevi
│   ├── 1n_funct.py         # 3. Adım: Evrim analizi görevi
│   └── fetch.py            # Ön Adım: TKGM'den veri çekme görevi
├── utils/
│   ├── utils.py            # Merkezi yardımcı fonksiyonlar (loglama, normalizasyon)
│   └── vpn_manager.py      # Otomatik IP yönetimi (Tunnelblick)
├── vpn_configs/
│   └── ... (.ovpn dosyaları buraya gelecek)
├── main_flow.py            # Tüm görevleri yöneten ana Prefect akışı
└── deploy.py               # Akışı Prefect sunucusuna kaydetmek için script

Gerekli Kurulumlar (Ön Koşullar)
Bu projeyi çalıştırabilmek için aşağıdaki yazılımların sisteminizde kurulu olması gerekmektedir.

Python 3.9+ ve pip paket yöneticisi.

PostgreSQL veritabanı ve PostGIS coğrafi veri eklentisi.

Homebrew (macOS için paket yöneticisi): brew --version komutuyla kontrol edebilirsiniz.

Tunnelblick: macOS için OpenVPN istemcisi. Resmi sitesinden indirin.

Python Kütüphaneleri: Proje klasöründe terminalde pip install -r requirements.txt komutunu çalıştırarak gerekli tüm kütüphaneleri yükleyin. (Not: requirements.txt dosyasını pip freeze > requirements.txt komutuyla oluşturabilirsiniz.)

İlk Kurulum Adımları (Sadece Bir Kez Yapılır)
Projeyi yeni bir bilgisayarda ilk kez kurarken aşağıdaki adımları sırayla takip edin.

1. Veritabanı Ayarları

DB_postgre/DB_postgre.py dosyasını açın.

DB_URL değişkenini kendi PostgreSQL bağlantı bilgilerinize göre güncelleyin (kullanici:sifre@host:port/veritabani_adi).

Veritabanınızda PostGIS eklentisinin aktif olduğundan emin olun (CREATE EXTENSION postgis;).

2. VPN Ayarları (Tunnelblick ve VPNBook)

Bu, otomatik IP değişimi için en kritik adımdır.

VPNBook'tan Dosyaları İndirin:

https://www.vpnbook.com/freevpn adresine gidin.

"OpenVPN" başlığı altındaki "Certificate Bundle" paketlerinden birini (örn: Euro2) indirin.

Aynı sayfada resim içinde yazan güncel kullanıcı adı ve şifreyi not alın.

vpn_configs Klasörünü Oluşturun:

Projenizin ana dizininde vpn_configs adında bir klasör oluşturun.

İndirdiğiniz ZIP dosyasının içindeki tüm .ovpn dosyalarını bu yeni klasörün içine taşıyın.

Tunnelblick'e Yükleyin:

vpn_configs klasöründeki tüm .ovpn dosyalarını seçin ve ekranın sağ üst köşesindeki Tunnelblick ikonunun üzerine sürükleyip bırakın.

Tunnelblick'in "yapılandırmaları kur" sorularını onaylayın.

Şifreyi Koda Girin:

utils/vpn_manager.py dosyasını açın.

VPN_PASSWORD değişkeninin değerini, VPNBook sitesinden aldığınız güncel şifre ile değiştirin.

Pipeline Nasıl Çalıştırılır?
Pipeline, Prefect tarafından yönetilir ve çalıştırılması için 3 ayrı terminal penceresi gerekir.

Adım 1: Prefect Sunucusunu Başlatın (Terminal 1)

Bu terminal, projenin "patronu" veya "kontrol merkezi" olarak sürekli açık kalmalıdır.

prefect server start

Bu komut size http://127.0.0.1:4200 gibi bir adres verecektir. Bu adresi tarayıcınızda açarak Prefect arayüzüne erişin.

Adım 2: Akışı Prefect'e Kaydedin (Deployment)

Bu işlem, akışınızda bir değişiklik yaptığınızda veya ilk kurulumda sadece bir kez yapılır.

python3 deploy.py

Bu komut, main_flow.py'deki akışı bulur ve Prefect sunucusuna kaydeder.

Adım 3: Çalışanı (Worker) Başlatın (Terminal 2)

Bu terminal, Prefect sunucusundan gelen iş emirlerini alıp çalıştıran "işçi" olarak sürekli açık kalmalıdır.

# API adresini ayarlayın (sunucunun adresini belirtir)
export PREFECT_API_URL="http://127.0.0.1:4200/api"

# Worker'ı başlatın
prefect worker start --pool 'default-agent-pool'

Adım 4: Arayüzden Akışı Çalıştırın (Tarayıcı)

Artık her şey hazır!

Tarayıcınızdaki Prefect arayüzüne gidin (http://127.0.0.1:4200).

Sol menüden "Deployments" sayfasına tıklayın.

"TKGM Pipeline - Python Deployment" satırının sağındaki "Run" butonuna tıklayın.

Karşınıza çıkan formda, pipeline'ı belirli bir il, ilçe, mahalle vb. için çalıştırmak üzere parametreleri doldurabilir veya tüm veritabanı için çalıştırmak üzere boş bırakabilirsiniz.

Formu gönderdiğinizde, "Worker"ın çalıştığı terminalde logların akmaya başladığını ve arayüzden tüm süreci canlı olarak takip edebildiğinizi göreceksiniz.

