# utils/vpn_manager.py

import os
import subprocess
import random
import time
import requests
from prefect import task, get_run_logger, flow

# ==========================================================================
# === BÖLÜM 1: KONFİGÜRASYON
# ==========================================================================

VPN_CONFIG_DIR = "vpn_configs"
AUTH_FILE = "vpn_auth.txt"
VPN_USERNAME = "vpnbook"
VPN_PASSWORD = "m34wk9w" # VPNBook şifresi değişirse burayı güncelleyin

current_vpn_process = None

# ==========================================================================
# === BÖLÜM 2: TEMEL VPN YÖNETİM FONKSİYONLARI - GÜNCELLENDİ
# ==========================================================================

def _setup_auth_file():
    """Kullanıcı adı ve şifreyi içeren bir metin dosyası oluşturur."""
    with open(AUTH_FILE, "w") as f:
        f.write(f"{VPN_USERNAME}\n")
        f.write(f"{VPN_PASSWORD}\n")

def _get_current_ip() -> str:
    """Mevcut genel IP adresini döndürür."""
    try:
        return requests.get("https://api.ipify.org", timeout=10).text
    except requests.RequestException:
        return "IP Alinamadi"

def stop_vpn():
    """Mevcut OpenVPN bağlantısını sonlandırır."""
    global current_vpn_process
    logger = get_run_logger()
    if current_vpn_process and current_vpn_process.poll() is None:
        logger.info(f"Aktif OpenVPN süreci (PID: {current_vpn_process.pid}) sonlandırılıyor...")
        subprocess.run(["sudo", "kill", str(current_vpn_process.pid)])
        current_vpn_process.wait()
        current_vpn_process = None
        logger.info("VPN bağlantısı başarıyla sonlandırıldı.")
        time.sleep(3)

def start_vpn() -> bool:
    """`vpn_configs` klasöründen rastgele bir .ovpn dosyası seçer ve yeni bir bağlantı başlatır."""
    global current_vpn_process
    logger = get_run_logger()
    config_files = [f for f in os.listdir(VPN_CONFIG_DIR) if f.endswith(".ovpn")]
    if not config_files:
        logger.error(f"'{VPN_CONFIG_DIR}' klasöründe hiç .ovpn dosyası bulunamadı!")
        return False

    selected_config = random.choice(config_files)
    config_path = os.path.join(VPN_CONFIG_DIR, selected_config)
    logger.info(f"'{config_path}' ile yeni VPN bağlantısı başlatılıyor...")
    _setup_auth_file()

    command = ["sudo", "openvpn", "--config", config_path, "--auth-user-pass", AUTH_FILE]
    try:
        # --- YENİ HATA YAKALAMA MANTIĞI ---
        # OpenVPN sürecini, standart çıktı (stdout) ve standart hata (stderr)
        # kanallarını yakalayacak şekilde başlatıyoruz.
        current_vpn_process = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True  # Çıktıyı metin olarak almak için
        )
        logger.info(f"OpenVPN süreci başlatıldı (PID: {current_vpn_process.pid}). Bağlantı durumu kontrol ediliyor...")
        
        # OpenVPN'in başlaması ve olası bir hatayı hemen vermesi için kısa bir süre bekle.
        time.sleep(5)

        # Süreç bu kısa sürede çöktü mü diye kontrol et.
        if current_vpn_process.poll() is not None:
            # Eğer süreç çöktüyse, hata mesajını oku ve logla.
            error_output = current_vpn_process.stderr.read()
            logger.error(f"OpenVPN BAŞLATILAMADI! Hata: {error_output.strip()}")
            return False

        logger.info("OpenVPN süreci aktif görünüyor. Bağlantının tam olarak kurulması için bekleniyor...")
        time.sleep(10) # Bağlantının tam kurulması için ek süre
        return True
        
    except Exception as e:
        logger.error(f"OpenVPN başlatılırken kritik bir hata oluştu: {e}")
        return False

def switch_ip() -> bool:
    """
    Mevcut VPN bağlantısını durdurur, yeni bir bağlantı başlatır ve
    IP'nin gerçekten değiştiğini doğrular.
    """
    logger = get_run_logger()
    initial_ip = _get_current_ip()
    logger.info(f"Mevcut IP: {initial_ip}. IP değiştirme süreci başlatılıyor...")
    
    stop_vpn()
    
    if not start_vpn():
        logger.error("IP değiştirme süreci başarısız oldu (start_vpn False döndü).")
        return False

    final_ip = _get_current_ip()
    if final_ip == initial_ip or final_ip == "IP Alinamadi":
        logger.error(f"IP DEĞİŞMEDİ! Eski IP: {initial_ip}, Yeni IP: {final_ip}. Bağlantı başarısız olmuş olabilir.")
        stop_vpn() # Başarısız denemeyi sonlandır
        return False
    else:
        logger.info(f"IP değiştirme başarılı. Eski IP: {initial_ip}, Yeni IP: {final_ip}")
        return True

# ==========================================================================
# === BÖLÜM 3: PREFECT GÖREVİ (TASK)
# ==========================================================================

@task(name="VPN IP Değiştirme Görevi")
def switch_ip_task():
    """
    Prefect arayüzünden tetiklenebilen, `switch_ip` fonksiyonunu çağıran
    bir sarmalayıcı (wrapper) görev.
    """
    if not switch_ip():
        raise RuntimeError("VPN bağlantısı kurulamadı veya IP adresi değişmedi.")

# ==========================================================================
# === BÖLÜM 4: DOĞRUDAN ÇALIŞTIRMA (TEST İÇİN)
# ==========================================================================
if __name__ == "__main__":
    @flow
    def test_vpn_flow():
        switch_ip_task()
    test_vpn_flow()
