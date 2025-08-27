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

# --- DİKKAT: ÇOK ÖNEMLİ ---
# VPNBook sitesindeki şifre birkaç saatte bir değişir.
# Pipeline'ı çalıştırmadan önce https://www.vpnbook.com/freevpn adresine gidip
# güncel şifreyi alıp aşağıdaki tırnak işaretlerinin arasına yapıştırmanız GEREKİR.
VPN_PASSWORD = "m34wk9w" # <--- GÜNCEL ŞİFREYİ BURAYA GİRİN

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

def _log_vpn_output(process: subprocess.Popen, reason: str):
    """
    Sonlandırılmış bir OpenVPN sürecinin çıktı ve hata loglarını okur.
    Bu, bağlantının neden başarısız olduğunu anlamak için kritik öneme sahiptir.
    """
    logger = get_run_logger()
    logger.warning(f"OpenVPN süreci '{reason}' nedeniyle sonlandırıldı. Çıktılar okunuyor...")
    try:
        stdout, stderr = process.communicate(timeout=5)
        if stdout:
            logger.info(f"OpenVPN Standart Çıktısı:\n---BEGIN VPN STDOUT---\n{stdout.strip()}\n---END VPN STDOUT---")
        if stderr:
            logger.error(f"OpenVPN Hata Çıktısı:\n---BEGIN VPN STDERR---\n{stderr.strip()}\n---END VPN STDERR---")
    except Exception as e:
        logger.error(f"OpenVPN çıktısı okunurken hata oluştu: {e}")


def stop_vpn():
    """Mevcut OpenVPN bağlantısını sonlandırır."""
    global current_vpn_process
    logger = get_run_logger()
    if current_vpn_process and current_vpn_process.poll() is None:
        logger.info(f"Aktif OpenVPN süreci (PID: {current_vpn_process.pid}) sonlandırılıyor...")
        subprocess.run(["sudo", "kill", str(current_vpn_process.pid)])
        current_vpn_process.wait()
        _log_vpn_output(current_vpn_process, "normal kapatma")
        current_vpn_process = None
        logger.info("VPN bağlantısı başarıyla sonlandırıldı.")
        time.sleep(3)

def start_vpn() -> bool:
    """Çalışan İngiltere VPN konfigürasyonlarından birini seçer ve bağlantı başlatır."""
    global current_vpn_process
    logger = get_run_logger()
    
    # Sadece çalışan UK VPN konfigürasyonu kullan
    selected_config = "vpnbook-uk205-tcp80.ovpn"
    config_path = os.path.join(VPN_CONFIG_DIR, selected_config)
    
    logger.info(f"'{config_path}' ile yeni UK VPN bağlantısı başlatılıyor...")
    _setup_auth_file()

    # --- YENİ KOMUT ---
    # --pull-filter ignore "redirect-gateway" parametresi, macOS'ta yaygın olan
    # "Can't assign requested address" yönlendirme (routing) hatasını engellemeye
    # yardımcı olur. OpenVPN'e "ana internet rotasını değiştirme" talimatını
    # görmezden gelmesini söyler.
    command = [
        "sudo", "openvpn",
        "--config", config_path,
        "--auth-user-pass", AUTH_FILE,
        "--verb", "3",
        "--pull-filter", "ignore", "redirect-gateway"
    ]
    try:
        current_vpn_process = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info(f"OpenVPN süreci başlatıldı (PID: {current_vpn_process.pid}). Bağlantı durumu kontrol ediliyor...")
        time.sleep(5)

        if current_vpn_process.poll() is not None:
            _log_vpn_output(current_vpn_process, "başlatma hatası")
            return False

        logger.info("OpenVPN süreci aktif görünüyor. Bağlantının tam olarak kurulması için bekleniyor...")
        time.sleep(10)
        return True
        
    except Exception as e:
        logger.error(f"OpenVPN başlatılırken kritik bir hata oluştu: {e}")
        return False

def switch_ip() -> bool:
    """
    Mevcut VPN bağlantısını durdurur, sadece çalışan İngiltere VPN ile yeni bağlantı başlatır ve
    IP'nin gerçekten değiştiğini doğrular.
    """
    logger = get_run_logger()
    initial_ip = _get_current_ip()
    logger.info(f"Mevcut IP: {initial_ip}. IP değiştirme süreci başlatılıyor...")
    
    stop_vpn()
    
    if not start_vpn():
        logger.error("IP değiştirme süreci başarısız oldu (start_vpn False döndü).")
        return False

    # İngiltere VPN için daha uzun bekleme süresi
    logger.info("İngiltere VPN bağlantısının kurulması bekleniyor...")
    
    # Port tipine göre bekleme süresi
    if "tcp80" in current_vpn_process.args:
        time.sleep(10)  # TCP 80 - hızlı
    elif "tcp443" in current_vpn_process.args:
        time.sleep(15)  # TCP 443 - güvenli, biraz yavaş
    elif "udp" in current_vpn_process.args:
        time.sleep(20)  # UDP - en yavaş ama güvenilir
    else:
        time.sleep(15)  # Varsayılan

    final_ip = _get_current_ip()
    if final_ip == initial_ip or final_ip == "IP Alinamadi":
        logger.error(f"IP DEĞİŞMEDİ! Eski IP: {initial_ip}, Yeni IP: {final_ip}. Bağlantı başarısız olmuş olabilir.")
        # Başarısız olan sürecin loglarını alıp sonlandırıyoruz.
        stop_vpn()
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
