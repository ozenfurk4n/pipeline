#!/usr/bin/env python3
# test_uk_vpn.py - UK VPN Konfigürasyon Test Scripti

import sys
import os
import time
import subprocess
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.vpn_manager import _get_current_ip, _setup_auth_file

def test_uk_vpn_configs():
    print("=== UK VPN Konfigürasyon Testi ===")
    
    # 1. Mevcut IP'yi göster
    current_ip = _get_current_ip()
    print(f"1. Mevcut IP: {current_ip}")
    
    # 2. Auth dosyasını oluştur
    print("2. Auth dosyası oluşturuluyor...")
    _setup_auth_file()
    
    # 3. UK VPN konfigürasyonlarını listele
    uk_configs = [
        "vpnbook-uk205-tcp80.ovpn",      # TCP port 80 (en hızlı)
        "vpnbook-uk205-tcp443.ovpn",     # TCP port 443 (en güvenli)
        "vpnbook-uk205-udp53.ovpn",      # UDP port 53 (alternatif)
        "vpnbook-uk205-udp25000.ovpn"    # UDP port 25000 (alternatif)
    ]
    
    print(f"3. Test edilecek UK VPN konfigürasyonları:")
    for i, config in enumerate(uk_configs, 1):
        config_path = os.path.join("vpn_configs", config)
        if os.path.exists(config_path):
            print(f"   {i}. ✅ {config}")
        else:
            print(f"   {i}. ❌ {config} (bulunamadı)")
    
    # 4. Her konfigürasyonu test et
    print("\n4. VPN Konfigürasyonları Test Ediliyor:")
    print("   Not: Her test için Ctrl+C ile VPN'i durdurun")
    
    for i, config in enumerate(uk_configs, 1):
        config_path = os.path.join("vpn_configs", config)
        if not os.path.exists(config_path):
            continue
            
        print(f"\n   --- Test {i}: {config} ---")
        print(f"   Komut: sudo openvpn --config {config_path} --auth-user-pass vpn_auth.txt")
        print(f"   Beklenen süre: {'10' if 'tcp80' in config else '15' if 'tcp443' in config else '20'} saniye")
        print(f"   Test etmek için yukarıdaki komutu çalıştırın")
        print(f"   IP değişimini kontrol etmek için yeni terminal açın: curl -s https://api.ipify.org")
        
        # Kullanıcıdan onay al
        response = input(f"\n   {config} ile devam etmek istiyor musunuz? (y/n): ")
        if response.lower() != 'y':
            print(f"   {config} atlandı.")
            continue
        
        # VPN başlat
        try:
            command = [
                "sudo", "openvpn",
                "--config", config_path,
                "--auth-user-pass", "vpn_auth.txt",
                "--verb", "1"
            ]
            
            print(f"   VPN başlatılıyor...")
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # 30 saniye bekle
            time.sleep(30)
            
            # VPN'i durdur
            process.terminate()
            process.wait()
            print(f"   {config} test tamamlandı.")
            
        except KeyboardInterrupt:
            print(f"   {config} test kullanıcı tarafından durduruldu.")
        except Exception as e:
            print(f"   {config} test hatası: {e}")
    
    print("\n=== Test Tamamlandı ===")
    print("Artık pipeline'ı çalıştırabilirsiniz!")
    print("UK VPN konfigürasyonları otomatik olarak test edilecek.")

if __name__ == "__main__":
    test_uk_vpn_configs()
