#!/usr/bin/env python
# coding: utf-8

# In[31]:


import pandas as pd
import numpy as np
import json


# In[32]:


import os
print(os.getcwd())   # Şu anki çalışma dizinini gösterir
print(os.listdir())


# In[35]:


with open("/Users/furkanozen/Documents/GitHub/pipeline/datas/tumVeri.json", "r", encoding="utf-8") as f:
    data = json.load(f)

table_obj = next(item for item in data if item.get("type") == "table")
rows = table_obj["data"]

df = pd.DataFrame(rows)

df.to_csv("tum_veri.csv", index=False, encoding="utf-8")


# In[36]:


df.head()


# In[ ]:


df.columns


# In[ ]:


# kolon isimlerini bi güncelle
rename_map = {
    "İlBilgisi": "IlBilgisi",
    "İlceBilgisi": "IlceBilgisi",
}

df = df.rename(columns=rename_map)


# In[ ]:


df.columns


# In[ ]:


df.to_csv("tum_veri.csv", index=False, encoding="utf-8")


# In[ ]:


df.head()


# # postgre tum veri tablosu için
# 
# 
# * bağlanalım
# brew install libpq
# echo 'export PATH="/opt/homebrew/opt/libpq/bin:$PATH"' >> ~/.zshrc
# source ~/.zshrc
# psql --version
# 
# * bağla
# psql "postgresql://postgres:prolegal2314++8.@localhost:5432/pipeline"
# 
# 
# DROP TABLE IF EXISTS public.tum_clean;
# 
# CREATE TABLE public.tum_clean (
#   Id                     integer,
#   IlBilgisi              varchar(255),
#   IlceBilgisi            varchar(255),
#   MahalleBilgisi         varchar(255),
#   AdaBilgisi             integer,
#   ParselBilgisi          integer,
#   YuzolcumBilgisi        varchar(255),
#   AnaTasinmazNitelik     varchar(255),
#   Hisse                  double precision,
#   mahalle_id             integer,
#   il_id                  smallint,
#   ilce_id                smallint,
#   polygon                geometry,
#   AnaTasinmazNitelik_1   varchar(255),
#   AnaTasinmazNitelik_2   varchar(255),
#   AnaTasinmazNitelik_2_yedek varchar(255),
#   Durum                  varchar(50)
# );
# 
# 
# Tüm satırların SRID’si 4326 olarak set edildi.
# 
# * postgis 
# -- === polygon_hex -> geometry (güvenli, toplu) ===
# BEGIN;
# 
# -- 1) (zaten aktif ama kalsın) PostGIS
# CREATE EXTENSION IF NOT EXISTS postgis;
# 
# -- 2) polygon kolonu her geometri tipini kabul edecek şekilde dursun
# ALTER TABLE public."tum_clean"
#   ALTER COLUMN "polygon" TYPE geometry(Geometry, 4326)
#   USING ST_SetSRID("polygon", 4326);
# 
# -- 3) hex'i temizle -> geçerli olanları süz -> geometry'ye yaz
# WITH cleaned AS (
#   SELECT
#     "Id",
#     -- 0x önekini kaldır
#     REGEXP_REPLACE("polygon_hex", '^\s*0x', '', 'i') AS hex_no_prefix
#   FROM public."tum_clean"
#   WHERE "polygon" IS NULL
#     AND "polygon_hex" IS NOT NULL
# ),
# normalized AS (
#   SELECT
#     "Id",
#     -- aradaki boşluk/garip karakterleri tamamen sil, sadece [0-9A-Fa-f] kalsın
#     REGEXP_REPLACE(hex_no_prefix, '[^0-9A-Fa-f]', '', 'g') AS hex_clean
#   FROM cleaned
# ),
# filtered AS (
#   SELECT
#     "Id",
#     hex_clean
#   FROM normalized
#   WHERE hex_clean IS NOT NULL
#     -- ilk bayt endian bayrağı: 00 (big) veya 01 (little) olsun
#     AND SUBSTRING(hex_clean FOR 2) IN ('00','01')
#     -- geçerli hex için uzunluk çift sayı olsun
#     AND (LENGTH(hex_clean) % 2) = 0
#     -- en azından anlamlı bir WKB gövdesi olsun
#     AND LENGTH(hex_clean) >= 18
# )
# UPDATE public."tum_clean" t
# SET "polygon" = ST_SetSRID(
#                   ST_GeomFromWKB(DECODE(f.hex_clean, 'hex')),
#                   4326
#                 )
# FROM filtered f
# WHERE t."Id" = f."Id";
# 
# -- 4) mekânsal indeks
# CREATE INDEX IF NOT EXISTS tum_clean_geom_idx
#   ON public."tum_clean" USING GIST ("polygon");
# 
# COMMIT;
# 
# -- 5) özet ve tip dağılımı
# SELECT COUNT(*) AS toplam, COUNT("polygon") AS geometrili
# FROM public."tum_clean";
# 
# SELECT ST_GeometryType("polygon") AS gtype, COUNT(*)
# FROM public."tum_clean"
# WHERE "polygon" IS NOT NULL
# GROUP BY 1
# ORDER BY 2 DESC;
# 
# SELECT ST_SRID("polygon") AS srid, COUNT(*)
# FROM public."tum_clean"
# WHERE "polygon" IS NOT NULL
# GROUP BY 1; 
# 
# 
# 
# 1) (En pratik) QGIS ile doğrudan bağlan ve haritada gör
# 	1.	QGIS’i kur.
# 	2.	QGIS → Data Source Manager → PostgreSQL.
# 	3.	New…
# 	•	Name: pipeline
# 	•	Host: localhost
# 	•	Port: 5432
# 	•	Database: pipeline
# 	•	User: postgres
# 	•	Password: (seninki)
# 	4.	Bağlan → public.“tum_clean” katmanını ekle.
# QGIS otomatik olarak polygon geometrisini okuyup Türkiye haritası üzerinde çizer.
# 
# İpucu (tip karışıklığı varsa): sadece alan geometrilerini göstermek için bir view oluştur:
# 
# 2) pgAdmin Geometry Viewer
# 
# pgAdmin’de zaten “Geometry Viewer” sekmesini gördün. Oradan da katmanı açıp yakınlaş-uzaklaş yapabilirsin; ama stil/analiz için QGIS daha güçlü.
# 

# In[ ]:


data = pd.read_csv("tum_veri.csv", low_memory=False)
data.head()


# # hem eslesti hem eslesmedi kontrol scripti
# 
# DELETE FROM public."eslesmedi_tkgm_sql" m
# USING  public."eslesti_tkgm_sql"  s
# WHERE m.id IS NOT NULL
#   AND s.id = m.id;

# # çalıştırmak için 
# 
# * python3 -m functions.compare
# 
# 
# * python3 -m functions.1n_func

# In[ ]:


data

# PostgreSQL'e veri yükleme
print("🚀 PostgreSQL'e veri yükleniyor...")

# DB_postgre modülünü import et
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import DB_postgre.DB_postgre as DB

# Tabloları oluştur
print("📊 Tablolar oluşturuluyor...")
DB.ensure_tables()

# Ana tabloyu oluştur (eğer yoksa)
ddl_tum_clean = """
CREATE TABLE IF NOT EXISTS public.tum_clean (
    "Id" integer,
    "IlBilgisi" varchar(255),
    "IlceBilgisi" varchar(255),
    "MahalleBilgisi" varchar(255),
    "AdaBilgisi" integer,
    "ParselBilgisi" integer,
    "YuzolcumBilgisi" varchar(255),
    "AnaTasinmazNitelik" varchar(255),
    "Hisse" double precision,
    "mahalle_id" integer,
    "il_id" smallint,
    "ilce_id" smallint
);
"""

# Tabloyu oluştur
eng = DB.engine()
with eng.begin() as con:
    con.execute(DB.text(ddl_tum_clean))
print("✅ tum_clean tablosu oluşturuldu")

# Veriyi PostgreSQL'e yükle
print(f"📥 {len(df)} satır veri yükleniyor...")
df.to_sql("tum_clean", eng, schema="public", if_exists="replace", index=False)

print("🎉 Veri yükleme tamamlandı!")

# Yüklenen veriyi kontrol et
print("\n📊 Yüklenen veri özeti:")
print(f"  • Toplam satır: {len(df)}")
print(f"  • Toplam kolon: {len(df.columns)}")
print(f"  • Kolonlar: {list(df.columns)}")

# İlk 5 satırı göster
print("\n🔍 İlk 5 satır:")
print(df.head())

print("\n✨ Transform işlemi tamamlandı!")

