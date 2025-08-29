# -- coding: utf-8 --
"""
PostgreSQL erişim katmanı.
Bu modül, pipeline'ın diğer tüm script'leri tarafından kullanılan, veritabanı ile
iletişim kuran merkezi bir kütüphanedir. Görevi, veri okuma, yazma ve tablo
yönetimi gibi işlemleri standart ve yeniden kullanılabilir fonksiyonlar
aracılığıyla sağlamaktır. İş mantığı içermez, sadece veritabanı operasyonlarına
odaklanır.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from prefect import get_run_logger
from sqlalchemy import MetaData, Table, insert

# ==========================================================================
# === BÖLÜM 1: KONFİGÜRASYON
# Bu bölümde, veritabanı bağlantı bilgileri ve kullanılacak tüm tablo
# isimleri merkezi olarak tanımlanır. Bir değişiklik gerektiğinde sadece
# bu alanı güncellemek yeterlidir.
# ==========================================================================

# --- BAĞLANTI AYARI (tek yer) ---
DB_URL = "postgresql+psycopg2://postgres:prolegal2314++8.@localhost:5432/pipeline"
SCHEMA = "public"

# --- TABLO ADLARI ---
# Kaynak Tablolar
T_TUM_CLEAN = f'{SCHEMA}."tum_clean"'
T_TKGM_TUM  = f'{SCHEMA}."tkgm_tum"'

# Ara Sonuç Tabloları (compare.py tarafından kullanılır)
T_MATCH = f'{SCHEMA}."eslesti_tkgm_sql"'
T_MISS  = f'{SCHEMA}."eslesmedi_tkgm_sql"'

# Ara Sonuç Tabloları (diğer script'ler tarafından kullanılır)
T_POLY_OK   = f'{SCHEMA}."eslesmedi_poly_ok"' # eslesmedi_catch.py çıktısı
T_AFTER_1N  = f'{SCHEMA}."after_1n_sql"'      # 1n_funct.py çıktısı
T_MAH_GEOM  = f'{SCHEMA}."tkgm_mahalle_geom"' # Mahalle geometrileri için cache

# Faz 2: Hata Yönetimi Tabloları
T_LOGS      = f'{SCHEMA}."pipeline_logs"'
T_ERRORS    = f'{SCHEMA}."second_check_still_error"'

# ==========================================================================
# === BÖLÜM 2: TEMEL FONKSİYONLAR VE TABLO YÖNETİMİ
# ==========================================================================

def engine():
    """
    Veritabanına bağlantı kurmak için bir SQLAlchemy 'engine' nesnesi oluşturur.
    `pool_pre_ping=True` parametresi, uzun süre açık kalan bağlantılarda oluşabilecek
    "bağlantı koptu" hatalarını önlemeye yardımcı olur.
    """
    return create_engine(DB_URL, pool_pre_ping=True)

def ensure_tables():
    """
    Pipeline'ın çalışması için gerekli olan tüm tabloların veritabanında
    var olduğundan emin olur. Eğer bir tablo yoksa, onu oluşturur.
    Bu fonksiyon, pipeline ilk kez kurulduğunda veya veritabanı sıfırlandığında
    çok kullanışlıdır.
    """
    eng = engine()
    
    # NOT: eslesti_tkgm_sql ve eslesmedi_tkgm_sql tabloları, pandas'ın `to_sql`
    # fonksiyonu tarafından `tum_clean` tablosunun yapısı kopyalanarak
    # otomatik olarak oluşturulur. Bu yüzden burada manuel olarak yaratmıyoruz.
    
    ddl_commands = [
        # TKGM'den çekilen idari yapı verilerini tutan tablo.
        f"""CREATE TABLE IF NOT EXISTS {T_TKGM_TUM} (
            il_id       integer, il_ad       varchar(255),
            ilce_id     integer, ilce_ad     varchar(255),
            mahalle_id  bigint,  mahalle_ad  varchar(255)
        );""",
        # eslesmedi_catch.py'nin, poligon geometrisiyle kurtardığı kayıtları yazdığı tablo.
        f"""CREATE TABLE IF NOT EXISTS {T_POLY_OK} (
            orig_id             BIGINT PRIMARY KEY,
            il                  VARCHAR(255), ilce                VARCHAR(255),
            mahalle_txt         VARCHAR(255), mahalle_id          BIGINT,
            ada                 INTEGER,      parsel              INTEGER,
            tkgm_properties     JSONB,        tkgm_geometry       JSONB,
            input_polygon_wkb   BYTEA,        created_at          TIMESTAMPTZ DEFAULT now()
        );""",
        # 1n_funct.py'nin, parsellerin evrim analiz sonuçlarını yazdığı tablo.
        f"""CREATE TABLE IF NOT EXISTS {T_AFTER_1N} (
            orig_id             BIGINT,       new_id              BIGINT,
            il                  VARCHAR(255), ilce                VARCHAR(255),
            mahalle             VARCHAR(255), mahalle_id          BIGINT,
            ada                 INTEGER,      parsel              INTEGER,
            bucket              VARCHAR(50),  tkgm_properties     JSONB,
            -- ... ihtiyaç duyulacak diğer kolonlar buraya eklenebilir ...
            created_at          TIMESTAMPTZ DEFAULT now()
        );""",
        # Mahalle poligonlarını tekrar tekrar API'den çekmemek için önbellek (cache) tablosu.
        f"""CREATE TABLE IF NOT EXISTS {T_MAH_GEOM} (
            ilce_id     BIGINT NOT NULL,      mahalle_id  BIGINT NOT NULL,
            mahalle_ad  VARCHAR(255),         geom        geometry(MultiPolygon, 4326),
            PRIMARY KEY (ilce_id, mahalle_id)
        );""",
        # Faz 2: Tüm pipeline adımlarının loglarını tutan merkezi tablo.
        f"""CREATE TABLE IF NOT EXISTS {T_LOGS} (
            log_id              SERIAL PRIMARY KEY,
            run_id              VARCHAR(255), script_adi          VARCHAR(255),
            parametreler        JSONB,        baslangic_zamani    TIMESTAMPTZ,
            bitis_zamani        TIMESTAMPTZ,  durum               VARCHAR(50),
            mesaj               TEXT,         retry_count         INTEGER DEFAULT 0
        );""",
        # Faz 2: Otomatik yeniden denemelere rağmen çözülemeyen inatçı hataların kaydedildiği tablo.
        f"""CREATE TABLE IF NOT EXISTS {T_ERRORS} (
            error_id            SERIAL PRIMARY KEY,
            original_log_id     INTEGER,      script_adi          VARCHAR(255),
            parametreler        JSONB,        son_hata_mesaji     TEXT,
            deneme_sayisi       INTEGER,      raporlanma_zamani   TIMESTAMPTZ DEFAULT now()
        );"""
    ]
    
    with eng.begin() as con:
        for ddl in ddl_commands:
            con.execute(text(ddl))
        # PostGIS'in geometrik sorguları hızlı yapabilmesi için GIST indeksi oluştur.
        con.execute(text(f'CREATE INDEX IF NOT EXISTS idx_tkgm_mh_geom_gist ON {T_MAH_GEOM} USING GIST(geom);'))

# ==========================================================================
# === BÖLÜM 3: VERİ OKUMA (SELECT) FONKSİYONLARI
# ==========================================================================

def list_unique_iller() -> pd.DataFrame:
    """`tum_clean` tablosundaki tüm benzersiz il isimlerini bir DataFrame olarak döndürür."""
    eng = engine()
    return pd.read_sql(
        f'SELECT DISTINCT "IlBilgisi" AS il FROM {T_TUM_CLEAN} ORDER BY "IlBilgisi"', eng
    )

def load_unmatched_for_il(il_name: str) -> pd.DataFrame:
    """`eslesmedi_catch.py` için, belirtilen ildeki eşleşmemiş kayıtları çeker."""
    eng = engine()
    # `eslesmedi_tkgm_sql` tablosu `tum_clean` ile aynı yapıya sahip olduğu için
    # tüm kolonları seçmek yeterlidir.
    q = text(f'SELECT * FROM {T_MISS} WHERE "IlBilgisi" = :il')
    return pd.read_sql(q, eng, params={"il": il_name})

def load_matched_for_il(il_name: str) -> pd.DataFrame:
    """`1n_funct.py` için, belirtilen ildeki eşleşmiş kayıtları çeker."""
    eng = engine()
    q = text(f'SELECT * FROM {T_MATCH} WHERE "IlBilgisi" = :il')
    return pd.read_sql(q, eng, params={"il": il_name})

def tkgm_rows_for_il(il_name: str) -> pd.DataFrame:
    """`compare.py` için, belirtilen ilin önbelleklenmiş TKGM verilerini çeker."""
    eng = engine()
    q = text(f"""
        SELECT il_ad AS tkgm_il, ilce_ad AS tkgm_ilce, mahalle_ad AS tkgm_mahalle,
               mahalle_id::bigint AS tkgm_mahalle_id
        FROM {T_TKGM_TUM} WHERE il_ad = :il
    """)
    return pd.read_sql(q, eng, params={"il": il_name})

def tkgm_has_il(il_name: str) -> bool:
    """Veritabanında belirtilen il için TKGM verisi olup olmadığını kontrol eder."""
    eng = engine()
    q = text(f'SELECT 1 FROM {T_TKGM_TUM} WHERE il_ad = :il LIMIT 1')
    return not pd.read_sql(q, eng, params={"il": il_name}).empty

# ==========================================================================
# === BÖLÜM 4: VERİ YAZMA (INSERT/DELETE) FONKSİYONLARI
# ==========================================================================

def wipe_tkgm_by_ilid(il_id: int) -> None:
    """Yeni veri çekmeden önce, bir ilin eski TKGM verilerini siler."""
    eng = engine()
    with eng.begin() as con:
        con.execute(text(f'DELETE FROM {T_TKGM_TUM} WHERE il_id = :i'), {"i": il_id})

def insert_tkgm_rows(df: pd.DataFrame) -> None:
    """API'den çekilen yeni TKGM verilerini tabloya yazar."""
    if df.empty: return
    eng = engine()
    df.to_sql(T_TKGM_TUM.split(".")[-1].strip('"'), eng, schema=SCHEMA, if_exists="append", index=False)

def clear_compare_results_for_il(il_name: str) -> None:
    """Bir il için yeniden çalıştırma yapmadan önce eski eşleşti/eşleşmedi sonuçlarını siler."""
    eng = engine()
    with eng.begin() as con:
        con.execute(text(f'DELETE FROM {T_MATCH} WHERE "IlBilgisi" = :x'), {"x": il_name})
        con.execute(text(f'DELETE FROM {T_MISS}  WHERE "IlBilgisi" = :x'), {"x": il_name})

def insert_mahalle_geometries(df: pd.DataFrame) -> None:
    """Mahalle geometri verilerini `tkgm_mahalle_geom` tablosuna yazar."""
    if df.empty: return
    eng = engine()
    logger = get_run_logger()
    
    try:
        # Her satır için UPSERT yap
        with eng.begin() as con:
            for _, row in df.iterrows():
                # Geometri verisini hazırla
                geom_data = {
                    "ilce_id": row["ilce_id"],
                    "mahalle_id": row["mahalle_id"],
                    "mahalle_ad": row["mahalle_ad"],
                    "geom": f"SRID=4326;{row['geom']}"  # PostGIS SRID ekle
                }
                
                # UPSERT sorgusu - varsa güncelle, yoksa ekle
                upsert_sql = text(f"""
                    INSERT INTO {T_MAH_GEOM} (ilce_id, mahalle_id, mahalle_ad, geom)
                    VALUES (:ilce_id, :mahalle_id, :mahalle_ad, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(:geom), 4326)))
                    ON CONFLICT (ilce_id, mahalle_id) 
                    DO UPDATE SET 
                        mahalle_ad = EXCLUDED.mahalle_ad,
                        geom = EXCLUDED.geom
                """)
                
                con.execute(upsert_sql, geom_data)
        
        logger.info(f"{len(df)} adet mahalle geometrisi başarıyla yüklendi/güncellendi.")
        
    except Exception as e:
        logger.error(f"Geometri verileri yazılırken hata: {e}")
        raise

def insert_matches(df: pd.DataFrame) -> None:
    """`compare.py`'nin bulduğu eşleşen kayıtları tabloya yazar."""
    if df.empty: return
    eng = engine()
    df.to_sql(T_MATCH.split(".")[-1].strip('"'), eng, schema=SCHEMA, if_exists="append", index=False)

def insert_misses(df: pd.DataFrame) -> None:
    """`compare.py`'nin bulduğu eşleşmeyen kayıtları tabloya yazar."""
    if df.empty: return
    eng = engine()
    df.to_sql(T_MISS.split(".")[-1].strip('"'), eng, schema=SCHEMA, if_exists="append", index=False)

def insert_poly_ok(df: pd.DataFrame) -> None:
    """`eslesmedi_catch.py`'nin kurtardığı kayıtları tabloya yazar."""
    if df.empty: return
    eng = engine()
    df.to_sql(T_POLY_OK.split(".")[-1].strip('"'), eng, schema=SCHEMA, if_exists="append", index=False)

def insert_after_1n(df: pd.DataFrame) -> None:
    """`1n_funct.py`'nin analiz sonuçlarını tabloya yazar."""
    if df.empty: return
    eng = engine()
    df.to_sql(T_AFTER_1N.split(".")[-1].strip('"'), eng, schema=SCHEMA, if_exists="append", index=False)
