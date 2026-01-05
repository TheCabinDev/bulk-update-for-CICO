import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
from dotenv import load_dotenv
import getpass  # Library standar untuk input password tersembunyi

# Load konfigurasi dari .env
load_dotenv()

# --- CONFIGURATION DARI .ENV ---
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = "room_prices"

def run_mass_update(csv_file):
    try:
        # 1. Cek keberadaan file CSV
        if not os.path.exists(csv_file):
            print(f"❌ Error: File '{csv_file}' tidak ditemukan!")
            return

        # 2. Baca CSV
        print(f"🚀 Membaca file: {csv_file}")
        df = pd.read_csv(csv_file)

        # 3. Validasi Dasar
        id_cols = ['hotel_id', 'room_id', 'price_category_id']
        price_cols = [f'price_{i}' for i in range(8, 25)]
        required_columns = id_cols + price_cols
        
        if not all(c in df.columns for c in required_columns):
            print(f"❌ Error: Kolom di CSV tidak lengkap!")
            return

        print(f"✅ Validasi File Berhasil. Total data: {len(df)} baris.")

        # 4. KONFIRMASI & INPUT PASSWORD MANUAL
        print("-" * 40)
        confirm = input(f"❓ Lanjutkan update '{csv_file}' ke tabel '{TABLE_NAME}'? (y/n): ")
        
        if confirm.lower() not in ['y', 'yes']:
            print("❌ Proses dibatalkan oleh pengguna.")
            return

        # Masukkan password secara manual (Input tidak akan terlihat di terminal)
        db_password = getpass.getpass(f"🔑 Masukkan Password untuk user '{DB_USER}': ")
        print("-" * 40)

        # 5. Inisialisasi Database Engine dengan password manual
        # Format: mysql+pymysql://user:password@host/dbname
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{db_password}@{DB_HOST}/{DB_NAME}")

        # 6. Proses Bulk Update via Temporary Table
        print("⏳ Menghubungkan dan memproses data ke database...")
        
        with engine.begin() as conn:
            # Step A: Kirim CSV ke tabel temporary
            TEMP_TABLE = "temp_price_sync"
            df.to_sql(TEMP_TABLE, con=conn, if_exists='replace', index=False)
            
            # Step B: Jalankan SQL Update Join
            set_clause = ", ".join([f"t.{col} = tmp.{col}" for col in price_cols])
            
            sql_query = f"""
                UPDATE {TABLE_NAME} t
                INNER JOIN {TEMP_TABLE} tmp ON 
                    t.hotel_id = tmp.hotel_id AND 
                    t.room_id = tmp.room_id AND 
                    t.price_category_id = tmp.price_category_id
                SET 
                    {set_clause},
                    t.updated_at = NOW();
            """
            
            result = conn.execute(text(sql_query))
            print(f"🎉 SUKSES! {result.rowcount} baris berhasil diperbarui.")

            # Step C: Hapus tabel temporary
            conn.execute(text(f"DROP TABLE IF EXISTS {TEMP_TABLE};"))

    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")
        print("💡 Tip: Pastikan password yang Anda masukkan benar.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Gunakan format: python massUpdate.py <nama_file.csv>")
    else:
        run_mass_update(sys.argv[1])