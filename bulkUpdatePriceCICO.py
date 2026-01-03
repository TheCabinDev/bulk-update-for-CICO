import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
from dotenv import load_dotenv # Membutuhkan: pip install python-dotenv

# Load konfigurasi dari file .env
load_dotenv()

# --- 1. KONFIGURASI DATABASE ---
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

# Koneksi Database
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

def run_update(csv_file):
    try:
        if not os.path.exists(csv_file):
            print(f"❌ Error: File '{csv_file}' tidak ditemukan!")
            return

        print(f"🚀 Membaca file {csv_file}...")
        df = pd.read_csv(csv_file)

        # --- 2. VALIDASI DATA ---
        id_cols = ['hotel_id', 'room_id', 'price_category_id']
        price_cols = [f'price_{i}' for i in range(8, 25)]
        required_columns = id_cols + price_cols
        
        if not all(col in df.columns for col in required_columns):
            print("❌ Error: Kolom di CSV tidak sesuai dengan struktur tabel!")
            return

        if (df[price_cols] < 0).any().any():
            print("❌ Error: Ditemukan harga bernilai negatif!")
            return

        print(f"✅ Validasi berhasil. Total: {len(df)} baris.")

        # --- 3. KONFIRMASI ---
        confirm = input(f"❓ Lanjutkan update '{csv_file}' ke database? (y/n): ")
        if confirm.lower() not in ['y', 'yes']:
            print("❌ Dibatalkan.")
            return

        # --- 4. PROSES DATABASE ---
        with engine.begin() as conn:
            TEMP_TABLE = "temp_batch_update"
            df.to_sql(TEMP_TABLE, con=conn, if_exists='replace', index=False)
            
            set_clause = ", ".join([f"t.{col} = tmp.{col}" for col in price_cols])
            query = f"""
            UPDATE room_prices t
            INNER JOIN {TEMP_TABLE} tmp ON 
                t.hotel_id = tmp.hotel_id AND 
                t.room_id = tmp.room_id AND 
                t.price_category_id = tmp.price_category_id
            SET {set_clause}, t.updated_at = NOW();
            """
            
            result = conn.execute(text(query))
            print(f"🎉 Sukses! {result.rowcount} baris diperbarui.")
            conn.execute(text(f"DROP TABLE IF EXISTS {TEMP_TABLE};"))

    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Penggunaan: python bulk_update_prices.py nama_file.csv")
    else:
        run_update(sys.argv[1])
