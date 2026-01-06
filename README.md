# Hotel Room Price Mass CICO Updater 🏨💰

Script Python untuk melakukan pembaruan harga hotel cabin dengan konsep CICO (check in check out freely) secara massal dari file CSV ke database MySQL. Menggunakan metode **Bulk Update via Temporary Table** untuk menjamin kecepatan tinggi dan integritas data.

## 🛠 Fitur Utama
- **Keamanan Tinggi:** Password database tidak disimpan di file; diinput manual saat runtime.
- **Validasi Data:** Mengecek format kolom dan harga negatif sebelum menyentuh database.
- **Performa Cepat:** Menggunakan SQL Join (Bulk) daripada looping baris-per-baris.
- **Aman:** Menggunakan transaksi database (commit/rollback otomatis).

---

## 📋 Persiapan (Prerequisites)

1. **Python 3.x** terinstal di sistem.
2. **Database MySQL/MariaDB** dengan akses untuk membuat tabel sementara.
3. **Virtual Environment** (Sangat disarankan).

### Instalasi Library
Jalankan perintah berikut untuk menginstal dependensi yang dibutuhkan:
```bash
pip install -r requirements.txt

## 🚀 Cara Menjalankan Script

Ikuti langkah-langkah di bawah ini untuk memastikan script berjalan dengan benar di lingkungan lokal maupun server:

### 1. Persiapan Lingkungan (First Time Only)
Pastikan Anda berada di dalam folder project, lalu buat dan aktifkan Virtual Environment:
```bash
# Membuat environment
python -m venv .venv

# Mengaktifkan environment
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

# Menginstal dependensi
pip install -r requirements.txt

# Menjalankan script
python massUpdate.py <nama_file.csv>
```


