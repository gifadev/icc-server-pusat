import asyncio
import websockets
import json
import psycopg2
import psycopg2.extras
from database_config import get_db_connection

def to_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

def to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# Fungsi untuk upsert device berdasarkan serial_number
def upsert_device(cur, device):
    query = """
    INSERT INTO devices (serial_number, ip, is_connected, created_at)
    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (serial_number) DO UPDATE SET
      ip = EXCLUDED.ip,
      is_connected = EXCLUDED.is_connected;
    """
    cur.execute(query, (
        device["serial_number"],
        device["ip"],
        bool(device["is_connected"])
    ))


# Fungsi untuk memasukkan data GSM
def insert_gsm_data(cur, campaign_id, device_id, gsm_list):
    query = """
    INSERT INTO gsm_data (
        campaign_id,
        device_id,
        mcc,
        mnc,
        operator,
        local_area_code,
        arfcn,
        cell_identity,
        rxlev,
        rxlev_access_min,
        status,
        rssi,
        created_at
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, CURRENT_TIMESTAMP
    )
    ON CONFLICT (campaign_id, mcc, mnc, local_area_code, cell_identity)
    DO UPDATE SET
        operator          = EXCLUDED.operator,
        arfcn             = EXCLUDED.arfcn,
        rxlev             = EXCLUDED.rxlev,
        rxlev_access_min  = EXCLUDED.rxlev_access_min,
        status            = EXCLUDED.status,
        rssi              = EXCLUDED.rssi,
        created_at        = CURRENT_TIMESTAMP;
    """

    for gsm in gsm_list:
        raw_mcc = gsm.get("mcc")
        raw_mnc = gsm.get("mnc")

        #jika mcc dan mnc nya kosong tidak disimpan ke db
        if not raw_mcc and not raw_mnc:
            continue

        status_value = True if gsm.get("status") is None else bool(gsm["status"])
        cur.execute(query, (
            campaign_id,
            device_id,
            to_int(gsm.get("mcc")),
            to_int(gsm.get("mnc")),
            gsm.get("operator"),
            to_int(gsm.get("local_area_code")),
            to_int(gsm.get("arfcn")),
            to_int(gsm.get("cell_identity")),
            to_int(gsm.get("rxlev")),
            to_float(gsm.get("rxlev_access_min")),
            status_value,
            to_float(gsm.get("rssi"))
        ))


# Fungsi untuk memasukkan data LTE
def insert_lte_data(cur, campaign_id, device_id, lte_list):
    query = """
    INSERT INTO lte_data (
        campaign_id,
        device_id,
        mcc,
        mnc,
        operator,
        arfcn,
        cell_identity,
        tracking_area_code,
        frequency_band_indicator,
        signal_level,
        snr,
        rx_lev_min,
        status,
        rssi,
        created_at
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, CURRENT_TIMESTAMP
    )
    ON CONFLICT (campaign_id, mcc, mnc, tracking_area_code, cell_identity)
    DO UPDATE SET
        operator                  = EXCLUDED.operator,
        arfcn                     = EXCLUDED.arfcn,
        frequency_band_indicator  = EXCLUDED.frequency_band_indicator,
        signal_level              = EXCLUDED.signal_level,
        snr                       = EXCLUDED.snr,
        rx_lev_min                = EXCLUDED.rx_lev_min,
        status                    = EXCLUDED.status,
        rssi                      = EXCLUDED.rssi,
        created_at                = CURRENT_TIMESTAMP;
    """

    for lte in lte_list:
        raw_mcc = lte.get("mcc")
        raw_mnc = lte.get("mnc")

        #jika mcc dan mnc nya kosong tidak disimpan ke db
        if not raw_mcc and not raw_mnc:
            continue

        status_value = True if lte.get("status") is None else bool(lte["status"])
        cur.execute(query, (
            campaign_id,
            device_id,
            to_int(lte.get("mcc")),
            to_int(lte.get("mnc")),
            lte.get("operator"),
            to_int(lte.get("arfcn")),
            to_int(lte.get("cell_identity")),
            to_int(lte.get("tracking_area_code")),
            to_int(lte.get("frequency_band_indicator")),
            to_int(lte.get("signal_level")),
            to_int(lte.get("snr")),
            to_int(lte.get("rx_lev_min")),
            status_value,
            to_float(lte.get("rssi"))
        ))


def process_message(message):
    try:
        message_parsed = json.loads(message)
        # Jika message_parsed adalah list, ambil elemen pertama sebagai data
        if isinstance(message_parsed, list):
            data = message_parsed[0]
        elif isinstance(message_parsed, dict):
            data = message_parsed.get("data")
        else:
            print("Struktur message tidak diketahui:", message_parsed)
            return
    except Exception as e:
        print("Gagal memparsing JSON:", e)
        return

    # Pastikan data merupakan dictionary
    if not isinstance(data, dict):
        print("Data yang di-parsing bukan dictionary:", data)
        return

    campaign_data = data.get("campaign")
    gsm_list = data.get("gsm_data", [])
    lte_list = data.get("lte_data", [])
    
    if campaign_data is None:
        print("Error processing message: campaign data is missing")
        return

    # Karena sekarang device_id akan diperoleh dari ip pada masing-masing record,
    # kita proses setiap record GSM dan LTE untuk mengambil ip, kemudian query ke tabel devices.
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Periksa setiap record GSM
                for gsm in gsm_list:
                    ip_val = gsm.get("ip")
                    if ip_val:
                        cur.execute("SELECT id, serial_number FROM devices WHERE ip = %s", (ip_val,))
                        device_row = cur.fetchone()
                        if device_row:
                            gsm["device_id"] = device_row["id"]
                            # Jika Anda ingin menyimpan serial device juga
                            gsm["device_serial"] = device_row.get("serial_number", "")
                        else:
                            print(f"Device dengan ip {ip_val} tidak ditemukan.")
                    else:
                        print("Record GSM tidak memiliki key 'ip':", gsm)
                # Periksa setiap record LTE
                for lte in lte_list:
                    ip_val = lte.get("ip")
                    if ip_val:
                        cur.execute("SELECT id, serial_number FROM devices WHERE ip = %s", (ip_val,))
                        device_row = cur.fetchone()
                        if device_row:
                            lte["device_id"] = device_row["id"]
                            lte["device_serial"] = device_row.get("serial_number", "")
                        else:
                            print(f"Device dengan ip {ip_val} tidak ditemukan.")
                    else:
                        print("Record LTE tidak memiliki key 'ip':", lte)
                # Setelah setiap record GSM/LTE memiliki device_id, lakukan penyisipan data.
                for gsm in gsm_list:
                    if "device_id" in gsm:
                        # Memasukkan record GSM secara individu (dalam list berisi satu record)
                        insert_gsm_data(cur, campaign_data["id"], gsm["device_id"], [gsm])
                    else:
                        print("Skipping GSM record tanpa device_id:", gsm)
                for lte in lte_list:
                    if "device_id" in lte:
                        insert_lte_data(cur, campaign_data["id"], lte["device_id"], [lte])
                    else:
                        print("Skipping LTE record tanpa device_id:", lte)
    except Exception as e:
        print("Error processing message:", e)
    finally:
        conn.close()


# Fungsi asynchronous untuk mendengarkan WebSocket dari satu device
async def listen_ws(uri: str):
    print(f"Membuka koneksi ke {uri}")
    try:
        async with websockets.connect(uri) as websocket:
            while True:
                message = await websocket.recv()
                print(f"Pesan diterima dari {uri}: {message}")
                process_message(message)
    except Exception as e:
        print(f"Error pada koneksi {uri}: {e}")

# Fungsi utama untuk mengambil IP device dari DB dan membuat task WebSocket untuk masing-masing
async def main(campaign_id : int):
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT server_url FROM groups")
        groups = cursor.fetchall()
    except Exception as e:
        print("Error retrieving group:", e)
        return
    finally:
        cursor.close()
        conn.close()

    tasks = []
    for group in groups:
        ip = group.get("server_url")
        if not ip:
            continue
        ws_uri = f"ws://{ip}:8004/ws/{campaign_id}"
        tasks.append(asyncio.create_task(listen_ws(ws_uri)))
    
    if tasks:
        await asyncio.gather(*tasks)
    else:
        print("Tidak ada device yang ditemukan.")

if __name__ == "__main__":
    import sys
    campaign_id = int(sys.argv[1])
    print("ini campaign id di main", campaign_id)
    asyncio.run(main(campaign_id))


