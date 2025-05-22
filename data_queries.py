# data_queries.py
import psycopg2
import psycopg2.extras
from database_config import get_db_connection  
from datetime import datetime
from auth import encrypt_password, fernet
from fastapi import HTTPException
from fastapi.responses import JSONResponse
import datetime
import datetime
import psycopg2.extras
from database_config import get_db_connection

def create_campaign(campaign_name: str, user_id: int, device_ids: list) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        created_at = datetime.datetime.utcnow()
        insert_query = """
            INSERT INTO campaign (name, status, time_start, user_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        cursor.execute(insert_query, (campaign_name, 'active', created_at, user_id))
        campaign_id = cursor.fetchone()[0]
        for device_id in device_ids:
            cursor.execute("INSERT INTO campaign_devices (campaign_id, device_id) VALUES (%s, %s)",
                           (campaign_id, device_id))
        connection.commit()
        return campaign_id
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()


def get_latest_campaign_with_data():
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil campaign terbaru; gunakan kolom time_start sebagai timestamp
        cursor.execute("SELECT id, time_start AS timestamp FROM campaign ORDER BY id DESC LIMIT 1")
        latest_campaign = cursor.fetchone()
        
        if not latest_campaign:
            return None
        id_campaign = latest_campaign['id']
        id_device = latest_campaign['id_device']
        
        # Ambil data GSM
        cursor.execute("SELECT * FROM gsm_data WHERE id_campaign = %s", (id_campaign,))
        gsm_data = cursor.fetchall()
        
        # Ambil data LTE
        cursor.execute("SELECT * FROM lte_data WHERE id_campaign = %s", (id_campaign,))
        lte_data = cursor.fetchall()

        cursor.execute("""
            SELECT d.*, dg.group_name 
            FROM devices d 
            JOIN device_group dg ON d.group_id = dg.id 
            WHERE d.id = %s
        """, (id_device,))
        device_data = cursor.fetchone()

        gsm_count = len(gsm_data)
        lte_count = len(lte_data)
        total = gsm_count + lte_count
        
        return {
            "status": "success",
            "id_campaign": latest_campaign['id'],
            "device": device_data,
            "timestamp": latest_campaign['timestamp'],
            "gsm_data": gsm_data,
            "lte_data": lte_data,
            "gsm_count": gsm_count, 
            "lte_count": lte_count,
            "total_count": total 
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_campaign_data_by_id(id_campaign):
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil informasi campaign; gunakan time_start sebagai timestamp
        cursor.execute("SELECT id, time_start AS timestamp FROM campaign WHERE id = %s", (id_campaign,))
        campaign = cursor.fetchone()
        
        if not campaign:
            return None
        
        # Ambil data GSM
        cursor.execute("SELECT * FROM gsm_data WHERE id_campaign = %s", (id_campaign,))
        gsm_data = cursor.fetchall()
        
        # Ambil data LTE
        cursor.execute("SELECT * FROM lte_data WHERE id_campaign = %s", (id_campaign,))
        lte_data = cursor.fetchall()

        gsm_count = len(gsm_data)
        lte_count = len(lte_data)
        total = gsm_count + lte_count
        
        return {
            "status": "success",
            "id_campaign": campaign['id'],
            "timestamp": campaign['timestamp'],
            "gsm_data": gsm_data,
            "lte_data": lte_data,
            "gsm_count": gsm_count, 
            "lte_count": lte_count,
            "total_count": total 
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_all_campaigns_data(page: int = 1, limit: int = 10):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Ambil semua campaign
            cur.execute("""
                SELECT c.id AS id_campaign, c.name, c.status, c.time_start, c.time_stop
                FROM campaign c
                ORDER BY c.id DESC
            """)
            campaigns_data = cur.fetchall()
            total_campaigns = len(campaigns_data)
            
            # Terapkan pagination jika limit tidak 0
            if limit == 0:
                campaigns_page = campaigns_data
            else:
                start_index = (page - 1) * limit
                end_index = start_index + limit
                campaigns_page = campaigns_data[start_index:end_index]

            results = []
            for campaign in campaigns_page:
                # Konversi waktu campaign ke string ISO jika memungkinkan
                time_start_str = (campaign["time_start"].isoformat() 
                                  if isinstance(campaign["time_start"], datetime.datetime) 
                                  else None)
                time_stop_str  = (campaign["time_stop"].isoformat()  
                                  if isinstance(campaign["time_stop"], datetime.datetime) 
                                  else None)
                
                # Hitung campaign duration:
                # Jika time_stop null (campaign belum berhenti), gunakan waktu sekarang.
                if campaign["time_start"]:
                    start_time = campaign["time_start"]
                    end_time = campaign["time_stop"] if campaign["time_stop"] is not None else datetime.datetime.utcnow()
                    campaign_duration = end_time - start_time
                else:
                    campaign_duration = None

                # Hitung total pause duration dari tabel campaign_pause
                cur.execute("""
                    SELECT pause_start, pause_end
                    FROM campaign_pause
                    WHERE campaign_id = %s AND pause_end IS NOT NULL
                """, (campaign["id_campaign"],))
                pauses = cur.fetchall()
                total_pause_duration = datetime.timedelta()
                for pause in pauses:
                    # Pastikan pause_start dan pause_end adalah objek datetime
                    if pause["pause_start"] and pause["pause_end"]:
                        total_pause_duration += pause["pause_end"] - pause["pause_start"]
                
                # Scanning duration = campaign_duration - total_pause_duration (jika campaign_duration ada)
                scanning_duration = (campaign_duration - total_pause_duration) if campaign_duration else None

                # Hitung threat_bts_count dan real_bts_count dari gsm_data dan lte_data
                cur.execute("""
                    WITH combined AS (
                        SELECT status FROM gsm_data WHERE campaign_id = %s
                        UNION ALL
                        SELECT status FROM lte_data WHERE campaign_id = %s
                    )
                    SELECT
                        SUM(CASE WHEN status = false THEN 1 ELSE 0 END) AS threat_bts_count,
                        SUM(CASE WHEN status = true  THEN 1 ELSE 0 END) AS real_bts_count
                    FROM combined
                """, (campaign["id_campaign"], campaign["id_campaign"]))
                bts_count = cur.fetchone()
                threat_bts_count = bts_count["threat_bts_count"] if bts_count["threat_bts_count"] is not None else 0
                real_bts_count   = bts_count["real_bts_count"]   if bts_count["real_bts_count"] is not None else 0

                # Ambil device yang terhubung ke campaign melalui tabel campaign_devices
                # Kemudian, ambil informasi group melalui join many-to-many (groups_devices dan groups)
                cur.execute("""
                    SELECT 
                        d.id AS device_id,
                        d.serial_number,
                        d.ip,
                        d.is_connected,
                        d.is_running,
                        d.created_at,
                        -- Ambil group info; jika device terdaftar di lebih dari satu group, ambil group pertama (misalnya)
                        g.id AS group_id,
                        g.group_name,
                        g.description,
                        g.created_at AS group_created_at
                    FROM campaign_devices cd
                    JOIN devices d ON cd.device_id = d.id
                    LEFT JOIN groups_devices gd ON d.id = gd.device_id
                    LEFT JOIN groups g ON gd.group_id = g.id
                    WHERE cd.campaign_id = %s
                    ORDER BY d.id ASC
                """, (campaign["id_campaign"],))
                devices_data = cur.fetchall()

                devices_result = []
                for dev in devices_data:
                    created_at_dev = (dev["created_at"].isoformat() 
                                      if isinstance(dev["created_at"], datetime.datetime) 
                                      else None)
                    group_data = None
                    if dev.get("group_id"):
                        group_data = {
                            "id": dev["group_id"],
                            "group_name": dev["group_name"],
                            "description": dev["description"],
                            "created_at": (dev["group_created_at"].isoformat() 
                                           if isinstance(dev["group_created_at"], datetime.datetime) 
                                           else None)
                        }
                    devices_result.append({
                        "device_id": dev["device_id"],
                        "serial_number": dev["serial_number"],
                        "ip": dev["ip"],
                        "is_connected": dev["is_connected"],
                        "is_running": dev["is_running"],
                        "created_at": created_at_dev,
                        "group": group_data
                    })

                results.append({
                    "id_campaign": campaign["id_campaign"],
                    "name": campaign["name"],
                    "status": campaign["status"],
                    "time_start": time_start_str,
                    "time_stop": time_stop_str,
                    "total_pause_duration": str(total_pause_duration),
                    "campaign_duration": str(scanning_duration) if scanning_duration else None,
                    "threat_bts_count": threat_bts_count,
                    "real_bts_count": real_bts_count,
                    "devices": devices_result
                })

            return {
                "status": "success",
                "page": page,
                "limit": limit,
                "total_campaigns": total_campaigns,
                "campaigns": results
            }
    except Exception as e:
        raise Exception(str(e))
    finally:
        conn.close()


def get_latest_campaign_with_unified_data(page: int = 1, limit: int = 10):
    """
    Menggabungkan data GSM dan LTE dari campaign terbaru, melakukan pagination terpadu,
    dan mengembalikan:
      - Data campaign (id, name, status, time_start, time_stop)
      - List data GSM dan LTE (masing-masing sudah diberi tanda 'type')
      - Jumlah BTS total, threat BTS (status False), dan real BTS (status True)
      - Informasi paging: page dan limit
    """
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil campaign terbaru beserta kolom time_start dan time_stop
        cursor.execute("""
            SELECT id, name, status, time_start, time_stop 
            FROM campaign 
            ORDER BY id DESC 
            LIMIT 1
        """)
        latest_campaign = cursor.fetchone()
        if not latest_campaign:
            return None

        id_campaign = latest_campaign['id']
        
        # Ambil data GSM dan tandai dengan tipe 'gsm'
        cursor.execute("SELECT * FROM gsm_data WHERE campaign_id = %s", (id_campaign,))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Ambil data LTE dan tandai dengan tipe 'lte'
        cursor.execute("SELECT * FROM lte_data WHERE campaign_id = %s", (id_campaign,))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan data GSM dan LTE
        combined_data = gsm_data + lte_data
        total_count = len(combined_data)
        # Hitung threat BTS: misalnya, status False dianggap threat
        threat_bts_count = sum(1 for item in combined_data if not item.get("status"))
        # Hitung real BTS: status True
        real_bts_count = sum(1 for item in combined_data if item.get("status"))
        
        # Lakukan pagination pada data gabungan
        start_index = (page - 1) * limit
        end_index = start_index + limit
        paginated_data = combined_data[start_index:end_index]
        
        gsm_data_paginated = [item for item in paginated_data if item['type'] == 'gsm']
        lte_data_paginated = [item for item in paginated_data if item['type'] == 'lte']
        
        return {
            "status": "success",
            "campaign": {
                "id_campaign": latest_campaign['id'],
                "name": latest_campaign.get("name", ""),
                "status": latest_campaign["status"],
                "time_start": latest_campaign["time_start"],
                "time_stop": latest_campaign["time_stop"]
            },
            "gsm_data": gsm_data_paginated,
            "lte_data": lte_data_paginated,
            "page": page,
            "limit": limit,
            "gsm_count": len(gsm_data),
            "lte_count": len(lte_data),
            "total_bts": total_count,
            "threat_bts_count": threat_bts_count,
            "real_bts_count": real_bts_count
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_campaign_with_unified_data_by_id(campaign_id: int, page: int = 0, limit: int = 0):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil data campaign lengkap
        cursor.execute(
            "SELECT id, name, status, time_start, time_stop FROM campaign WHERE id = %s",
            (campaign_id,)
        )
        campaign = cursor.fetchone()
        if not campaign:
            return None

        # Hitung campaign duration: jika time_stop null, gunakan waktu sekarang
        if campaign["time_start"]:
            start_time = campaign["time_start"]
            end_time = campaign["time_stop"] if campaign["time_stop"] is not None else datetime.datetime.utcnow()
            campaign_duration = end_time - start_time
        else:
            campaign_duration = None

        # Hitung total pause duration dari tabel campaign_pause
        cursor.execute("""
            SELECT pause_start, pause_end
            FROM campaign_pause
            WHERE campaign_id = %s AND pause_end IS NOT NULL
        """, (campaign_id,))
        pauses = cursor.fetchall()
        total_pause_duration = datetime.timedelta()
        for pause in pauses:
            if pause["pause_start"] and pause["pause_end"]:
                total_pause_duration += pause["pause_end"] - pause["pause_start"]
        
        # Scanning duration = campaign_duration - total_pause_duration (jika campaign_duration ada)
        scanning_duration = campaign_duration - total_pause_duration if campaign_duration is not None else None

        # Ambil data GSM
        cursor.execute("SELECT * FROM gsm_data WHERE campaign_id = %s", (campaign_id,))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Ambil data LTE
        cursor.execute("SELECT * FROM lte_data WHERE campaign_id = %s", (campaign_id,))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan data GSM dan LTE
        combined_data = gsm_data + lte_data

        # Urutkan berdasarkan created_at (sesuaikan descending/ascending sesuai kebutuhan)
        combined_data.sort(key=lambda x: x.get("created_at") or "")
        total_count = len(combined_data)
        
        # Hitung jumlah threat_bts_count dan real_bts_count
        threat_bts_count = sum(1 for item in combined_data if item.get("status") == False)
        real_bts_count = sum(1 for item in combined_data if item.get("status") == True)
        
        # Terapkan pagination jika limit tidak 0
        if limit == 0:
            paginated_data = combined_data
        else:
            start_index = (page - 1) * limit
            end_index = start_index + limit
            paginated_data = combined_data[start_index:end_index]
        
        gsm_data_paginated = [item for item in paginated_data if item['type'] == 'gsm']
        lte_data_paginated = [item for item in paginated_data if item['type'] == 'lte']
        
        # Ambil informasi device terkait campaign dari tabel campaign_devices dan devices
        cursor.execute("""
            SELECT 
                d.id AS device_id,
                d.serial_number,
                d.ip,
                d.is_connected,
                d.is_running,
                d.created_at AS device_created_at
            FROM campaign_devices cd
            JOIN devices d ON cd.device_id = d.id
            WHERE cd.campaign_id = %s
            ORDER BY d.id
        """, (campaign_id,))
        devices = cursor.fetchall()
        
        return {
            "status": "success",
            "campaign": {
                "id": campaign["id"],
                "name": campaign.get("name", ""),
                "status": campaign["status"],
                "time_start": campaign["time_start"],
                "time_stop": campaign["time_stop"],
                "total_pause_duration": str(total_pause_duration),
                "campaign_duration": str(scanning_duration) if scanning_duration else None,
            },
            "devices": devices,
            "gsm_data": gsm_data_paginated,
            "lte_data": lte_data_paginated, 
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "gsm_total": len(gsm_data),
            "lte_total": len(lte_data),
            "threat_bts_count": threat_bts_count,
            "real_bts_count": real_bts_count
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()



def get_campaign_for_ws(campaign_id: int):
    try:
        connection = get_db_connection()
        if connection is None:
            print("Koneksi database gagal!")
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil data campaign lengkap
        cursor.execute(
            "SELECT id, name, status, time_start, time_stop FROM campaign WHERE id = %s",
            (campaign_id,)
        )
        campaign = cursor.fetchone()
        if not campaign:
            print(f"Campaign dengan ID {campaign_id} tidak ditemukan!")
            return None
        
        # Konversi datetime ke string ISO 8601
        campaign["time_start"] = campaign["time_start"].isoformat() if isinstance(campaign["time_start"], datetime.datetime) else None
        campaign["time_stop"] = campaign["time_stop"].isoformat() if isinstance(campaign["time_stop"], datetime.datetime) else None

        # Ambil data GSM
        cursor.execute("SELECT * FROM gsm_data WHERE campaign_id = %s", (campaign_id,))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Ambil data LTE
        cursor.execute("SELECT * FROM lte_data WHERE campaign_id = %s", (campaign_id,))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan data GSM dan LTE
        combined_data = gsm_data + lte_data
        total_count = len(combined_data)
        
        # Hitung jumlah threat BTS (status False) dan real BTS (status True)
        threat_bts_count = sum(1 for item in combined_data if item.get("status") == False)
        real_bts_count = sum(1 for item in combined_data if item.get("status") == True)
        
        # Ambil data devices yang terhubung dengan campaign melalui tabel many-to-many campaign_devices
        cursor.execute("""
            SELECT 
                d.id AS device_id,
                d.serial_number,
                d.ip,
                d.is_connected,
                d.is_running,
                d.created_at AS device_created_at
            FROM campaign_devices cd
            JOIN devices d ON cd.device_id = d.id
            WHERE cd.campaign_id = %s
            ORDER BY d.id
        """, (campaign_id,))
        devices = cursor.fetchall()
        devices_cleaned = []
        for dev in devices:
            created_at = dev["device_created_at"].isoformat() if isinstance(dev["device_created_at"], datetime.datetime) else None
            devices_cleaned.append({
                "device_id": dev["device_id"],
                "serial_number": dev["serial_number"],
                "ip": dev["ip"],
                "is_connected": dev["is_connected"],
                "is_running": dev["is_running"],
                "created_at": created_at
            })

        result = {
            "status": "success",
            "campaign": campaign,
            "gsm_data": gsm_data,
            "lte_data": lte_data,
            "total_count": total_count,
            "threat_bts_count": threat_bts_count,
            "real_bts_count": real_bts_count,
            "devices": devices_cleaned
        }
        return result

    except Exception as e:
        print(f"Error di get_campaign_for_ws: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def delete_campaign_by_id(id_campaign: int):
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor()
        
        # Hapus relasi di tabel join (campaign_devices)
        cursor.execute("DELETE FROM campaign_devices WHERE campaign_id = %s", (id_campaign,))
        
        # Hapus data GSM dan LTE yang terkait
        cursor.execute("DELETE FROM gsm_data WHERE campaign_id = %s", (id_campaign,))
        cursor.execute("DELETE FROM lte_data WHERE campaign_id = %s", (id_campaign,))
        
        # Hapus data pause (jika tabel campaign_pause ada)
        cursor.execute("DELETE FROM campaign_pause WHERE campaign_id = %s", (id_campaign,))
        
        # Hapus campaign
        cursor.execute("DELETE FROM campaign WHERE id = %s", (id_campaign,))
        
        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

            
# def search_campaign_data(id_campaign: int, query: str):
#     try:
#         connection = get_db_connection()
#         if connection is None:
#             return None

#         cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
#         like_pattern = f"%{query}%"
        
#         gsm_query = """
#             SELECT * FROM gsm_data
#             WHERE id_campaign = %s AND (
#                 CAST(mcc AS TEXT) LIKE %s OR 
#                 CAST(mnc AS TEXT) LIKE %s OR 
#                 operator LIKE %s OR 
#                 CAST(local_area_code AS TEXT) LIKE %s OR 
#                 CAST(cell_identity AS TEXT) LIKE %s
#             )
#         """
#         cursor.execute(gsm_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
#         gsm_data = cursor.fetchall()
        
#         lte_query = """
#             SELECT * FROM lte_data
#             WHERE id_campaign = %s AND (
#                 mcc LIKE %s OR 
#                 mnc LIKE %s OR 
#                 operator LIKE %s OR 
#                 CAST(cell_identity AS TEXT) LIKE %s OR 
#                 tracking_area_code LIKE %s OR 
#                 frequency_band_indicator LIKE %s
#             )
#         """
#         cursor.execute(lte_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
#         lte_data = cursor.fetchall()
        
#         gsm_count = len(gsm_data)
#         lte_count = len(lte_data)
#         total_count = gsm_count + lte_count
        
#         return {
#             "status": "success",
#             "id_campaign": id_campaign,
#             "gsm_data": gsm_data,
#             "lte_data": lte_data,
#             "gsm_count": gsm_count,
#             "lte_count": lte_count,
#             "total_count": total_count
#         }
        
#     except Exception as e:
#         print(f"Error: {e}")
#         return None
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()

def search_campaign_data_paginate(id_campaign: int, query: str, page: int = 0, limit: int = 0):
    try:
        connection = get_db_connection()
        if connection is None:
            print("Database tidak connect")
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        like_pattern = f"%{query}%"
        
        # Query untuk tabel GSM
        gsm_query = """
            SELECT * FROM gsm_data
            WHERE campaign_id = %s AND (
                CAST(mcc AS TEXT) ILIKE %s OR 
                CAST(mnc AS TEXT) ILIKE %s OR 
                operator ILIKE %s OR 
                CAST(local_area_code AS TEXT) ILIKE %s OR 
                CAST(cell_identity AS TEXT) ILIKE %s
            )
        """
        cursor.execute(gsm_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Query untuk tabel LTE
        lte_query = """
            SELECT * FROM lte_data
            WHERE campaign_id = %s AND (
                mcc ILIKE %s OR 
                mnc ILIKE %s OR 
                operator ILIKE %s OR 
                CAST(cell_identity AS TEXT) ILIKE %s OR 
                tracking_area_code ILIKE %s OR 
                frequency_band_indicator ILIKE %s
            )
        """
        cursor.execute(lte_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan hasil pencarian dari kedua tabel
        combined_data = gsm_data + lte_data
        total_count = len(combined_data)
        
        # Lakukan pagination pada data gabungan (jika limit tidak 0)
        if limit == 0:
            paginated_data = combined_data
        else:
            start_index = (page - 1) * limit
            end_index = start_index + limit
            paginated_data = combined_data[start_index:end_index]
        
        # Pisahkan data hasil pagination berdasarkan tipe
        gsm_data_paginated = [item for item in paginated_data if item['type'] == 'gsm']
        lte_data_paginated = [item for item in paginated_data if item['type'] == 'lte']
        
        return {
            "status": "success",
            "id_campaign": id_campaign,
            "gsm_data": gsm_data_paginated,
            "lte_data": lte_data_paginated,
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "gsm_total": len(gsm_data),
            "lte_total": len(lte_data)
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def list_devices():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Query untuk mendapatkan data device dan group melalui join many-to-many
            query = """
            SELECT 
                d.id AS device_id,
                d.serial_number,
                d.ip,
                d.is_connected,
                d.is_running,
                d.lat,
                d."long",
                d.created_at AS device_created_at,
                g.id AS group_id,
                g.group_name,
                g.description,
                g.created_at AS group_created_at
            FROM devices d
            LEFT JOIN groups_devices gd ON d.id = gd.device_id
            LEFT JOIN groups g ON gd.group_id = g.id
            ORDER BY d.id;
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            devices = {}
            for row in rows:
                device_id = row["device_id"]
                if device_id not in devices:
                    devices[device_id] = {
                        "id": device_id,
                        "serial_number": row["serial_number"],
                        "ip": row["ip"],
                        "is_connected": row["is_connected"],
                        "is_running": row["is_running"],
                        "lat": row["lat"],
                        "long": row["long"],
                        "created_at": row["device_created_at"].isoformat() if row["device_created_at"] else None,
                        "group": None  # Akan diisi jika ada data group
                    }
                # Karena tiap device hanya memiliki satu group, jika ada group, simpan sebagai objek tunggal
                if row["group_id"] is not None:
                    group = {
                        "id": row["group_id"],
                        "group_name": row["group_name"],
                        "description": row["description"],
                        "created_at": row["group_created_at"].isoformat() if row["group_created_at"] else None
                    }
                    devices[device_id]["group"] = group
            
            response = {
                "status": "success",
                "devices": list(devices.values())
            }
            return JSONResponse(status_code=200, content=response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
def device_information():
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        
        # Inactive count: device yang tidak terhubung (is_connected = FALSE)
        cursor.execute("SELECT COUNT(*) FROM devices WHERE is_connected = FALSE")
        inactive_count = cursor.fetchone()[0]

        # Available count: device yang terhubung tapi tidak running (is_connected = TRUE AND is_running = FALSE)
        cursor.execute("SELECT COUNT(*) FROM devices WHERE is_connected = TRUE AND is_running = FALSE")
        available_count = cursor.fetchone()[0]

        # Running count: device yang sedang running (is_running = TRUE)
        cursor.execute("SELECT COUNT(*) FROM devices WHERE is_running = TRUE")
        running_count = cursor.fetchone()[0]

        # Threat BTS: jumlah data GSM dan LTE dengan status FALSE
        # cursor.execute("SELECT COUNT(*) FROM gsm_data WHERE status = FALSE")
        # threat_gsm = cursor.fetchone()[0]
        # cursor.execute("SELECT COUNT(*) FROM lte_data WHERE status = FALSE")
        # threat_lte = cursor.fetchone()[0]
        # threat_bts = threat_gsm + threat_lte
        cursor.execute("""
            SELECT COUNT(*) 
            FROM gsm_data
            WHERE status = FALSE
            AND campaign_id = (
                SELECT MAX(campaign_id)
                FROM gsm_data
            )
        """)
        threat_gsm = cursor.fetchone()[0]
        cursor.execute("""
            SELECT COUNT(*) 
            FROM lte_data
            WHERE status = FALSE
            AND campaign_id = (
                SELECT MAX(campaign_id)
                FROM lte_data
            )
        """)
        threat_lte = cursor.fetchone()[0]
        threat_bts = threat_gsm + threat_lte

        # Total BTS: total data dari gsm_data dan lte_data
        # cursor.execute("SELECT COUNT(*) FROM gsm_data")
        # total_gsm = cursor.fetchone()[0]
        # cursor.execute("SELECT COUNT(*) FROM lte_data")
        # total_lte = cursor.fetchone()[0]
        # total_bts = total_gsm + total_lte

        cursor.execute("""
        SELECT COUNT(*) 
        FROM gsm_data
        WHERE campaign_id = (
            SELECT MAX(campaign_id) FROM gsm_data
        )
        """)
        total_gsm = cursor.fetchone()[0]

        # Total LTE untuk campaign_id terbaru
        cursor.execute("""
            SELECT COUNT(*) 
            FROM lte_data
            WHERE campaign_id = (
                SELECT MAX(campaign_id) FROM lte_data
            )
        """)
        total_lte = cursor.fetchone()[0]

        total_bts = total_gsm + total_lte

        # Count per generation:
        # Asumsikan semua data GSM merupakan 2G.
        count_2g = total_gsm
        count_4g = total_lte

        return {
            "inactive": inactive_count,
            "available": available_count,
            "running": running_count,
            "threat_bts": threat_bts,
            "total_bts": total_bts,
            "total_4G": count_4g,
            "total_2G": count_2g
        }
    except Exception as e:
        print(f"Error retrieving status counts: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def device_information_detail(device_id: int):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM devices WHERE id = %s", (device_id,))
        device = cursor.fetchone()
        return device
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

def devicegroup():
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Ambil semua group beserta informasi owner (jika ada)
        cursor.execute("""
            SELECT 
                g.id AS group_id, 
                g.group_name, 
                g.description, 
                g.created_at, 
                g.user_id,
                u.username,
                u.role
            FROM groups g
            LEFT JOIN users u ON g.user_id = u.id
            ORDER BY g.id ASC
        """)
        groups = cursor.fetchall()
        
        device_groups = []
        for grp in groups:
            # Konversi created_at group ke ISO string jika perlu
            group_created_at = (grp["created_at"].isoformat() 
                                if isinstance(grp["created_at"], datetime.datetime) 
                                else grp["created_at"])
            
            # Ambil devices yang terhubung dengan group via tabel join groups_devices
            cursor.execute("""
                SELECT 
                    d.id AS device_id,
                    d.serial_number,
                    d.ip,
                    d.is_connected,
                    d.is_running,
                    d.created_at
                FROM groups_devices gd
                JOIN devices d ON gd.device_id = d.id
                WHERE gd.group_id = %s
                ORDER BY d.id ASC
            """, (grp["group_id"],))
            devices = cursor.fetchall()
            devices_list = []
            for d in devices:
                created_at_dev = (d["created_at"].isoformat() 
                                  if isinstance(d["created_at"], datetime.datetime)
                                  else d["created_at"])
                devices_list.append({
                    "device_id": d["device_id"],
                    "serial_number": d["serial_number"],
                    "ip": d["ip"],
                    "is_connected": d["is_connected"],
                    "is_running": d["is_running"],
                    "created_at": created_at_dev
                })
            
            # Tentukan status group; jika ada device, status dianggap "assigned"
            group_status = "assigned" if devices_list else "unassigned"
            
            # Ambil owner info (jika ada)
            owner = None
            if grp.get("user_id"):
                owner = {
                    "user_id": grp["user_id"],
                    "username": grp.get("username"),
                    "role": grp.get("role")
                }
            
            device_groups.append({
                "group_id": grp["group_id"],
                "group_name": grp["group_name"],
                "description": grp["description"],
                "created_at": group_created_at,
                "group_status": group_status,
                "owner": owner,
                "devices": devices_list
            })
        
        return {
            "status": "success",
            "device_groups": device_groups
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
def get_all_campaigns(page: int = 1, limit: int = 10):
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil total jumlah campaign
        cursor.execute("SELECT COUNT(*) AS total FROM campaign")
        total_campaigns = cursor.fetchone()["total"]
        
        offset = (page - 1) * limit
        
        # Ambil data campaign dengan pagination
        cursor.execute("""
            SELECT id, name, status, time_start, time_stop, user_id 
            FROM campaign 
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        campaigns = cursor.fetchall()
        
        results = []
        for camp in campaigns:
            campaign_id = camp["id"]
            # Ambil data user (pembuat campaign)
            cursor.execute("""
                SELECT id, username, email, role 
                FROM users 
                WHERE id = %s
            """, (camp["user_id"],))
            user = cursor.fetchone()
            
            # Ambil daftar device yang terlibat (JOIN campaign_devices dan devices)
            cursor.execute("""
                SELECT d.id, d.serial_number, d.ip, d.is_connected, d.created_at 
                FROM campaign_devices cd
                JOIN devices d ON cd.device_id = d.id
                WHERE cd.campaign_id = %s
            """, (campaign_id,))
            devices = cursor.fetchall()
            device_count = len(devices)
            
            # Hitung jumlah BTS dari GSM dan LTE
            cursor.execute("SELECT COUNT(*) AS count FROM gsm_data WHERE campaign_id = %s", (campaign_id,))
            gsm_count = cursor.fetchone()["count"]
            cursor.execute("SELECT COUNT(*) AS count FROM lte_data WHERE campaign_id = %s", (campaign_id,))
            lte_count = cursor.fetchone()["count"]
            total_bts = gsm_count + lte_count
            # Diasumsikan jumlah threads sama dengan jumlah device
            thread_count = device_count

            summary = {
                "id_campaign": campaign_id,
                "campaign_name": camp.get("name", ""),
                "status": camp["status"],
                "time_start": camp["time_start"],
                "time_stop": camp["time_stop"],
                "user": user,
                "jumlah_device": device_count,
                "devices": devices,
                "jumlah_bts": total_bts,
                "jumlah_threads": thread_count
            }
            results.append(summary)
        
        return {
            "status": "success",
            "page": page,
            "limit": limit,
            "total_campaigns": total_campaigns,
            "campaigns": results
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def remove_device(device_id):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Cek apakah device terasosiasi dengan group di tabel groups_devices
        cursor.execute("SELECT * FROM groups_devices WHERE device_id = %s", (device_id,))
        associations = cursor.fetchall()
        if not associations:
            raise HTTPException(status_code=404, detail="Device is not assigned to any group")
        
        # Hapus semua asosiasi device tersebut dari groups_devices
        cursor.execute("DELETE FROM groups_devices WHERE device_id = %s", (device_id,))
        conn.commit()
        return {
            "status": "success",
            "message": f"Device {device_id} removed from group(s) successfully."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

def addDeviceToGroup(device_id, group_id):
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # Cek apakah device ada
        cursor.execute("SELECT id FROM devices WHERE id = %s", (device_id,))
        device_row = cursor.fetchone()
        if not device_row:
            raise HTTPException(status_code=404, detail="Device not found")
        
        # Cek apakah group ada (tabel groups)
        cursor.execute("SELECT id FROM groups WHERE id = %s", (group_id,))
        group_row = cursor.fetchone()
        if not group_row:
            raise HTTPException(status_code=404, detail="Group not found")
        
        # Insert ke tabel join groups_devices
        query = """
            INSERT INTO groups_devices (group_id, device_id)
            VALUES (%s, %s)
            ON CONFLICT (group_id, device_id) DO NOTHING;
        """
        cursor.execute(query, (group_id, device_id))
        conn.commit()
        
        return {
            "status": "success",
            "message": f"Device {device_id} added to group {group_id} successfully."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()


def deleteuser(user_id):
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection error")

    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # 1. Cek apakah user dengan user_id tersebut ada
        cursor.execute("SELECT id, username FROM users WHERE id = %s", (user_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        # 2. Cek apakah ada group yang dimiliki user ini, set user_id ke NULL
        cursor.execute("SELECT id FROM groups WHERE user_id = %s", (user_id,))
        groups_owned = cursor.fetchall()
        if groups_owned:
            # Set user_id = NULL untuk semua group yang dimiliki user ini
            cursor.execute("UPDATE groups SET user_id = NULL WHERE user_id = %s", (user_id,))

        # 3. Hapus user dari tabel users
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))

        conn.commit()

        return {
            "status": "success",
            "message": f"User with id={user_id} has been deleted successfully.",
            "deleted_user": {
                "id": user_id,
                "username": user_row["username"]
            }
        }
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def editUser(user_id, username , password):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # Cek apakah user dengan user_id tersebut ada
        cursor.execute("SELECT id FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Jika email diberikan, cek apakah sudah digunakan oleh user lain
        if username:
            cursor.execute("SELECT id FROM users WHERE username = %s AND id <> %s", (username, user_id))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="username already in use by another user.")

        # Siapkan update query dinamis berdasarkan field yang diupdate
        update_fields = []
        update_values = []
        if username:
            update_fields.append("username = %s")
            update_values.append(username)
        if password:
            # Gantikan fungsi hashing dengan enkripsi reversible
            encrypted_pw = encrypt_password(password)
            update_fields.append("password = %s")
            update_values.append(encrypted_pw)
        
        if update_fields:
            # Tambahkan kondisi WHERE
            update_values.append(user_id)
            update_query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"
            cursor.execute(update_query, tuple(update_values))
            conn.commit()

        # Kembalikan data user terbaru
        cursor.execute("SELECT id, username, created_at FROM users WHERE id = %s", (user_id,))
        updated_user = cursor.fetchone()
        return {
            "status": "success",
            "message": "User updated successfully",
            "user": updated_user
        }
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()



def listUser():
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
            SELECT 
                u.id AS user_id, 
                u.username, 
                u.role, 
                u.created_at AS user_created_at,
                g.id AS group_id, 
                g.group_name, 
                g.description AS group_description, 
                g.created_at AS group_created_at
            FROM users u
            LEFT JOIN groups g ON u.id = g.user_id
            ORDER BY u.id, g.id;
            """
            cur.execute(query)
            rows = cur.fetchall()

            # Group data berdasarkan user; ambil group pertama jika ada
            users = {}
            for row in rows:
                user_id = row["user_id"]
                if user_id not in users:
                    users[user_id] = {
                        "id": user_id,
                        "username": row.get("username"),
                        "role": row.get("role"),
                        "created_at": row.get("user_created_at"),
                        "group": None
                    }
                # Jika ada group dan group belum diset untuk user ini, set group
                if row["group_id"] is not None and users[user_id]["group"] is None:
                    users[user_id]["group"] = {
                        "group_id": row["group_id"],
                        "group_name": row["group_name"],
                        "group_description": row["group_description"],
                        "group_created_at": row["group_created_at"]
                    }
            return {
                "status": "success",
                "users": list(users.values())
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


def getPassword(user_id: str) -> str:
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute("SELECT password FROM users WHERE id = %s", (user_id,))
        password_record = cursor.fetchone()

        if not password_record:
            raise HTTPException(status_code=404, detail="User tidak ditemukan")

        encrypted_password = password_record["password"]

        decrypted_password = fernet.decrypt(encrypted_password.encode()).decode()

        return {
            "status": "success",
            "password": decrypted_password,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()