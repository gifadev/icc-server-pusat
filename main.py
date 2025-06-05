import asyncio
from fastapi import FastAPI, HTTPException, Depends, status, Form, WebSocket, Query, Path
from fastapi.responses import JSONResponse
import threading
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
from data_queries import (
    get_all_campaigns_data,
    get_latest_campaign_with_unified_data,
    get_campaign_for_ws,
    get_campaign_with_unified_data_by_id,
    delete_campaign_by_id,
    search_campaign_data_paginate,
    list_devices,
    device_information,
    device_information_detail,
    devicegroup,
    get_all_campaigns,
    remove_device,
    addDeviceToGroup,
    deleteuser,
    editUser,
    listUser,
    getPassword
)
from auth import router as auth_router
from fastapi.security import OAuth2PasswordBearer
import jwt
from database_config import get_db_connection
import requests
import datetime
import psycopg2.extras
from broadcaster import manager
import os
from dotenv import load_dotenv
from fastapi.encoders import jsonable_encoder

load_dotenv()


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sertakan router auth di bawah prefix /auth
app.include_router(auth_router, prefix="/auth")

# ==================== SETUP AUTENTIKASI ====================
SECRET_KEY = os.environ.get("SECRET_KEY")
ALGORITHM = os.environ.get("ALGORITHM")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

ALLOWED_TABLES = {
    "gsm": "gsm_data",
    "lte": "lte_data"
}

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token tidak valid1",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid2",
            headers={"WWW-Authenticate": "Bearer"},
        )
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = "SELECT * FROM users WHERE id = %s"
        cursor.execute(query, (user_id,))
        user = cursor.fetchone()
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User tidak ditemukan",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        connection.close()


def require_role(allowed_roles: list):
    def role_checker(current_user: dict = Depends(get_current_user)):
        if current_user.get("role") not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient privileges"
            )
        return current_user
    return role_checker


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


def update_campaign_status(campaign_id, new_status):
    connection = get_db_connection()
    cursor = connection.cursor()
    sql = "UPDATE campaign SET status = %s WHERE id = %s"
    cursor.execute(sql, (new_status, campaign_id))
    connection.commit()
    cursor.close()
    connection.close()

def stop_campaign_devices(campaign_id: int):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Ambil device yang terlibat pada campaign aktif
        cursor.execute("""
            SELECT d.id, d.ip 
            FROM campaign_devices cd
            JOIN devices d ON cd.device_id = d.id
            WHERE cd.campaign_id = %s
        """, (campaign_id,))
        devices = cursor.fetchall()
    except Exception as e:
        raise Exception(f"Error retrieving devices for campaign {campaign_id}: {e}")
    finally:
        cursor.close()
        conn.close()

    # Untuk setiap device, kirim request stop dan update status
    for device in devices:
        ip = device.get("ip")
        if ip:
            url = f"http://{ip}:8003/stop-capture/{campaign_id}"
            try:
                resp = requests.get(url)
                resp_json = resp.json()
                # Update status device
                conn_update = get_db_connection()
                try:
                    cursor_update = conn_update.cursor()
                    update_query = "UPDATE devices SET is_running = FALSE WHERE id = %s"
                    cursor_update.execute(update_query, (device["id"],))
                    conn_update.commit()
                except Exception as ex:
                    print(f"Error updating is_running for device {device['id']}: {ex}")
                finally:
                    cursor_update.close()
                    conn_update.close()
            except Exception as e:
                print(f"Error stopping capture for device {device['id']} at {ip}: {e}")

    # Update campaign status dan time_stop
    try:
        conn_campaign = get_db_connection()
        cursor_campaign = conn_campaign.cursor()
        time_stop = datetime.datetime.utcnow()
        update_query_campaign = "UPDATE campaign SET status = %s, time_stop = %s WHERE id = %s"
        cursor_campaign.execute(update_query_campaign, ('stopped', time_stop, campaign_id))
        conn_campaign.commit()
    except Exception as e:
        raise Exception(f"Error updating campaign status: {e}")
    finally:
        cursor_campaign.close()
        conn_campaign.close()


stop_event = threading.Event()
capture_thread = None

@app.post("/start-capture", status_code=200)
async def start_campaign(
    campaign_name: str = Form(...),
    device_ids: str = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    # cek apakah ada campaign active atau tidak
    conn_check = get_db_connection()
    try:
        with conn_check.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM campaign WHERE status = %s", ('active',))
            active_count = cur.fetchone()[0]
        if active_count > 0:
            raise HTTPException(
                status_code=400,
                detail="already has an active campaign. Cannot create a new one"
            )
    finally:
        conn_check.close()

    # === 1. Parse device_ids dari string ke list of integer ===
    try:
        device_id_list = [int(x.strip()) for x in device_ids.split(",") if x.strip()]
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid device_ids format")
    
    # === 2. Query data device berdasarkan device_ids dengan join groups_devices untuk mendapatkan group_id ===
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
                SELECT d.id, d.ip, gd.group_id
                FROM devices d
                JOIN groups_devices gd ON d.id = gd.device_id
                WHERE d.id = ANY(%s);
            """
            cur.execute(query, (device_id_list,))
            devices = cur.fetchall()
            if not devices:
                raise HTTPException(status_code=404, detail="No devices found for given IDs")
            
            # Kelompokkan IP device berdasarkan group_id
            group_devices = defaultdict(list)
            for device in devices:
                # Karena many-to-many, sebuah device bisa memiliki lebih dari satu group.
                # Di sini, misalnya kita ambil setiap group_id yang terkait.
                group_devices[device["group_id"]].append(device["ip"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving devices: {e}")
    finally:
        conn.close()
    
    # === 3. Buat campaign global dan dapatkan campaign_id ===
    
    try:
        campaign_id = create_campaign(campaign_name, current_user["id"], device_id_list)
        await manager.broadcast(campaign_id, {
            "message": f"Campaign '{campaign_name}' (ID: {campaign_id}) telah dimulai"
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating campaign: {e}")

    # Jalankan listener WS secara asynchronous setelah 10 detik
    async def run_ws_listener():
        await asyncio.sleep(10)
        await asyncio.create_subprocess_exec("python3", "wsReceivedata.py", str(campaign_id))
    asyncio.create_task(run_ws_listener())
    
    responses = []
    # === 4. Untuk setiap group, ambil server_url dan kirim request ke endpoint /start-capture di server group ===
    for group_id, ips in group_devices.items():
        # Ambil server_url untuk group tersebut
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT server_url FROM groups WHERE id = %s", (group_id,))
                group_record = cur.fetchone()
                if not group_record or not group_record.get("server_url"):
                    responses.append({
                        "group_id": group_id,
                        "error": "Server URL for group not found"
                    })
                    continue
                server_url = group_record["server_url"]
        except Exception as e:
            responses.append({
                "group_id": group_id,
                "error": str(e)
            })
            continue
        finally:
            conn.close()
        
        # Gabungkan IP device untuk group ini (dipisahkan dengan koma)
        device_ips = ",".join(ips)
        payload = {
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "device_ips": device_ips,
            "group_id": group_id
        }
        
        # Kirim POST request ke endpoint /start-capture di server group
        group_endpoint = f"http://{server_url}:8004/start-capture"
        try:
            resp = requests.post(group_endpoint, data=payload)
            resp_json = resp.json()
        except Exception as e:
            resp_json = {"error": str(e)}
        
        responses.append({
            "group_id": group_id,
            "server_url": server_url,
            "device_ips": device_ips,
            "response": resp_json
        })
    
    # === 5. Kembalikan respons JSON akhir ===
    return JSONResponse(status_code=200, content={
        "message": "Campaign started successfully",
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "group_responses": responses
    })

@app.websocket("/ws/{campaign_id}")
async def websocket_endpoint(
    websocket: WebSocket, 
    campaign_id: int
    ):
    await manager.connect(campaign_id, websocket)
    print(f"Klien WebSocket terhubung untuk campaign {campaign_id}")
    try:
        while True:
            # Buka koneksi untuk mengambil status campaign
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SELECT status FROM campaign WHERE id = %s", (campaign_id,))
            campaign = cursor.fetchone()
            cursor.close()
            conn.close()

            # Ambil data campaign (misalnya, untuk broadcast jika aktif)
            campaign_data = get_campaign_for_ws(campaign_id)
            if not campaign_data:
                await websocket.send_json({"message": "Campaign data not found."})
                await websocket.close()
                break

            if campaign["status"] == "stopped":
                print(f"Campaign {campaign_id} telah dihentikan. Menutup koneksi WebSocket.")
                await websocket.send_json({"message": "Campaign has been stopped."})
                await websocket.close()
                break

            elif campaign["status"] == "paused":
                # Kirim pesan pause satu kali dan kemudian tunggu sampai status berubah
                await websocket.send_json({"message": "Campaign is paused."})
                # Loop untuk menunggu hingga status berubah dari "paused"
                while True:
                    await asyncio.sleep(5)
                    conn = get_db_connection()
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cursor.execute("SELECT status FROM campaign WHERE id = %s", (campaign_id,))
                    updated_campaign = cursor.fetchone()
                    cursor.close()
                    conn.close()
                    if updated_campaign["status"] != "paused":
                        break
                # Setelah status tidak lagi paused, lanjutkan loop
            else:
                # Jika status aktif, broadcast data campaign
                encoded_data = jsonable_encoder({
                    "message": "send data campaign.",
                    "data": campaign_data
                })
                await websocket.send_json(encoded_data)

            await asyncio.sleep(5)
    except Exception as e:
        manager.disconnect(websocket)
        print(f"Klien terputus dari campaign {campaign_id}: {e}")


@app.post("/pause-campaign", status_code=200)
def pause_campaign(
    campaign_id: int = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Update status campaign menjadi 'paused'
                cur.execute("UPDATE campaign SET status = 'paused' WHERE id = %s", (campaign_id,))
                # Insert baris baru ke campaign_pause dengan pause_start = now
                cur.execute("""
                    INSERT INTO campaign_pause (campaign_id, pause_start)
                    VALUES (%s, CURRENT_TIMESTAMP)
                    RETURNING id;
                """, (campaign_id,))
                pause_id = cur.fetchone()[0]
                conn.commit()
                return {"status": "success", "message": f"Campaign {campaign_id} paused.", "pause_id": pause_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error pausing campaign: {e}")
    finally:
        conn.close()


@app.post("/resume-campaign", status_code=200)
def resume_campaign(
    campaign_id: int = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Periksa apakah ada baris di campaign_pause tanpa pause_end untuk campaign ini
                cur.execute("""
                    SELECT id, pause_start FROM campaign_pause
                    WHERE campaign_id = %s AND pause_end IS NULL
                    ORDER BY id DESC LIMIT 1
                """, (campaign_id,))
                pause_record = cur.fetchone()
                if not pause_record:
                    raise HTTPException(status_code=400, detail="No active pause found for this campaign.")
                
                # Update status campaign menjadi 'active'
                cur.execute("UPDATE campaign SET status = 'active' WHERE id = %s", (campaign_id,))
                # Update baris pause dengan pause_end = now
                cur.execute("""
                    UPDATE campaign_pause
                    SET pause_end = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (pause_record["id"],))
                conn.commit()
                return {"status": "success", "message": f"Campaign {campaign_id} resumed."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error resuming campaign: {e}")
    finally:
        conn.close()


@app.post("/stop-capture", status_code=200)
async def stop_campaign(
    campaign_id: int = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    # === 1. Ambil daftar device_id dari tabel campaign_devices untuk campaign_id yang diberikan ===
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT device_id FROM campaign_devices WHERE campaign_id = %s", (campaign_id,))
            campaign_devices = cur.fetchall()
            if not campaign_devices:
                raise HTTPException(status_code=400, detail="No devices associated with this campaign.")
            device_ids_list = [row["device_id"] for row in campaign_devices]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving campaign devices: {e}")
    finally:
        conn.close()
    
    # === 2. Ambil data device (termasuk IP dan group_id) berdasarkan device_ids_list 
    # Gunakan join dengan groups_devices karena kolom group_id tidak ada di tabel devices
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
                SELECT d.id, d.ip, gd.group_id, d.is_running
                FROM devices d
                JOIN groups_devices gd ON d.id = gd.device_id
                WHERE d.id = ANY(%s);
            """
            cur.execute(query, (device_ids_list,))
            devices = cur.fetchall()
            if not devices:
                raise HTTPException(status_code=404, detail="Devices not found for given IDs.")
            # Kelompokkan device IP berdasarkan group_id
            group_devices = defaultdict(list)
            for device in devices:
                if device.get("group_id") and device.get("ip"):
                    group_devices[device["group_id"]].append(device["ip"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving devices: {e}")
    finally:
        conn.close()
    
    responses = []
    # === 3. Untuk setiap group, ambil server_url dari tabel groups dan kirim request stop-capture ke server group ===
    for group_id, ip_list in group_devices.items():
        # Ambil server_url untuk group tersebut
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT server_url FROM groups WHERE id = %s", (group_id,))
                group_record = cur.fetchone()
                if not group_record or not group_record.get("server_url"):
                    responses.append({
                        "group_id": group_id,
                        "error": "Server URL for group not found"
                    })
                    continue
                server_url = group_record["server_url"]
        except Exception as e:
            responses.append({
                "group_id": group_id,
                "error": str(e)
            })
            continue
        finally:
            conn.close()
        
        # Siapkan payload untuk dikirim ke server group
        payload = {
            "campaign_id": campaign_id
        }
        group_endpoint = f"http://{server_url}:8004/stop-capture"
        try:
            resp = requests.post(group_endpoint, data=payload)
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {"error": "Invalid JSON response"}
        except Exception as e:
            resp_json = {"error": str(e)}
        responses.append({
            "group_id": group_id,
            "server_url": server_url,
            "device_ips": ",".join(ip_list),
            "response": resp_json
        })
    
    # === 4. Update status campaign dan catat waktu stop di database pusat ===
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            time_stop = datetime.datetime.utcnow()
            update_query = "UPDATE campaign SET status = 'stop', time_stop = %s WHERE id = %s"
            cur.execute(update_query, (time_stop, campaign_id))
            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating campaign status: {e}")
    finally:
        conn.close()
    
    # === 5. Kirim pesan ke WebSocket untuk memberitahu WS di server pusat bahwa campaign telah dihentikan ===
    await manager.broadcast(campaign_id, "Campaign has been stopped.")
    await manager.close_campaign_connections(campaign_id)
    
    # === 6. Kembalikan respons JSON akhir ===
    return JSONResponse(status_code=200, content={
        "message": "Live capture stopped successfully for devices associated with the campaign",
        "campaign_id": campaign_id,
        "group_responses": responses
    })

@app.delete("/campaign/{campaign_id}")
def delete_campaign(campaign_id: int,
                    current_user: dict = Depends(require_role(["admin", "superadmin"]))
                    ):
    result = delete_campaign_by_id(campaign_id)
    if result is None:
        raise HTTPException(status_code=500, detail="Error deleting campaign or campaign not found")
    return {
        "status": "success",
        "message": f"Campaign with id {campaign_id} deleted successfully."
    }

@app.get("/all-campaigns")
def get_all_campaigns(page: int = 0, 
                      limit: int = 0,
                      current_user: dict = Depends(require_role(["admin", "superadmin"]))
                      ):
    try:
        result = get_all_campaigns_data(page, limit)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/campaign-data/{campaign_id}")
def get_campaign_data_unified(
    campaign_id: int, 
    page: int = 0, limit: int = 0,
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = get_campaign_with_unified_data_by_id(campaign_id, page=page, limit=limit)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Campaign with ID {campaign_id} not found")
    return result

@app.get("/campaign/search-data")
def search_campaign_data_endpoint(
    id_campaign: int,
    q: str,
    page: int = 1,
    limit: int = 10,
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = search_campaign_data_paginate(id_campaign, q, page=page, limit=limit)
    if result is None:
        raise HTTPException(status_code=404, detail="No matching data found")
    return result


@app.get("/device-information")
def dev_information(
    current_user: dict = Depends(require_role(["admin", "superadmin"]))

):
    result = device_information()
    if result is None:
        raise HTTPException(status_code=500, detail="Error retrieving device information")
    return result

@app.get("/device-groups")
def get_device_groups(
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = devicegroup()
    if result is None:
        raise HTTPException(status_code=500, detail="Error retrieving group information")
    return result

@app.delete("/remove-device-from-group/{device_id}")
def remove_device_from_group(
    device_id: int,
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = remove_device(device_id)
    if result is None:
        raise HTTPException(status_code=500, detail="Error  remove group")
    return result


@app.post("/add-device-to-group")
def add_device_to_group(
    device_id: int = Form(...),
    group_id: int = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = addDeviceToGroup(device_id, group_id)
    if result is None:
        raise HTTPException(status_code=500, detail="Error  add device to group")
    return result

@app.delete("/delete-user/{user_id}")
def delete_user(
    user_id: int,
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = deleteuser(user_id)
    if result is None:
        raise HTTPException(status_code=500, detail="Error remove user")
    return result


@app.put("/edit-user/{user_id}")
def edit_user(
    user_id: int,
    username: str = Form(None),
    password: str = Form(None),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = editUser(user_id,username, password)
    if result is None:
        raise HTTPException(status_code=500, detail="Error edit user")
    return result


@app.get("/users")
def list_users(
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = listUser()
    if result is None:
        raise HTTPException(status_code=500, detail="Error get list  user")
    return result

@app.get('/password/{user_id}')
def get_password(
    user_id: int,
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = getPassword(user_id)
    if result is None:
        raise HTTPException(status_code=500, detail="Error get password  user")
    
    return result 

@app.post("/create-group", status_code=201)
async def create_group(
    group_name: str = Form(...),
    description: str = Form(None),
    server_url: str = Form(None),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                INSERT INTO groups (group_name, description, server_url, created_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id, group_name, description, server_url, created_at;
                """
                cur.execute(query, (group_name, description, server_url))
                new_group = cur.fetchone()
                return new_group
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/groups")
def list_groups(
    page: int = 1, 
    limit: int = 10,
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
                
):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Ambil semua group, misal urut berdasarkan id ascending
            cur.execute("SELECT id, group_name, description, server_url, created_at FROM groups ORDER BY id ASC")
            groups_data = cur.fetchall()
            total = len(groups_data)
            
            # Jika limit = 0, maka kembalikan seluruh data
            if limit == 0:
                paginated = groups_data
            else:
                start = (page - 1) * limit
                end = start + limit
                paginated = groups_data[start:end]
            
            return {
                "status": "success",
                "page": page,
                "limit": limit,
                "total_groups": total,
                "groups": paginated
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.put("/edit-group/{group_id}", status_code=200)
async def edit_group(
    group_id: int = Path(..., description="ID of the group to update"),
    group_name: str = Form(...),
    description: str = Form(None),
    server_url: str = Form(None),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Update dan kembalikan record yang baru
                query = """
                UPDATE groups
                   SET group_name  = %s,
                       description = %s,
                       server_url  = %s
                 WHERE id = %s
                RETURNING id, group_name, description, server_url, created_at;
                """
                cur.execute(query, (group_name, description, server_url, group_id))
                updated = cur.fetchone()
                if not updated:
                    raise HTTPException(status_code=404, detail="Group not found")
                return updated

    except HTTPException:
        # lempar ulang 404
        raise
    except Exception as e:
        # error lainnya jadi 500
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.delete("/delete-group/{group_id}", status_code=200)
async def delete_group(
    group_id: int = Path(..., description="ID of the group to delete"),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1) Pastikan group ada
                cur.execute("SELECT 1 FROM groups WHERE id = %s", (group_id,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Group not found")

                # 2) Hapus semua relasi di groups_devices
                cur.execute(
                    "DELETE FROM groups_devices WHERE group_id = %s",
                    (group_id,)
                )

                # 3) Hapus grup-nya
                cur.execute(
                    "DELETE FROM groups WHERE id = %s",
                    (group_id,)
                )

        return JSONResponse(
            status_code=200,
            content={"status": "success", "msg": "Group deleted successfully"}
        )

    except HTTPException:
        # Lempar ulang 404
        raise
    except Exception as e:
        # Error lain jadi 500
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/add-device", status_code=201)
async def add_device(
    serial_number: str   = Form(...),
    ip: str              = Form(...),
    group_id: int        = Form(None),
    lat: float           = Form(None),
    long: float          = Form(None),
    is_connected: bool   = Form(False),
    is_running: bool     = Form(False),
    current_user: dict   = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # 1) Masukkan ke tabel devices
                cur.execute("""
                    INSERT INTO devices
                        (serial_number, ip, lat, "long", is_connected, is_running, created_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING id, serial_number, ip, lat, "long", is_connected, is_running, created_at;
                """, (
                    serial_number,
                    ip,
                    lat,
                    long,
                    is_connected,
                    is_running
                ))
                device = cur.fetchone()
                device_id = device["id"]

                # 2) Masukkan mapping ke groups_devices jika ada group_id
                if group_id is not None:
                    cur.execute("""
                        INSERT INTO groups_devices (group_id, device_id)
                        VALUES (%s, %s)
                    """, (group_id, device_id))

        # 3) Kembalikan response
        return JSONResponse(
            status_code=201,
            content={
                "status": "success",
                "msg": "Device created successfully",
                "device": {
                    "id":               device["id"],
                    "serial_number":    device["serial_number"],
                    "ip":               device["ip"],
                    "group_id":         group_id,
                    "lat":              device["lat"],
                    "long":             device["long"],
                    "is_connected":     device["is_connected"],
                    "is_running":       device["is_running"],
                    "created_at":       device["created_at"].isoformat()
                }
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()


@app.get("/list-devices")
def list_device(
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    result = list_devices()
    if result is None:
        raise HTTPException(status_code=500, detail="Error retrieving list device")
    return result

@app.put("/edit-device/{device_id}", status_code=200)
async def edit_device(
    device_id: int       = Path(..., description="ID of the device to update"),
    serial_number: str   = Form(...),
    ip: str              = Form(...),
    group_id: int        = Form(None),
    lat: float           = Form(None),
    long: float          = Form(None),
    is_connected: bool   = Form(False),
    is_running: bool     = Form(False),
    current_user: dict   = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # 1) Pastikan device ada
                cur.execute("SELECT 1 FROM devices WHERE id = %s", (device_id,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Device not found")

                # 2) Update tabel devices
                cur.execute("""
                    UPDATE devices
                       SET serial_number = %s,
                           ip            = %s,
                           lat           = %s,
                           "long"        = %s,
                           is_connected  = %s,
                           is_running    = %s
                     WHERE id = %s
                     RETURNING id, serial_number, ip, lat, "long", is_connected, is_running, created_at;
                """, (
                    serial_number,
                    ip,
                    lat,
                    long,
                    is_connected,
                    is_running,
                    device_id
                ))
                updated = cur.fetchone()

                # 3) Perbarui mapping di groups_devices
                cur.execute("DELETE FROM groups_devices WHERE device_id = %s", (device_id,))
                if group_id is not None:
                    cur.execute("""
                        INSERT INTO groups_devices (group_id, device_id)
                        VALUES (%s, %s)
                    """, (group_id, device_id))

        # 4) Kembalikan response
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "msg": "Device updated successfully",
                "device": {
                    "id":               updated["id"],
                    "serial_number":    updated["serial_number"],
                    "ip":               updated["ip"],
                    "group_id":         group_id,
                    "lat":              updated["lat"],
                    "long":             updated["long"],
                    "is_connected":     updated["is_connected"],
                    "is_running":       updated["is_running"],
                    "created_at":       updated["created_at"].isoformat()
                }
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

@app.delete("/delete-device/{device_id}", status_code=200)
async def delete_device(
    device_id: int = Path(..., description="ID of the device to delete"),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1) Pastikan device ada
                cur.execute("SELECT 1 FROM devices WHERE id = %s", (device_id,))
                if cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="Device not found")

                # 2) Hapus mapping di groups_devices
                cur.execute(
                    "DELETE FROM groups_devices WHERE device_id = %s",
                    (device_id,)
                )

                # 3) Hapus device
                cur.execute(
                    "DELETE FROM devices WHERE id = %s",
                    (device_id,)
                )

        return JSONResponse(
            status_code=200,
            content={"status": "success", "msg": "Device deleted successfully"}
        )

    except HTTPException:
        # lempar ulang 404
        raise
    except Exception as e:
        # error lain jadi 500
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()



@app.post("/update-status-alert/{data_type}", status_code=200)
def update_status_alert(
    data_type: str = Path(..., description="Must be 'gsm' or 'lte'"),
    id: int = Form(...),
    campaign_id: int = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    # 1. Validate data_type
    table = ALLOWED_TABLES.get(data_type.lower())
    if not table:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="data_type must be 'gsm' or 'lte'"
        )

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 2. Build and execute the UPDATE
            sql = f"""
                UPDATE {table}
                SET status_alert = TRUE
                WHERE id = %s AND campaign_id = %s
            """
            cur.execute(sql, (id, campaign_id))
            if cur.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No row found in {table} for given id & campaign_id"
                )
            conn.commit()

        return JSONResponse({
            "status": "success",
            "message": f"status_alert on {data_type} has been set to TRUE"
        })
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating status_alert: {e}"
        )
    finally:
        conn.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8005)
