from fastapi import APIRouter, HTTPException, Depends, Form
from datetime import datetime
import psycopg2
import psycopg2.extras
from database_config import get_db_connection
import jwt
from cryptography.fernet import Fernet
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

KEY = os.environ.get("KEY").encode() 
fernet = Fernet(KEY)

def encrypt_password(password: str) -> str:
    encrypted = fernet.encrypt(password.encode())
    return encrypted.decode()

def decrypt_password(encrypted_password: str) -> str:
    decrypted = fernet.decrypt(encrypted_password.encode())
    return decrypted.decode()

def verify_password(plain_password: str, encrypted_password: str) -> bool:
    try:
        return plain_password == decrypt_password(encrypted_password)
    except Exception:
        return False
    
# Konfigurasi JWT
SECRET_KEY = os.environ.get("SECRET_KEY")
ALGORITHM = os.environ.get("ALGORITHM")

def create_access_token(data: dict) -> str:
    token = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

@router.post("/register", status_code=201)
async def register(
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form("admin"),
    group_id: int = Form(None)
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Cek apakah group dengan group_id tersedia (user_id masih NULL)
                if group_id !=None:
                    cur.execute("SELECT id, group_name, user_id FROM groups WHERE id = %s", (group_id,))
                    group = cur.fetchone()
                    if not group:
                        raise HTTPException(status_code=404, detail="Group tidak ditemukan")
                    if group["user_id"] is not None:
                        raise HTTPException(status_code=400, detail="Group sudah dipilih oleh user lain")
                
                encrypted_password = encrypt_password(password) 
                # Insert user baru ke tabel users
                user_query = """
                    INSERT INTO users (username, password, role, created_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING id, username, role, created_at;
                """
                cur.execute(user_query, (username, encrypted_password, role))
                new_user = cur.fetchone()
                user_id = new_user["id"]
                
                # Update group dengan mengaitkan user_id yang baru dibuat
                update_group_query = """
                    UPDATE groups
                    SET user_id = %s
                    WHERE id = %s
                    RETURNING id, group_name, description, user_id, server_url, created_at;
                """
                cur.execute(update_group_query, (user_id, group_id))
                updated_group = cur.fetchone()
                
                response = {
                    "status": "success",
                    "msg": "User created successfully",
                    "user": {
                        "id": new_user["id"],
                        "username": new_user["username"],
                        "created_at": new_user["created_at"].isoformat()
                    },
                    "group": {
                        "id": updated_group["id"],
                        "group_name": updated_group["group_name"],
                        "created_at": updated_group["created_at"].isoformat()
                    }
                }

                return response
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.post("/login")
def login(
    username: str = Form(...),
    password: str = Form(...)
):
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query, (username,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        
        if not verify_password(password, user["password"]):
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        
        access_token = create_access_token(data={"sub": str(user["id"])})
        return {
            "status": "success",
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user["id"],
                "username": user["username"],
                "role": user["role"]
            }
        }
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {e}")
    finally:
        cursor.close()
        connection.close()
