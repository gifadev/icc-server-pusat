from typing import List,Tuple
from fastapi import WebSocket, WebSocketDisconnect

class WebSocketManager:
    def __init__(self):
        # Simpan tuple (campaign_id, websocket)
        self.active_connections: List[Tuple[int, WebSocket]] = []

    async def connect(self, campaign_id: int, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append((campaign_id, websocket))
        print(f"New connection for campaign {campaign_id}. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        # Hapus tuple dengan websocket yang sama
        self.active_connections = [
            (cid, ws) for cid, ws in self.active_connections if ws != websocket
        ]

    async def broadcast(self, campaign_id: int, message: str):
        for cid, connection in self.active_connections:
            if cid == campaign_id:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    print(f"Error broadcasting message for campaign {campaign_id}: {e}")

    async def close_campaign_connections(self, campaign_id: int):
        to_close = [ws for cid, ws in self.active_connections if cid == campaign_id]
        for ws in to_close:
            try:
                await ws.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
            self.disconnect(ws)

# Instansiasi objek WebSocketManager yang akan digunakan di seluruh aplikasi
manager = WebSocketManager()
