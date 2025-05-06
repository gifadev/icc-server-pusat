import asyncio
import websockets
import json

async def listen_ws(campaign_id):
    uri = f"ws://172.15.3.181:8005/ws/{campaign_id}"
    try:
        async with websockets.connect(uri) as websocket:
            print(f"✅ Terhubung ke WebSocket Server untuk campaign {campaign_id}...")
            while True:
                message = await websocket.recv()
                try:
                    data = json.loads(message)
                    print(f"📩 Data diterima:\n{json.dumps(data, indent=2)}")
                except json.JSONDecodeError:
                    print(f"⚠️ Gagal decode JSON: {message}")
    except websockets.exceptions.InvalidStatus as e:
        print(f"Error: Server rejected connection: {e}")
    except Exception as e:
        print(f"Error pada koneksi {uri}: {e}")

if __name__ == "__main__":
    campaign_id = input("Masukkan Campaign ID: ")
    asyncio.run(listen_ws(campaign_id))
