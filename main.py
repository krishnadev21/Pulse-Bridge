# main.py
import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis  # pip install redis[async]

REDIS_URL = "redis://localhost:6379/0"
app = FastAPI()

# Connections in memory (per process). Use Redis pub/sub to broadcast across instances.
active_rooms: Dict[str, List[WebSocket]] = {}

# Redis client (async)
redis = None

@app.on_event("startup")
async def startup():
    global redis
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)


@app.on_event("shutdown")
async def shutdown():
    global redis
    if redis:
        await redis.close()


def room_name(user1: int, user2: int) -> str:
    a, b = sorted([int(user1), int(user2)])
    return f"private_{a}_{b}"


async def persist_message_to_db(sender_id: int, to_user: int, text: str):
    """
    Persist the message and return an official message_id and timestamp.
    Replace this with:
      - call to Django REST endpoint to save message, or
      - direct DB save using an async ORM.
    Simulate success/failure here for demo.
    """
    await asyncio.sleep(0.05)  # simulate small IO latency
    # Example: success -> return message_id, timestamp
    # To simulate failure, raise Exception
    message_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat() + "Z"
    # return a dict representing saved row
    return {"message_id": message_id, "timestamp": timestamp}\

# import httpx

# async def persist_message_to_db(sender_id: int, to_user: int, text: str):
#     async with httpx.AsyncClient() as client:
#         resp = await client.post(
#             "http://127.0.0.1:8000/chat/save-message/",
#             json={
#                 "sender": sender_id,
#                 "recipient": to_user,
#                 "body": text,
#             },
#             timeout=5.0
#         )

#     data = resp.json()
#     return {
#         "message_id": data["message_id"],
#         "timestamp": data["timestamp"]
#     }

async def publish_room_message(room: str, payload: dict):
    """Publish to Redis channel for cross-process broadcast"""
    await redis.publish(room, json.dumps(payload))


async def subscribe_to_room_pubsub(websocket: WebSocket, room: str):
    """Create a per-connection task to listen to Redis pubsub and forward messages to websocket."""
    try:
        pubsub = redis.pubsub()
        await pubsub.subscribe(room)

        async for msg in pubsub.listen():
            # msg example: {'type':'message','pattern':None,'channel':'private_1_2','data':'...'}
            if msg is None:
                continue
            if msg.get("type") != "message":
                continue
            data = msg.get("data")
            
            try:
                payload = json.loads(data)
            except Exception:
                payload = {"type": "raw", "data": data}

            # Forward to this websocket (keep safe)
            try:
                await websocket.send_text(json.dumps(payload))

                 # 🟢 Step 2: If this websocket belongs to the recipient, send delivery ACK back to sender
                if payload.get("type") == "chat":
                    sender_id = payload.get("sender_id")
                    recipient_id = (
                        int(room.split("_")[2]) if str(sender_id) == room.split("_")[1]
                        else int(room.split("_")[1])
                    )
                    # Send a "delivered_to_recipient" ACK only if this websocket belongs to recipient
                    # meaning: user_id == recipient_id
                    # But to know websocket's user_id, we pass it in closure via partial or attr
                    if getattr(websocket, "user_id", None) == recipient_id:
                        delivered_ack = {
                            "type": "receipt",
                            "temp_id": payload.get("temp_id"),
                            "status": "delivered_to_recipient",
                            "server_time": datetime.utcnow().isoformat() + "Z",
                        }
                        # Publish ACK to room, sender will pick it up
                        await publish_room_message(room, delivered_ack)
                        
            except WebSocketDisconnect:
                break  # Exit loop if client disconnected
            except Exception as e:
                print(f"WebSocket send error: {e}")
                break

    except asyncio.CancelledError:
        # Normal: happens when client disconnects
        pass

    finally:
        try:
            await pubsub.unsubscribe(room)
            await pubsub.close()
        except Exception:
            pass


@app.websocket("/ws/chat/{user_id}/{to_user}")
async def chat_socket(websocket: WebSocket, user_id: int, to_user: int):
    await websocket.accept()

    websocket.user_id = user_id  # attach it for identification ✅
    
    room = room_name(user_id, to_user)

    # Register connection in-memory
    active_rooms.setdefault(room, []).append(websocket)

    # Start background Redis subscription for this websocket
    redis_task = asyncio.create_task(subscribe_to_room_pubsub(websocket, room))

    # Inform client server connection established <system>
    await websocket.send_text(json.dumps({"type": "chat", "message": "Connection Established"}))

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)

            # === Incoming client message event ===
            # Client should send { type: "chat", temp_id: "...", message: "Hello" }
            if data.get("type") == "chat":
                temp_id = data.get("temp_id") or str(uuid.uuid4())  # client-provided temp id for optimistic UI
                text = data.get("message", "")

                # 1) Send immediate server-ACK (receipt) back to *sender* only:
                #    lets the client know the server got it (but hasn't persisted yet).
                #    Use 'receipt' status to update local UI from 'pending' -> 'received_by_server'
                receipt_payload = {
                    "type": "receipt",
                    "temp_id": temp_id,
                    "status": "received_by_server",
                    "server_time": datetime.utcnow().isoformat() + "Z",
                }
                await websocket.send_text(json.dumps(receipt_payload))

                # 2) Persist message (try / except)
                try:
                    saved = await persist_message_to_db(int(user_id), int(to_user), text)
                    # saved => {"message_id": "...", "timestamp": "..."}
                    official_payload = {
                        "type": "chat",
                        "message_id": saved["message_id"],
                        "sender_id": int(user_id),
                        "message": text,
                        "timestamp": saved["timestamp"],
                        "temp_id": temp_id,  # so clients can reconcile
                    }

                    # 3) publish to Redis so *all* server instances will broadcast to their connected websockets
                    await publish_room_message(room, official_payload)

                    # (Optional) also publish delivery receipt if you want delivery stages
                    # delivered_payload = {...}
                    # await publish_room_message(room, delivered_payload)

                except Exception as e:
                    # Persistence failed -> notify sender only with failure
                    fail_payload = {
                        "type": "receipt",
                        "temp_id": temp_id,
                        "status": "failed",
                        "error": str(e),
                        "server_time": datetime.utcnow().isoformat() + "Z",
                    }
                    await websocket.send_text(json.dumps(fail_payload))

            # === typing indicator ===
            elif data.get("type") == "typing":
                typing_payload = {
                    "type": "typing",
                    "user_id": int(user_id),
                    "is_typing": bool(data.get("is_typing", False)),
                }
                # publish typing event to room via Redis; frontend clients will display it transiently
                await publish_room_message(room, typing_payload)

            # handle other event types...
    except WebSocketDisconnect:
        pass
    finally:
        # cleanup: remove connection and cancel pubsub task
        try:
            active_rooms[room].remove(websocket)
        except Exception:
            pass
        if not active_rooms.get(room):
            active_rooms.pop(room, None)

        redis_task.cancel()
        try:
            await redis_task
        except Exception:
            pass
