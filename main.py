# main.py
import json
import uuid
import httpx
import asyncio
from datetime import datetime
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis  # pip install redis[async]

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",   # Django
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_URL = "redis://localhost:6379/0"

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


def roomName(user1: int, user2: int) -> str:
    a, b = sorted([int(user1), int(user2)])
    return f"private_{a}_{b}"

def group_room_name(room_id: int) -> str:
    return f"group_{room_id}"

# async def persist_message_to_db(sender_id: int, to_user: int, text: str):     
    # await asyncio.sleep(0.05)  # simulate small IO latency
    # # Example: success -> return message_id, timestamp
    # # To simulate failure, raise Exception
    # message_id = str(uuid.uuid4())
    # timestamp = datetime.utcnow().isoformat() + "Z"
    # # return a dict representing saved row
    # return {"message_id": message_id, "timestamp": timestamp}\

async def persist_message_to_db(sender_id: int, to_user: int, text: str):
    """
    Persist the message and return an official message_id and timestamp.
    Replace this with:
      - call to Django REST endpoint to save message, or
      - direct DB save using an async ORM.
    
    Simulate success/failure here for demo.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "http://127.0.0.1:8000/chat/save-message/",
            json={
                "sender": sender_id,
                "recipient": to_user,
                "body": text,
                "delivered": True,
            },
            timeout=5.0
        )

    data = resp.json()
    return {
        "message_id": data["message_id"],
        "timestamp": data["timestamp"]
    }

async def saveGroupMessage(user_id: int, room_id: int, message: str):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "http://127.0.0.1:8000/chat/save-group-message/",
            json={
                "room": room_id,
                "sender": user_id,
                "message": message
            }
        )

    data = resp.json()
    return {
        "message_id": data["message_id"],
        "timestamp": data["timestamp"]
    }

async def updateLastSeen(user_id: int, last_seen: str):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "http://127.0.0.1:8000/chat/update-last-seen/",
            json={
                "user_id": user_id,
                "last_seen": last_seen
            }
        )

    data = resp.json()
    return {
        "status": data["status"],
        "message": data["message"],
        "user_id": data["user_id"],
        "last_seen": data["last_seen"]
    }


async def publishRoomMessage(room: str, payload: dict):
    """Publish to Redis channel for cross-process broadcast"""
    await redis.publish(room, json.dumps(payload))


async def subscribeToRoomPubsub(websocket: WebSocket, room: str):
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
                if payload.get("type") == "chat" and payload.get("one_to_one"):
                    sender_id = payload.get("sender_id")
                    recipient_id = payload.get("recipient_id")
        
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
                        await publishRoomMessage(room, delivered_ack)
                
                if payload.get("type") == "chat" and payload.get("many_to_many"):
                    sender_id = payload.get("sender_id")
                    participant_ids = payload.get("participant_ids")  # already a Python list

                    print(f"Participant IDs Type: {type(participant_ids)} --> Python list")

                    # Correct condition
                    if getattr(websocket, "user_id", None) in participant_ids:
                        delivered_ack = {
                            "type": "receipt",
                            "temp_id": payload.get("temp_id"),
                            "status": "delivered_to_recipients",
                            "server_time": datetime.utcnow().isoformat() + "Z",
                        }

                        await publishRoomMessage(room, delivered_ack)

                        
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

async def presence_listener(websocket: WebSocket):
    pubsub = redis.pubsub()
    await pubsub.subscribe("presence_global")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        try:
            await websocket.send_text(msg["data"])
        except Exception:
            break

@app.websocket("/ws/presence/{user_id}")
async def presence_socket(websocket: WebSocket, user_id: int):
    await websocket.accept()

    # 🔐 Bind identity to the socket
    websocket.user_id = user_id

    # 🟢 Mark ONLINE
    await redis.set(f"online:{websocket.user_id}", "1")

    # 🔔 Broadcast ONLINE
    await redis.publish(
        "presence_global",
        json.dumps({
            "type": "presence",
            "user_id": websocket.user_id,
            "status": "online"
        })
    )

    # 🟢 START Redis listener
    listener_task = asyncio.create_task(
        presence_listener(websocket)
    )

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        pass

    finally:
        listener_task.cancel()

        last_seen = datetime.utcnow().isoformat() + "Z"

        await redis.delete(f"online:{websocket.user_id}")
        await redis.set(f"last_seen:{websocket.user_id}", last_seen)

        await redis.publish(
            "presence_global",
            json.dumps({
                "type": "presence",
                "user_id": websocket.user_id,
                "status": "offline",
                "last_seen": last_seen
            })
        )

        await updateLastSeen(websocket.user_id, last_seen)


@app.get("/user/{user_id}/last_seen")
async def get_last_seen(user_id: int):
    online = await redis.exists(f"online:{user_id}")

    if online:
        return {"status": "online"}

    last_seen = await redis.get(f"last_seen:{user_id}")

    return {
        "status": "offline",
        "last_seen": last_seen.decode() if last_seen else None
    }

@app.websocket("/ws/chat/{user_id}/{to_user}")
async def chatSocket(websocket: WebSocket, user_id: int, to_user: int):
    await websocket.accept()

    websocket.user_id = user_id  # attach it for identification ✅
    
    room = roomName(user_id, to_user)

    # Register connection in-memory
    active_rooms.setdefault(room, []).append(websocket)

    # Start background Redis subscription for this websocket
    redis_task = asyncio.create_task(subscribeToRoomPubsub(websocket, room))

    # Inform client server connection established
    await websocket.send_text(json.dumps({"type": "system", "message": "Connection Established"}))

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
                await websocket.send_text(json.dumps({
                    "type": "receipt",
                    "temp_id": temp_id,
                    "status": "received_by_server",
                    "server_time": datetime.utcnow().isoformat() + "Z",
                }))

                # 2) Persist message (try / except)
                try:
                    saved = await persist_message_to_db(int(user_id), int(to_user), text)
                    # saved => {"message_id": "...", "timestamp": "..."}
                    payload = {
                        "type": "chat",
                        "one_to_one": True,
                        "message_id": saved["message_id"],
                        "sender_id": int(user_id),
                        "recipient_id": int(to_user),
                        "message": text,
                        "timestamp": saved["timestamp"],
                        "temp_id": temp_id,  # so clients can reconcile
                    }

                    # 3) publish to Redis so *all* server instances will broadcast to their connected websockets
                    await publishRoomMessage(room, payload)

                    # (Optional) also publish delivery receipt if you want delivery stages
                    # delivered_payload = {...}
                    # await publish_room_message(room, delivered_payload)

                except Exception as e:
                    # Persistence failed -> notify sender only with failure
                    payload = {
                        "type": "receipt",
                        "temp_id": temp_id,
                        "status": "failed",
                        "error": str(e),
                        "server_time": datetime.utcnow().isoformat() + "Z",
                    }
                    await websocket.send_text(json.dumps(payload))

            # === typing indicator ===
            elif data.get("type") == "typing":
                typing_payload = {
                    "type": "typing",
                    "user_id": int(user_id),
                    "to_user": int(to_user),
                    "is_typing": bool(data.get("is_typing", False)),
                }
                # publish typing event to room via Redis; frontend clients will display it transiently
                await publishRoomMessage(room, typing_payload)

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

@app.websocket("/ws/group/{user_id}/{group_id}/{participant_ids}")
async def group_chat_socket(websocket: WebSocket, user_id: int, group_id: int, participant_ids: str):
    await websocket.accept()

    websocket.user_id = user_id # attach it for identification ✅
    
    group = group_room_name(group_id)
    participant_ids = [int(p) for p in participant_ids.split(",") if p.strip()]

    # Register connection in-memory
    active_rooms.setdefault(group, []).append(websocket)

    # Start background Redis subscription for this websocket
    redis_task = asyncio.create_task(subscribeToRoomPubsub(websocket, group))

    # Inform client server connection established
    await websocket.send_text(json.dumps({"type": "system", "message": "Connected to group"}))

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)

            # === Chat message ===
            if data.get("type") == "chat":
                temp_id = data.get("temp_id")
                text = data.get("message")
                sender_avatar = data.get("sender_avatar")

                # Acknowledge to sender
                await websocket.send_text(json.dumps({
                    "type": "receipt",
                    "temp_id": temp_id,
                    "status": "received_by_server",
                    "server_time": datetime.utcnow().isoformat() + "Z",
                }))

                # Save to DB (group message)
                try:
                    saved = await saveGroupMessage(
                        user_id=user_id,
                        room_id=group_id,
                        message=text
                    )

                    # Broadcast message to group
                    payload = {
                        "type": "chat",
                        "many_to_many": True,
                        "message_id": saved["message_id"],
                        "sender_id": user_id,
                        "sender_avatar": sender_avatar,
                        "room_id": group_id,
                        "message": text,
                        "participant_ids": participant_ids,
                        "timestamp": saved["timestamp"],
                        "temp_id": temp_id,
                    }

                    await publishRoomMessage(group, payload)

                except Exception as e:
                    await websocket.send_text(json.dumps({
                        "type": "receipt",
                        "temp_id": temp_id,
                        "status": "failed",
                        "error": str(e)
                    }))

            # === Typing indicator ===
            elif data.get("type") == "typing":
                await publishRoomMessage(group, {
                    "type": "typing",
                    "user_id": user_id,
                    "room_id": group_id,
                    "is_typing": bool(data.get("is_typing", False)),
                })

    except WebSocketDisconnect:
        pass
    finally:
         # cleanup: remove connection and cancel pubsub task
        try:
            active_rooms[group].remove(websocket)
        except Exception:
            pass
        if not active_rooms.get(group):
            active_rooms.pop(group, None)

        redis_task.cancel()
        try:
            await redis_task
        except Exception:
            pass
