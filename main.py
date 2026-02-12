# main.py
import json
import uuid
import httpx
import asyncio
import time as t
from typing import Dict, List
from collections import defaultdict
from django.http import JsonResponse
from datetime import datetime, timedelta

from fastapi import Body, Request
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Response, status
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import init_redis, get_redis, close_redis
from presence import PresenceManager

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",   # Django
        "http://127.0.0.1:8000",   # Django (IP)
        "http://localhost:8001",   # FastAPI
        "http://127.0.0.1:8001",   # FastAPI (IP)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis = None
presence_manager = None

# Startup and shutdown events
@app.on_event("startup")
async def startup():
    global redis, presence_manager
    redis = await init_redis()
    presence_manager = PresenceManager(redis)
    print("Presence manager ready")



@app.on_event("shutdown")
async def shutdown():
    # Close Redis connection
    await close_redis()
    print("Redis connection closed")

@app.get("/favicon.ico", status_code=status.HTTP_204_NO_CONTENT)
async def favicon():
    return Response(status_code=status.HTTP_204_NO_CONTENT)


#  =============================================================================================================



# Helper function to serialize datetime objects
def serializeDateTime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


@app.websocket("/ws/presence/{user_id}")
async def presenceSocket(websocket: WebSocket, user_id: int):
    await websocket.accept()

    client_id = str(uuid.uuid4())
    websocket.user_id = user_id
    websocket.client_id = client_id

    print(f"{datetime.now().strftime('%I:%M:%S %p')} - User {user_id} connected with client ID {client_id}")
    listener_task = asyncio.create_task(
        presence_manager.presenceListener(user_id, websocket)
    )
    
    print(f"{datetime.now().strftime('%I:%M:%S %p')} - Starting heartbeat loop for user {user_id}, client {client_id}")
    await presence_manager.addConnection(user_id, client_id, websocket)

    try:
        while True:
            try:
                data = await websocket.receive_text()

                message = json.loads(data)

                message_type = message.get('type')

                if message_type == 'heartbeat':
                    pass
                    # await presence_manager.update_heartbeat(user_id, client_id)
                
                elif message_type == 'visibility_change':
                    pass
                    # is_visible = message.get('is_visible', True)
                    # await presence_manager.update_visibility(user_id, client_id, is_visible)
                
                elif message_type == 'connection_metadata':
                    pass

            except WebSocketDisconnect:
                print(f"{datetime.now().strftime('%I:%M:%S %p')}: WebSocket disconnected for user {user_id}")
                return  # 🔴 terminate task
            
            except asyncio.CancelledError:
                return  # 🔴 terminate task
            
            except json.JSONDecodeError:
                await presence_manager.update_heartbeat(user_id, client_id)

            except Exception as e:
                print(f"Heartbeat error for user {user_id}: {e}")
                return
        
    finally:
        # print(f"Cleaning up presence for user {user_id}, client {client_id}"
        listener_task.cancel()
        await presence_manager.removeConnection(user_id, client_id)
  



# @app.post("/users/presence")
# async def get_users_presence(request: Request):
#     """Get presence status for multiple users"""
#     try:
#         data = await request.json()
#         user_ids = data.get("user_ids", [])
        
#         result = {}
#         for user_id in user_ids:
#             presence = await presence_manager.get_user_presence(user_id)
#             result[str(user_id)] = {
#                 "status": presence['status'],
#                 "last_seen": presence['last_seen']
#             }
        
#         return result
#     except Exception as e:
#         print(f"Error in /users/presence: {e}")
#         return {"error": str(e)}

@app.post("/users/presence")
async def get_users_presence(request: Request):
    data = await request.json()
    user_ids = data.get("user_ids", [])

    if not user_ids:
        return {}

    pipe = redis.pipeline()
    for user_id in user_ids:
        pipe.hgetall(f"presence:{user_id}")
    # print(f"==================================================> {pipe}")

    raw_results = await pipe.execute()
    # print(f" =============================? {raw_results}")

    result = {}
    for user_id, data in zip(user_ids, raw_results):
        if data:
            result[str(user_id)] = {
                "status": data.get("status", "offline"),
                "last_seen": data.get("last_seen", 0),
            }
        else:
            result[str(user_id)] = {
                "status": "offline",
                "last_seen": None
            }
    # print(f" =============================? {result}")
    return result



@app.get("/user/{user_id}/last_seen")
async def get_last_seen(user_id: int):
    """Get last seen timestamp for a user"""
    try:
        presence = await presence_manager.get_user_presence(user_id)
        
        return {
            "user_id": user_id,
            "status": presence['status'],
            "last_seen": presence['last_seen']
        }
    except Exception as e:
        print(f"Error in /user/{user_id}/last_seen: {e}")
        return {
            "user_id": user_id,
            "status": "offline",
            "last_seen": datetime.now().isoformat()
        }


# =================================================================================================================


# Connections in memory (per process). Use Redis pub/sub to broadcast across instances.
active_rooms: Dict[str, List[WebSocket]] = {}

def roomName(user1: int, user2: int) -> str:
    a, b = sorted([int(user1), int(user2)])
    return f"private_{a}_{b}"


def groupRoomName(room_id: int) -> str:
    return f"group_{room_id}"


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


async def publishRoomMessage(room: str, payload: dict):
    """Publish to Redis channel for cross-process broadcast"""
    redis = await get_redis()
    await redis.publish(room, json.dumps(payload))


async def subscribeToRoomPubsub(websocket: WebSocket, room: str):
    """Create a per-connection task to listen to Redis pubsub and forward messages to websocket."""
    try:
        redis = await get_redis()
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
                sender_avatar = data.get("sender_avatar")

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
                        "message": text,
                        "temp_id": temp_id,  # so clients can reconcile
                        "one_to_one": True,
                        "sender_id": int(user_id),
                        "recipient_id": int(to_user),
                        "sender_avatar": sender_avatar,
                        "timestamp": saved["timestamp"],
                        "message_id": saved["message_id"],
                    }

                    # 3) publish to Redis so *all* server instances will broadcast to their connected websockets
                    await publishRoomMessage(room, payload)

                    # (Optional) also publish delivery receipt if you want delivery stages
                    # delivered_payload = {...}
                    # await publish_room_message(room, delivered_payload)

                except Exception as e:
                    # Persistence failed -> notify sender only with failure
                    payload = {
                        "error": str(e),
                        "type": "receipt",
                        "status": "failed",
                        "temp_id": temp_id,
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
    
    group = groupRoomName(group_id)
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
                        "message": text,
                        "temp_id": temp_id,
                        "room_id": group_id,
                        "many_to_many": True,
                        "sender_id": user_id,
                        "sender_avatar": sender_avatar,
                        "timestamp": saved["timestamp"],
                        "message_id": saved["message_id"],
                        "participant_ids": participant_ids,
                    }

                    await publishRoomMessage(group, payload)

                except Exception as e:
                    await websocket.send_text(json.dumps({
                        "error": str(e),
                        "type": "receipt",
                        "status": "failed",
                        "temp_id": temp_id,
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
