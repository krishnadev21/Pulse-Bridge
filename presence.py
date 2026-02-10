import time as t
import json
import redis
import asyncio
from typing import Dict, Any
from fastapi import WebSocket
from collections import defaultdict
from datetime import datetime, time, timedelta
from starlette.websockets import WebSocketState


user_connections = defaultdict(dict) # user_id: {client_id: websocket}.., etc. 


class PresenceManager:
    def __init__(self, redis):
        self.redis = redis # Shared Redis instance

    async def presenceListener(self, user_id: int, websocket: WebSocket):
        """Listen for global presence updates"""
        
        if not self.redis: 
            return

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("presence_global")
        print(f"{datetime.now().strftime('%I:%M:%S %p')} - User {user_id} subscribed to global presence updates")

        try:
            async for msg in pubsub.listen():
                if websocket.client_state != WebSocketState.CONNECTED: 
                    return
                if msg["type"] != "message": 
                    continue
                
                try:
                    data = json.loads(msg["data"])

                    # FILTER: Don't send the user their own status updates
                    if data.get("user_id") == user_id: 
                        continue  # Skip self-updates

                    await websocket.send_text(json.dumps(data))

                except json.JSONDecodeError as json_error:
                    print(f"Error decoding presence message for user {user_id}: {json_error}")
                    break 

                except asyncio.CancelledError:
                    break
                
                except Exception as send_error:
                    print(f"Error sending presence update to user {user_id}: {send_error}")
                    break

        except asyncio.CancelledError:
            raise                    
        
        except Exception as e:
            print(f"Presence listener error for user {user_id}: {e}")

        finally:
            await pubsub.unsubscribe("presence_global")
            await pubsub.close()


            
    async def addConnection(self, user_id: int, client_id: str, websocket: WebSocket):
        try:
            user_connections[user_id][client_id] = websocket
            now = datetime.utcnow().isoformat()

            t1 = t.perf_counter()
            await self.redis.hset(
                f"presence:{user_id}",
                mapping={
                    "status": "online",
                    "last_seen": now,
                })
            print("HSET took:", t.perf_counter() - t1)
            
            # print(f"User connections after adding: {user_connections}")
            await self.redis.expire(f"presence:{user_id}", 60)

            # 1. Send immediate status to THIS client
            await websocket.send_text(json.dumps({
                "type": "presence",
                "status": "online",
                "user_id": user_id,
                "last_seen": now,
                "source": "direct"  # Optional: indicate this is direct message
            }))
            
            print(f"{datetime.now().strftime('%I:%M:%S %p')} - Published presence online for user {user_id}")
            await self.redis.publish(
                "presence_global",
                json.dumps({
                    "type": "presence",
                    "status": "online",
                    "user_id": user_id,
                    "last_seen": now
                })
            )

        except Exception as e:
            await redis.publish(
                "presence_global",
                json.dumps({
                    "type": "presence",
                    "user_id": user_id,
                    "status": "error",
                }))
            

    async def removeConnection(self, user_id: int, client_id: str):
        try:
            user_connections[user_id].pop(client_id, None)

            if user_connections[user_id]:
                return # still online on another device

            now = datetime.utcnow().isoformat() + "Z"
        
            await self.redis.hset(
                f"presence:{user_id}",
                mapping={
                    "status": "offline",
                    "last_seen": now,
                }
            )

            print(f"Published presence offline for user {user_id}")
            await self.redis.publish(
                "presence_global",
                json.dumps({
                    "type": "presence",
                    "status": "offline",
                    "user_id": user_id,
                    "last_seen": now
                })
            )

        except Exception as e:
            await self.redis.publish(
                "presence_global",
                json.dumps({
                    "type": "presence",
                    "user_id": user_id,
                    "status": "error",
                })
            )


    async def get_user_presence(self, user_id: int) -> Dict[str, Any]:
        if not self.redis:
            return {
                "status": "offline",
                "last_seen": None,
            }

        user_presence = await self.redis.hgetall(f"presence:{user_id}")

        if not user_presence:
            return {
                "status": "offline",
                "last_seen": None,
            }

        return {
            "status": user_presence.get("status", "offline"),
            "last_seen": user_presence.get("last_seen"),
        }

    async def presence_sweeper(self):
        while True:
            await asyncio.sleep(10 + (0.1 if not hasattr(self, '_jittered') else 0))

            users = await self.redis.keys("presence:*")

            for key in users:
                user_id = key.split(":")[1]

                alive = await self.redis.exists(f"heartbeat:{user_id}")

                if not alive:
                    await self.redis.hset(
                        f"presence:{user_id}",
                        mapping={
                            "status": "offline",
                            "last_seen": datetime.utcnow().isoformat()
                        }
                    )

    async def cleanup_stale_connections(self):
        """Remove connections that haven't sent heartbeat in a while"""
        if not self.redis:
            return
        
        while True:
            await asyncio.sleep(60)  # Check every minute
            now = datetime.now()
            stale_threshold = timedelta(minutes=2)
            
            for user_id, connections in list(user_connections.items()):
                for client_id, connection in list(connections.items()):
                    if now - connection['last_heartbeat'] > stale_threshold:
                        print(f"Removing stale connection: user={user_id}, client={client_id}")
                        await self.removeConnection(user_id, client_id)


    











