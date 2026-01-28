import json
import httpx
import asyncio
from typing import Dict, Any
from fastapi import WebSocket
from collections import defaultdict
from datetime import datetime, timedelta

import redis

# Import redis from config
from config import get_redis

user_connections = defaultdict(dict) # user_id: {client_id: websocket}.., etc. 


class PresenceManager:


    def __init__(self, redis):
        self.redis = redis # Shared Redis instance


    async def presence_listener(self, user_id: int, client_id: str, websocket: WebSocket, subscription_ready=None):
        """Listen for global presence updates"""
        
        if not self.redis:
            pass

        pubsub = self.redis.pubsub()
        print(f" -----------------> User {user_id} subscribing to presence_global")
        await pubsub.subscribe("presence_global")

        if subscription_ready is not None:
            subscription_ready.set()

        try:
            async for msg in pubsub.listen():
                print(f"Message for user {user_id}: {msg}")
                if msg["type"] != "message":
                    continue
                
                try:
                    data = json.loads(msg["data"])
                    print(f"Presence data for user {user_id}: {data}")
                    
                    # Check if this user cares about the presence update
                    # (You might want to filter based on user's contact list)
                    try:
                        print(f"```````````````````````````````````Sending presence update to user {user_id}: {data}")
                        await websocket.send_text(json.dumps(data))
                    except Exception as send_error:
                        # WebSocket might be closed
                        print(f"Error sending presence update to user {user_id}: {send_error}")
                        break
                        
                except Exception as e:
                    print(f"Error processing presence message: {e}")
                    continue
        
        except Exception as e:
            print(f"Presence listener error for user {user_id}: {e}")

        finally:
            try:
                await pubsub.unsubscribe("presence_global")
                await pubsub.close()
            except:
                pass

            


    async def add_connection(self, user_id: int, client_id: str, websocket: WebSocket):
        
        try:
            user_connections[user_id][client_id] = websocket
            now = datetime.utcnow().isoformat()

            await self.redis.hset(
                f"presence:{user_id}",
                mapping={
                    "status": "online",
                    "last_seen": now,
                })
            
            print(f"User connections after adding: {user_connections}")

            await self.redis.expire(f"presence:{user_id}", 60)
            
            print(f"Published presence online for user {user_id}")
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
            

    async def remove_connection(self, user_id: int, client_id: str):

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

    async def presence_sweeper():
        while True:
            await asyncio.sleep(10)

            users = await redis.keys("presence:*")

            for key in users:
                user_id = key.split(":")[1]

                alive = await redis.exists(f"heartbeat:{user_id}")

                if not alive:
                    await redis.hset(
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
                        await self.remove_connection(user_id, client_id)()


    











