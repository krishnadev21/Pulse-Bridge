import json
import httpx
import asyncio
from typing import Dict, Any
from fastapi import WebSocket
from collections import defaultdict
from datetime import datetime, timedelta

# Import redis from config
from config import get_redis

# Store multiple connections per user
user_connections = defaultdict(dict)  # user_id -> {client_id: websocket}
user_presence = {}  # user_id -> {status, last_seen, active_connections}


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

class PresenceManager:
    def __init__(self, redis):
        """Initialize with shared Redis connection"""
        self.redis = redis  # Get the SHARED Redis instance (no await!)
        # print(f"PresenceManager initialized with Redis: {self.redis}")

    async def presence_listener(self, websocket: WebSocket, client_id: str, user_id: int, subscription_ready=None):
        """Listen for global presence updates"""

        # print(f" --------------------> Presence Global redis: {self.redis}")

        if not self.redis:
            print(f"Redis not available for user {user_id}")
            return
        
        pubsub = self.redis.pubsub()
        # print(f" -----------------> User {user_id} subscribing to presence_global")
        await pubsub.subscribe("presence_global")
        if subscription_ready is not None:
            subscription_ready.set()
        
        try:
            async for msg in pubsub.listen():
                # print(f"Presence message for user {user_id}: {msg}")
                if msg["type"] != "message":
                    continue

                try:
                    data = json.loads(msg["data"])
                    # print(f"Presence data for user {user_id}: {data}")
                    
                    # Check if this user cares about the presence update
                    # (You might want to filter based on user's contact list)
                    try:
                        # print(f"Sending presence update to user {user_id}: {data}")
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

    
    # REMOVE @staticmethod - use instance methods!
    async def add_connection(self, user_id: int, client_id: str, websocket: WebSocket):
        """Add a new connection for a user"""
        if not self.redis:
            print("Redis not available in add_connection")
            return
        
        user_connections[user_id][client_id] = {
            'websocket': websocket,
            'last_heartbeat': datetime.now(),
            'is_visible': True,
            'client_id': client_id
        }

        print(user_connections)

        # Update presence status
        user_presence[user_id] = {
            'status': 'online',
            'last_seen': datetime.now().isoformat(),
            'active_connections': len(user_connections[user_id])
        }

        print(user_presence)
        
        # Store in Redis
        await self.redis.set(f"online:{user_id}", "1")
        
        # Publish presence update
        try:
            await self.redis.publish(
                "presence_global",
                json.dumps({
                    "type": "presence",
                    "user_id": user_id,
                    "status": "online",
                    "last_seen": datetime.now().isoformat(),
                    "active_connections": len(user_connections[user_id])
                })
            )
        except Exception as e:
            print(f"Error publishing presence for user {user_id}: {e}")
    
    async def remove_connection(self, user_id: int, client_id: str):
        """Remove a specific connection for a user"""
        if not self.redis:
            print("Redis not available in remove_connection")
            return
        
        if user_id in user_connections and client_id in user_connections[user_id]:
            # Remove the specific connection
            del user_connections[user_id][client_id]
            
            # If no connections left, mark as offline
            if not user_connections[user_id]:
                # Remove from Redis
                try:
                    await self.redis.delete(f"online:{user_id}")
                    
                    last_seen = datetime.utcnow().isoformat() + "Z"
                    await self.redis.set(f"last_seen:{user_id}", str(last_seen))
                    
                    # Update Django database
                    await updateLastSeen(user_id, last_seen)
                except Exception as e:
                    print(f"Error updating Redis for user {user_id}: {e}")
                
                user_presence[user_id] = {
                    'status': 'offline',
                    'last_seen': datetime.now().isoformat(),
                    'active_connections': 0
                }
            else:
                # Still has other connections, remain online
                user_presence[user_id] = {
                    'status': 'online',
                    'last_seen': datetime.now().isoformat(),
                    'active_connections': len(user_connections[user_id])
                }
            
            # Publish presence update
            try:
                await self.redis.publish(
                    "presence_global",
                    json.dumps({
                        "type": "presence",
                        "user_id": user_id,
                        "status": user_presence[user_id]['status'],
                        "last_seen": user_presence[user_id]['last_seen'],
                        "active_connections": user_presence[user_id]['active_connections']
                    })
                )
            except Exception as e:
                print(f"Error publishing presence update for user {user_id}: {e}")
    
    async def update_heartbeat(self, user_id: int, client_id: str):
        """Update heartbeat for a specific connection"""
        if user_id in user_connections and client_id in user_connections[user_id]:
            user_connections[user_id][client_id]['last_heartbeat'] = datetime.now()
    
    async def update_visibility(self, user_id: int, client_id: str, is_visible: bool):
        """Update visibility for a specific connection"""
        if not self.redis:
            return
        
        if user_id in user_connections and client_id in user_connections[user_id]:
            user_connections[user_id][client_id]['is_visible'] = is_visible
            
            # If at least one connection is visible, user is considered online
            any_visible = any(
                conn['is_visible'] 
                for conn in user_connections[user_id].values()
            )
            
            status = 'online' if any_visible else 'away'
            user_presence[user_id] = {
                'status': status,
                'last_seen': datetime.now().isoformat(),
                'active_connections': len(user_connections[user_id])
            }
            
            # Publish presence update
            try:
                await self.redis.publish(
                    "presence_global",
                    json.dumps({
                        "type": "presence",
                        "user_id": user_id,
                        "status": status,
                        "last_seen": user_presence[user_id]['last_seen'],
                        "active_connections": user_presence[user_id]['active_connections']
                    })
                )
            except Exception as e:
                print(f"Error publishing visibility update for user {user_id}: {e}")
    
    async def get_user_presence(self, user_id: int) -> Dict[str, Any]:
        """Get current presence status for a user"""
        # Check in-memory first
        if user_id in user_presence:
            return user_presence[user_id]
        
        # Fallback to Redis if not in memory
        if self.redis:
            try:
                online = await self.redis.get(f"online:{user_id}")
                if online:
                    return {
                        'status': 'online',
                        'last_seen': datetime.now().isoformat(),
                        'active_connections': 1
                    }
                else:
                    last_seen_str = await self.redis.get(f"last_seen:{user_id}")
                    return {
                        'status': 'offline',
                        'last_seen': last_seen_str or datetime.now().isoformat(),
                        'active_connections': 0
                    }
            except Exception as e:
                print(f"Error getting presence from Redis for user {user_id}: {e}")
        
        return {
            'status': 'offline',
            'last_seen': datetime.now().isoformat(),
            'active_connections': 0
        }
    
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
                        await self.remove_connection(user_id, client_id)

