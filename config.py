import asyncio
import redis.asyncio as aioredis

# Instead of aioredis, use redis.asyncio (newer)
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

REDIS_URL = "redis://localhost:6379/0"

# Private variables
_redis_instance = None
_redis_initialized = False

import redis.asyncio as aioredis

_redis_instance = None


async def init_redis():
    global _redis_instance
    if _redis_instance is None:
        pool = ConnectionPool.from_url(
            "redis://localhost:6379/0",
            decode_responses=True,
            max_connections=50,
            socket_keepalive=True,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        
        _redis_instance = Redis(connection_pool=pool)
        
        # Warm up
        await _redis_instance.ping()
        
        # Create minimum connections
        for i in range(5):
            await _redis_instance.set(f"warmup:{i}", "true", ex=1)
        
        print(f"✅ Redis initialized with pool (id: {id(pool)})")
        print(f"📊 Pool: max={pool.max_connections}")
    
    return _redis_instance

def get_redis():
    """
    Get Redis connection - SYNCHRONOUS
    Returns None if not initialized
    """
    if not _redis_initialized:
        print("❌ Redis not initialized. Call init_redis() first.")
    return _redis_instance

async def close_redis():
    """Clean shutdown"""
    global _redis_instance, _redis_initialized
    if _redis_instance:
        await _redis_instance.close()
        _redis_instance = None
        _redis_initialized = False
        print("✅ Redis closed")