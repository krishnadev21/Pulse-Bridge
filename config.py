import asyncio
import redis.asyncio as aioredis

REDIS_URL = "redis://localhost:6379/0"

# Private variables
_redis_instance = None
_redis_initialized = False

import redis.asyncio as aioredis

_redis_instance = None

async def init_redis():
    global _redis_instance
    if _redis_instance is None:
        _redis_instance = aioredis.from_url(
            "redis://localhost:6379/0",
            decode_responses=True,
            socket_keepalive=True,
            max_connections=50,   # 🔥 2x expected concurrent users
        )

        # 🔥 Warm ALL connections
        await asyncio.gather(
            *[_redis_instance.ping() for _ in range(5)]
        )

        print(f"✅ Redis initialized with pool (id: {id(_redis_instance)})")

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