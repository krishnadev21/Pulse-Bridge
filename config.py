import redis.asyncio as aioredis

REDIS_URL = "redis://localhost:6379/0"

# Private variables
_redis_instance = None
_redis_initialized = False

async def init_redis():
    """Initialize Redis (call this once at startup)"""
    global _redis_instance, _redis_initialized
    if not _redis_initialized:
        _redis_instance = await aioredis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_keepalive=True
        )
        await _redis_instance.ping()  # Test connection
        _redis_initialized = True
        print(f"✅ Redis initialized (id: {id(_redis_instance)})")
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