


# ---------------------------------------------------


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
    await redis.set(f"online:{user_id}", "1")

    # 1️⃣ FIRST: Start listening for messages
    listener_task = asyncio.create_task(
        presence_listener(websocket)  # 👂 Start listening FIRST
    )

    # 2️⃣ THEN: Announce you're online
    await redis.publish(
        "presence_global",
        json.dumps({
            "type": "presence",
            "user_id": user_id,
            "status": "online"  # 🔊 Announce SECOND
        })
    )

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        pass

    finally:
        listener_task.cancel()

        last_seen = datetime.utcnow().isoformat() + "Z"

        await redis.delete(f"online:{user_id}")
        await redis.set(f"last_seen:{user_id}", str(last_seen))


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
    try:
        # 1️⃣ Check online
        if await redis.exists(f"online:{user_id}"):
            return {"status": "online"}

        # 2️⃣ Get last seen
        last_seen = await redis.get(f"last_seen:{user_id}")

        if last_seen:
            if isinstance(last_seen, bytes):
                last_seen = last_seen.decode()

            return {
                "status": "offline",
                "last_seen": last_seen
            }

        # 3️⃣ Never seen
        return {
            "status": "offline",
            "last_seen": None
        }

    except Exception as e:
        return JsonResponse(
            status_code=500,
            content={"error": str(e)}
        )
    
@app.post("/users/presence")
async def get_users_presence(payload: dict = Body(...)):
    try:
        user_ids = payload.get("user_ids", [])
        result = {}

        for user_id in user_ids:
            # 🟢 Online
            if await redis.exists(f"online:{user_id}"):
                result[str(user_id)] = {"status": "online"}
                continue

            # 🕒 Last seen
            last_seen = await redis.get(f"last_seen:{user_id}")

            if last_seen:
                if isinstance(last_seen, bytes):
                    last_seen = last_seen.decode()

                result[str(user_id)] = {
                    "status": "offline",
                    "last_seen": last_seen
                }
            else:
                result[str(user_id)] = {
                    "status": "offline",
                    "last_seen": None
                }
                
        return result

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

