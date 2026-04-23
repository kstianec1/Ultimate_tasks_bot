"""
API server (aiohttp) — serves the Mini App and REST API.
Runs alongside the bot in the same process.
"""

import os
import json
import hashlib
import hmac
import logging
from urllib.parse import parse_qs, unquote

from aiohttp import web
import database as db

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_DIR = os.path.join(os.path.dirname(__file__), "webapp")


# ── Telegram WebApp Auth Validation ──

def validate_init_data(init_data: str) -> dict | None:
    """
    Validate Telegram WebApp initData.
    Returns parsed user data if valid, None otherwise.
    https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
    """
    try:
        parsed = parse_qs(init_data)
        check_hash = parsed.get("hash", [None])[0]
        if not check_hash:
            return None

        # Build data-check-string
        data_pairs = []
        for key, values in parsed.items():
            if key == "hash":
                continue
            data_pairs.append(f"{key}={unquote(values[0])}")
        data_pairs.sort()
        data_check_string = "\n".join(data_pairs)

        # Compute secret key
        secret_key = hmac.new(
            b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256
        ).digest()

        # Compute hash
        computed_hash = hmac.new(
            secret_key, data_check_string.encode(), hashlib.sha256
        ).hexdigest()

        if computed_hash != check_hash:
            return None

        # Parse user
        user_data = parsed.get("user", [None])[0]
        if user_data:
            return json.loads(unquote(user_data))
        return None
    except Exception as e:
        logger.error(f"Auth validation error: {e}")
        return None


async def auth_middleware(request, handler):
    """Extract and validate Telegram user from Authorization header."""
    # Static files don't need auth
    if request.path.startswith("/webapp") or request.path == "/health":
        return await handler(request)

    init_data = request.headers.get("Authorization", "")
    if init_data.startswith("tma "):
        init_data = init_data[4:]

    tg_user = validate_init_data(init_data)
    if not tg_user:
        return web.json_response({"error": "Unauthorized"}, status=401)

    # Get or create user in DB
    user = await db.get_or_create_user(
        telegram_id=tg_user["id"],
        username=tg_user.get("username"),
        first_name=tg_user.get("first_name"),
        last_name=tg_user.get("last_name"),
    )
    request["user"] = user
    return await handler(request)


# ── API Routes ──

routes = web.RouteTableDef()


@routes.get("/health")
async def health(request):
    return web.json_response({"status": "ok"})


# ── Teams ──

@routes.get("/api/teams")
async def get_teams(request):
    user = request["user"]
    teams = await db.get_user_teams(user["id"])
    return web.json_response(teams)


@routes.post("/api/teams")
async def create_team(request):
    user = request["user"]
    data = await request.json()
    name = data.get("name", "").strip()
    if not name:
        return web.json_response({"error": "Name required"}, status=400)
    team = await db.create_team(name, user["id"])
    return web.json_response(team, status=201)


@routes.get("/api/teams/{team_id}/members")
async def get_members(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    members = await db.get_team_members(team_id)
    return web.json_response(members)


# ── Tasks ──

@routes.get("/api/teams/{team_id}/tasks")
async def get_tasks(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)

    status = request.query.get("status")
    priority = request.query.get("priority")
    assignee_id = request.query.get("assignee_id")
    if assignee_id:
        assignee_id = int(assignee_id)

    tasks = await db.get_team_tasks(team_id, status=status, priority=priority, assignee_id=assignee_id)
    return web.json_response(tasks)


@routes.post("/api/teams/{team_id}/tasks")
async def create_task(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)

    data = await request.json()
    title = data.get("title", "").strip()
    if not title:
        return web.json_response({"error": "Title required"}, status=400)

    task = await db.create_task(
        team_id=team_id,
        title=title,
        creator_id=user["id"],
        description=data.get("description", ""),
        status=data.get("status", "todo"),
        priority=data.get("priority", "medium"),
        assignee_id=data.get("assignee_id"),
        deadline=data.get("deadline"),
    )

    # Send notification if assigned to someone else
    if task.get("assignee_id") and task["assignee_id"] != user["id"]:
        from bot import notify_task_assigned
        await notify_task_assigned(task, user.get("first_name", "Someone"))

    return web.json_response(task, status=201)


@routes.patch("/api/tasks/{task_id}")
async def update_task(request):
    user = request["user"]
    task_id = int(request.match_info["task_id"])

    task = await db.get_task(task_id)
    if not task:
        return web.json_response({"error": "Not found"}, status=404)
    if not await db.is_team_member(task["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)

    data = await request.json()
    old_status = task["status"]
    old_assignee = task.get("assignee_id")

    updated = await db.update_task(task_id, **data)

    # Notify on status change
    if data.get("status") and data["status"] != old_status and updated.get("assignee_telegram_id"):
        from bot import notify_status_changed
        await notify_status_changed(updated, user.get("first_name", "Someone"), old_status)

    # Notify on new assignment
    if data.get("assignee_id") and data["assignee_id"] != old_assignee and data["assignee_id"] != user["id"]:
        from bot import notify_task_assigned
        await notify_task_assigned(updated, user.get("first_name", "Someone"))

    return web.json_response(updated)


@routes.delete("/api/tasks/{task_id}")
async def delete_task(request):
    user = request["user"]
    task_id = int(request.match_info["task_id"])

    task = await db.get_task(task_id)
    if not task:
        return web.json_response({"error": "Not found"}, status=404)
    if not await db.is_team_member(task["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)

    await db.delete_task(task_id)
    return web.json_response({"ok": True})


# ── Stats ──

@routes.get("/api/teams/{team_id}/stats")
async def get_stats(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)

    stats = await db.get_team_stats(team_id)
    return web.json_response(stats)


# ── Notifications ──

@routes.get("/api/notifications")
async def get_notifications(request):
    user = request["user"]
    notifs = await db.get_unread_notifications(user["id"])
    return web.json_response(notifs)


@routes.post("/api/notifications/read")
async def mark_read(request):
    user = request["user"]
    await db.mark_notifications_read(user["id"])
    return web.json_response({"ok": True})


# ── Static files (Mini App) ──

@routes.get("/webapp")
@routes.get("/webapp/")
async def serve_webapp(request):
    return web.FileResponse(os.path.join(WEBAPP_DIR, "index.html"))


# ── App factory ──

def create_app() -> web.Application:
    app = web.Application(middlewares=[auth_middleware])
    app.router.add_routes(routes)
    # Serve static webapp files
    app.router.add_static("/webapp/static/", os.path.join(WEBAPP_DIR, "static"), show_index=False)
    return app


def run_server(host="0.0.0.0", port=8080):
    app = create_app()
    web.run_app(app, host=host, port=port)
