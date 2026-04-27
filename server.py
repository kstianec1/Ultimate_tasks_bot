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
        token = os.getenv("BOT_TOKEN", "")
        secret_key = hmac.new(
            b"WebAppData", token.encode(), hashlib.sha256
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


@web.middleware
async def auth_middleware(request, handler):
    """Extract and validate Telegram user from Authorization header."""
    # Static files don't need auth
    if request.path.startswith("/webapp") or request.path == "/health" or request.path == "/":
        return await handler(request)

    init_data = request.headers.get("Authorization", "")
    if init_data.startswith("tma "):
        init_data = init_data[4:]

    tg_user = validate_init_data(init_data)
    if not tg_user:
        logger.warning(f"Auth failed for path {request.path}, init_data length: {len(init_data)}")
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
    tag_id = request.query.get("tag_id")
    if assignee_id:
        assignee_id = int(assignee_id)
    if tag_id:
        tag_id = int(tag_id)

    tasks = await db.get_team_tasks(team_id, status=status, priority=priority,
                                    assignee_id=assignee_id, tag_id=tag_id)
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
        tag_id=data.get("tag_id"),
    )

    # Send notification if assigned to someone else
    if task.get("assignee_id") and task["assignee_id"] != user["id"]:
        from bot import notify_task_assigned
        await notify_task_assigned(task, user.get("first_name", "Someone"))
        await db.record_task_assigned(task["assignee_id"], team_id)

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

    updated = await db.update_task_with_log(task_id, user["id"], **data)

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


# ── Tags ──

@routes.get("/api/teams/{team_id}/tags")
async def get_tags(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    tags = await db.get_team_tags(team_id)
    return web.json_response(tags)


@routes.post("/api/teams/{team_id}/tags")
async def create_tag(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    data = await request.json()
    name = data.get("name", "").strip()
    if not name:
        return web.json_response({"error": "Name required"}, status=400)
    tag = await db.get_or_create_tag(team_id, name, data.get("color", "#6c5ce7"))
    return web.json_response(tag, status=201)


@routes.delete("/api/tags/{tag_id}")
async def delete_tag(request):
    user = request["user"]
    tag_id = int(request.match_info["tag_id"])
    # Find which team this tag belongs to
    db_conn = await db.get_db()
    try:
        cursor = await db_conn.execute("SELECT * FROM tags WHERE id=?", (tag_id,))
        tag = await cursor.fetchone()
    finally:
        await db_conn.close()
    if not tag:
        return web.json_response({"error": "Not found"}, status=404)
    tag = dict(tag)
    if not await db.is_team_member(tag["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    await db.delete_tag(tag_id, tag["team_id"])
    return web.json_response({"ok": True})


# ── Personal efficiency ──

@routes.get("/api/me/efficiency")
async def get_my_efficiency(request):
    user = request["user"]
    eff = await db.get_my_efficiency(user["id"])
    return web.json_response(eff)


@routes.get("/api/me/tasks")
async def get_my_tasks(request):
    user = request["user"]
    tasks = await db.get_my_tasks(user["id"])
    return web.json_response(tasks)


# ── Comments ──

@routes.get("/api/tasks/{task_id}/comments")
async def get_comments(request):
    user = request["user"]
    task_id = int(request.match_info["task_id"])
    task = await db.get_task(task_id)
    if not task:
        return web.json_response({"error": "Not found"}, status=404)
    if not await db.is_team_member(task["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    comments = await db.get_task_comments(task_id)
    return web.json_response(comments)


@routes.post("/api/tasks/{task_id}/comments")
async def add_comment(request):
    user = request["user"]
    task_id = int(request.match_info["task_id"])
    task = await db.get_task(task_id)
    if not task:
        return web.json_response({"error": "Not found"}, status=404)
    if not await db.is_team_member(task["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    data = await request.json()
    text = data.get("text", "").strip()
    if not text:
        return web.json_response({"error": "Text required"}, status=400)

    comment = await db.add_comment(task_id, user["id"], text)
    await db.log_activity(task_id, user["id"], "commented", new_value=text[:100])

    # Notify mentioned users (@username)
    import re
    mentions = re.findall(r'@(\w+)', text)
    for username in mentions:
        mentioned = await db.get_user_by_username(username)
        if mentioned and mentioned["id"] != user["id"] and mentioned.get("telegram_id"):
            from bot import send_notification
            from aiogram.utils.markdown import hbold
            await send_notification(
                mentioned["telegram_id"],
                f"💬 {hbold(user.get('first_name', 'Кто-то'))} упомянул тебя в комментарии:\n\n"
                f"📋 {hbold(task['title'])}\n\n"
                f"«{text[:200]}»",
                task_id
            )

    return web.json_response(comment, status=201)


@routes.delete("/api/comments/{comment_id}")
async def delete_comment(request):
    user = request["user"]
    comment_id = int(request.match_info["comment_id"])
    deleted = await db.delete_comment(comment_id, user["id"])
    if not deleted:
        return web.json_response({"error": "Not found or not yours"}, status=404)
    return web.json_response({"ok": True})


# ── Activity Log ──

@routes.get("/api/tasks/{task_id}/activity")
async def get_activity(request):
    user = request["user"]
    task_id = int(request.match_info["task_id"])
    task = await db.get_task(task_id)
    if not task:
        return web.json_response({"error": "Not found"}, status=404)
    if not await db.is_team_member(task["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    activity = await db.get_task_activity(task_id)
    return web.json_response(activity)


# ── Recurring Tasks ──

@routes.get("/api/teams/{team_id}/recurring")
async def get_recurring(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    recurring = await db.get_team_recurring_tasks(team_id)
    return web.json_response(recurring)


@routes.post("/api/teams/{team_id}/recurring")
async def create_recurring(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    data = await request.json()
    title = data.get("title", "").strip()
    if not title:
        return web.json_response({"error": "Title required"}, status=400)
    recurrence = data.get("recurrence", "weekly")
    if recurrence not in ("daily", "weekly", "monthly"):
        return web.json_response({"error": "Invalid recurrence"}, status=400)
    r = await db.create_recurring_task(
        team_id=team_id,
        title=title,
        description=data.get("description", ""),
        priority=data.get("priority", "medium"),
        assignee_id=data.get("assignee_id"),
        tag_id=data.get("tag_id"),
        recurrence=recurrence,
        recurrence_day=data.get("recurrence_day", 0),
        estimated_hours=data.get("estimated_hours", 0),
        created_by=user["id"],
    )
    return web.json_response(r, status=201)


@routes.delete("/api/recurring/{recurring_id}")
async def delete_recurring(request):
    user = request["user"]
    recurring_id = int(request.match_info["recurring_id"])
    # Find team
    db_conn = await db.get_db()
    try:
        cursor = await db_conn.execute("SELECT * FROM recurring_tasks WHERE id=?", (recurring_id,))
        r = await cursor.fetchone()
    finally:
        await db_conn.close()
    if not r:
        return web.json_response({"error": "Not found"}, status=404)
    r = dict(r)
    if not await db.is_team_member(r["team_id"], user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    await db.delete_recurring_task(recurring_id, r["team_id"])
    return web.json_response({"ok": True})


# ── Burndown & Analytics ──

@routes.get("/api/teams/{team_id}/burndown")
async def get_burndown(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    days = int(request.query.get("days", 14))
    data = await db.get_burndown_data(team_id, days)
    return web.json_response(data)


@routes.get("/api/teams/{team_id}/time-in-status")
async def get_time_in_status(request):
    user = request["user"]
    team_id = int(request.match_info["team_id"])
    if not await db.is_team_member(team_id, user["id"]):
        return web.json_response({"error": "Forbidden"}, status=403)
    data = await db.get_time_in_status(team_id)
    return web.json_response(data)

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

@routes.get("/")
@routes.get("/webapp")
@routes.get("/webapp/")
async def serve_webapp(request):
    return web.FileResponse(os.path.join(WEBAPP_DIR, "index.html"))


# ── App factory ──

def create_app() -> web.Application:
    app = web.Application(middlewares=[auth_middleware])
    app.router.add_routes(routes)
    # Serve static webapp files only if directory exists
    static_dir = os.path.join(WEBAPP_DIR, "static")
    if os.path.isdir(static_dir):
        app.router.add_static("/webapp/static/", static_dir, show_index=False)
    return app


def run_server(host="0.0.0.0", port=8080):
    app = create_app()
    web.run_app(app, host=host, port=port)
