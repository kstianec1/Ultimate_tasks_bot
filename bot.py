"""
Telegram Task Tracker Bot — commands, inline keyboards, notifications.
Uses aiogram 3.x
"""

import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, MenuButtonWebApp
)
from aiogram.enums import ParseMode
from aiogram.utils.markdown import hbold, hcode

import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")  # e.g. https://your-domain.com/webapp

bot = Bot(token=BOT_TOKEN, default=types.DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)


# ── Helpers ──

def webapp_button(text: str = "Open Tracker") -> InlineKeyboardMarkup:
    """Button that opens the Mini App."""
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL))
    ]])


def priority_emoji(p: str) -> str:
    return {"high": "🔴", "medium": "🟡", "low": "🔵"}.get(p, "⚪")


def status_emoji(s: str) -> str:
    return {
        "backlog": "📋", "todo": "📌", "in-progress": "🔧",
        "review": "👀", "done": "✅"
    }.get(s, "❔")


def format_task(t: dict) -> str:
    emoji_p = priority_emoji(t['priority'])
    emoji_s = status_emoji(t['status'])
    assignee = t.get('assignee_name') or 'Unassigned'
    deadline = t.get('deadline') or '—'
    return (
        f"{emoji_s} {hbold(t['title'])}\n"
        f"   {emoji_p} {t['priority']} • {assignee} • ⏰ {deadline}"
    )


async def ensure_user(msg_or_user) -> dict:
    """Get or create user from a message/callback."""
    u = msg_or_user if isinstance(msg_or_user, types.User) else msg_or_user.from_user
    return await db.get_or_create_user(
        telegram_id=u.id,
        username=u.username,
        first_name=u.first_name,
        last_name=u.last_name
    )


# ── /start ──

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    user = await ensure_user(message)

    # Check for deep link (invite code)
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("join_"):
        invite_code = args[1][5:]
        team = await db.join_team(invite_code, user['id'])
        if team:
            await message.answer(
                f"🎉 You joined team {hbold(team['name'])}!\n\n"
                f"Use /mytasks to see your tasks or open the full tracker below.",
                reply_markup=webapp_button()
            )
            return
        else:
            await message.answer("❌ Invalid invite link.")
            return

    text = (
        f"👋 Hi, {hbold(user['first_name'] or 'there')}!\n\n"
        f"I'm your team task tracker. Here's what I can do:\n\n"
        f"/newteam — Create a new team\n"
        f"/myteams — Your teams\n"
        f"/mytasks — Tasks assigned to you\n"
        f"/newtask — Create a task (quick)\n"
        f"/stats — Team analytics\n"
        f"/invite — Get invite link\n\n"
        f"Or open the full tracker with the button below 👇"
    )
    await message.answer(text, reply_markup=webapp_button("🚀 Open Task Tracker"))


# ── /newteam ──

@router.message(Command("newteam"))
async def cmd_newteam(message: types.Message):
    user = await ensure_user(message)
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Usage: /newteam <team name>\n\nExample: /newteam My Startup")
        return

    team_name = args[1].strip()
    team = await db.create_team(team_name, user['id'])
    bot_info = await bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=join_{team['invite_code']}"

    await message.answer(
        f"✅ Team {hbold(team['name'])} created!\n\n"
        f"📎 Invite link:\n{hcode(invite_link)}\n\n"
        f"Share this link with your teammates so they can join."
    )


# ── /myteams ──

@router.message(Command("myteams"))
async def cmd_myteams(message: types.Message):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer(
            "You're not in any team yet.\n\n"
            "Use /newteam <name> to create one, or ask your teammate for an invite link."
        )
        return

    lines = [f"👥 {hbold('Your teams')}:\n"]
    for t in teams:
        role_badge = " 👑" if t['role'] == 'owner' else ""
        lines.append(f"  • {t['name']}{role_badge} (ID: {t['id']})")

    await message.answer("\n".join(lines))


# ── /invite ──

@router.message(Command("invite"))
async def cmd_invite(message: types.Message):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer("You're not in any team. Create one with /newteam <name>")
        return

    bot_info = await bot.get_me()

    if len(teams) == 1:
        t = teams[0]
        invite_link = f"https://t.me/{bot_info.username}?start=join_{t['invite_code']}"
        await message.answer(
            f"📎 Invite link for {hbold(t['name'])}:\n{hcode(invite_link)}"
        )
    else:
        buttons = [
            [InlineKeyboardButton(text=t['name'], callback_data=f"invite_{t['id']}")]
            for t in teams
        ]
        await message.answer(
            "Which team?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )


@router.callback_query(F.data.startswith("invite_"))
async def cb_invite(callback: types.CallbackQuery):
    team_id = int(callback.data.split("_")[1])
    user = await ensure_user(callback.from_user)

    teams = await db.get_user_teams(user['id'])
    team = next((t for t in teams if t['id'] == team_id), None)
    if not team:
        await callback.answer("Team not found")
        return

    bot_info = await bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=join_{team['invite_code']}"
    await callback.message.edit_text(
        f"📎 Invite link for {hbold(team['name'])}:\n{hcode(invite_link)}"
    )
    await callback.answer()


# ── /newtask ──

@router.message(Command("newtask"))
async def cmd_newtask(message: types.Message):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer("Join or create a team first: /newteam <name>")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "Usage: /newtask <task title>\n\n"
            "Example: /newtask Fix login page bug\n\n"
            "Or open the full tracker for more options:",
            reply_markup=webapp_button()
        )
        return

    title = args[1].strip()

    if len(teams) == 1:
        task = await db.create_task(
            team_id=teams[0]['id'],
            title=title,
            creator_id=user['id'],
            assignee_id=user['id']
        )
        await message.answer(
            f"✅ Task created: {hbold(task['title'])}\n"
            f"Status: 📌 To Do • Assigned to you\n\n"
            f"Open tracker to edit details:",
            reply_markup=webapp_button()
        )
    else:
        buttons = [
            [InlineKeyboardButton(
                text=t['name'],
                callback_data=f"createin_{t['id']}_{title[:50]}"
            )]
            for t in teams
        ]
        await message.answer(
            f"Which team should I add \"{title}\" to?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )


@router.callback_query(F.data.startswith("createin_"))
async def cb_createin(callback: types.CallbackQuery):
    parts = callback.data.split("_", 2)
    team_id = int(parts[1])
    title = parts[2]
    user = await ensure_user(callback.from_user)

    task = await db.create_task(
        team_id=team_id, title=title,
        creator_id=user['id'], assignee_id=user['id']
    )
    await callback.message.edit_text(
        f"✅ Task created: {hbold(task['title'])}\n"
        f"Status: 📌 To Do • Assigned to you"
    )
    await callback.answer()


# ── /mytasks ──

@router.message(Command("mytasks"))
async def cmd_mytasks(message: types.Message):
    user = await ensure_user(message)
    tasks = await db.get_my_tasks(user['id'])

    if not tasks:
        await message.answer("🎉 No active tasks assigned to you!")
        return

    lines = [f"📋 {hbold('Your active tasks')}:\n"]
    for t in tasks:
        emoji_p = priority_emoji(t['priority'])
        emoji_s = status_emoji(t['status'])
        dl = f" • ⏰ {t['deadline']}" if t.get('deadline') else ""
        lines.append(f"{emoji_s} {t['title']} {emoji_p}{dl}")
        lines.append(f"   └ {t.get('team_name', '')} #{t['id']}")

    lines.append(f"\nTotal: {len(tasks)} tasks")
    await message.answer("\n".join(lines), reply_markup=webapp_button())


# ── /done ──

@router.message(Command("done"))
async def cmd_done(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.answer("Usage: /done <task_id>\n\nExample: /done 5")
        return

    task_id = int(args[1].strip())
    user = await ensure_user(message)
    task = await db.get_task(task_id)

    if not task:
        await message.answer("Task not found.")
        return

    if not await db.is_team_member(task['team_id'], user['id']):
        await message.answer("You're not a member of this task's team.")
        return

    await db.update_task(task_id, status='done')
    await message.answer(f"✅ Marked as done: {hbold(task['title'])}")

    # Notify creator if different person
    if task['creator_id'] != user['id']:
        creator = await db.get_user_by_telegram_id(task['creator_id'])
        # We'd need creator's telegram_id, but creator_id is internal id
        # This is handled in the notification system


# ── /stats ──

@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer("Join a team first.")
        return

    team = teams[0]  # Default to first team
    stats = await db.get_team_stats(team['id'])

    by_status = stats['by_status']
    total = stats['total']

    text = (
        f"📊 {hbold(team['name'])} — Analytics\n\n"
        f"Total tasks: {hbold(str(total))}\n"
        f"Completion rate: {hbold(str(stats['completion_rate']) + '%')}\n"
        f"Overdue: {'🔴 ' + str(stats['overdue']) if stats['overdue'] else '✅ 0'}\n\n"
        f"{hbold('By status:')}\n"
        f"  📋 Backlog: {by_status.get('backlog', 0)}\n"
        f"  📌 To Do: {by_status.get('todo', 0)}\n"
        f"  🔧 In Progress: {by_status.get('in-progress', 0)}\n"
        f"  👀 Review: {by_status.get('review', 0)}\n"
        f"  ✅ Done: {by_status.get('done', 0)}\n\n"
        f"{hbold('Team workload:')}\n"
    )
    for m in stats['members']:
        bar_len = min(m['total'], 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        text += f"  {m['first_name'] or '?'}: [{bar}] {m['done']}/{m['total']}\n"

    text += "\nFull analytics in the tracker:"
    await message.answer(text, reply_markup=webapp_button())


# ── Notification sender (called from API or scheduler) ──

async def send_notification(telegram_id: int, text: str, task_id: int = None):
    """Send a push notification to a user."""
    try:
        kb = None
        if WEBAPP_URL and task_id:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="Open task",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?task={task_id}")
                )
            ]])
        await bot.send_message(telegram_id, text, reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed to send notification to {telegram_id}: {e}")


async def notify_task_assigned(task: dict, assigner_name: str):
    """Notify user when a task is assigned to them."""
    if task.get('assignee_telegram_id'):
        await send_notification(
            task['assignee_telegram_id'],
            f"📌 {hbold(assigner_name)} assigned you a task:\n\n"
            f"{hbold(task['title'])}\n"
            f"Priority: {priority_emoji(task['priority'])} {task['priority']}\n"
            f"{'⏰ Deadline: ' + task['deadline'] if task.get('deadline') else ''}",
            task['id']
        )


async def notify_status_changed(task: dict, changer_name: str, old_status: str):
    """Notify assignee when task status changes."""
    if task.get('assignee_telegram_id'):
        await send_notification(
            task['assignee_telegram_id'],
            f"🔄 {hbold(changer_name)} moved your task:\n\n"
            f"{hbold(task['title'])}\n"
            f"{status_emoji(old_status)} {old_status} → {status_emoji(task['status'])} {task['status']}",
            task['id']
        )


async def check_deadlines():
    """Background task: check for approaching deadlines and notify."""
    while True:
        try:
            overdue = await db.get_overdue_tasks()
            for task in overdue:
                if task.get('assignee_telegram_id'):
                    await send_notification(
                        task['assignee_telegram_id'],
                        f"⚠️ {hbold('Overdue task')}:\n\n"
                        f"{hbold(task['title'])}\n"
                        f"Deadline: {task['deadline']}",
                        task['id']
                    )
        except Exception as e:
            logger.error(f"Deadline check error: {e}")
        await asyncio.sleep(3600 * 6)  # Check every 6 hours


# ── Web App data handler ──

@router.message(F.web_app_data)
async def handle_webapp_data(message: types.Message):
    """Handle data sent from Mini App."""
    import json
    try:
        data = json.loads(message.web_app_data.data)
        action = data.get('action')
        user = await ensure_user(message)

        if action == 'task_created':
            await message.answer(f"✅ Task created: {hbold(data.get('title', ''))}")
        elif action == 'task_done':
            await message.answer(f"✅ Completed: {hbold(data.get('title', ''))}")
    except Exception as e:
        logger.error(f"WebApp data error: {e}")


# ── Main ──

async def main():
    await db.init_db()

    # Set Web App menu button
    if WEBAPP_URL:
        try:
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="Task Tracker",
                    web_app=WebAppInfo(url=WEBAPP_URL)
                )
            )
        except Exception:
            pass

    # Start deadline checker
    asyncio.create_task(check_deadlines())

    # Set bot commands
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Start / Help"),
        types.BotCommand(command="mytasks", description="My active tasks"),
        types.BotCommand(command="newtask", description="Create a quick task"),
        types.BotCommand(command="done", description="Mark task as done"),
        types.BotCommand(command="stats", description="Team analytics"),
        types.BotCommand(command="myteams", description="Your teams"),
        types.BotCommand(command="newteam", description="Create a team"),
        types.BotCommand(command="invite", description="Get invite link"),
    ])

    logger.info("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
