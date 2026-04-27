"""
Telegram Task Tracker Bot — commands, FSM dialogs, notifications.
Uses aiogram 3.x
"""

import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, MenuButtonWebApp, ReplyKeyboardMarkup,
    KeyboardButton, ReplyKeyboardRemove
)
from aiogram.enums import ParseMode
from aiogram.utils.markdown import hbold, hcode

import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# ── FSM States ──

class TaskCreation(StatesGroup):
    waiting_title = State()
    waiting_assignee = State()
    waiting_deadline = State()
    waiting_tag = State()
    waiting_priority = State()


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


def _fmt_deadline(deadline: str) -> str:
    """Format deadline string for display."""
    if not deadline:
        return "—"
    try:
        from datetime import datetime
        dt = datetime.strptime(deadline, '%Y-%m-%d %H:%M')
        return dt.strftime('%d.%m.%Y %H:%M')
    except Exception:
        return deadline


def format_task(t: dict) -> str:
    emoji_p = priority_emoji(t['priority'])
    emoji_s = status_emoji(t['status'])
    assignee = t.get('assignee_name') or 'Без исполнителя'
    deadline = _fmt_deadline(t.get('deadline'))
    tag = f" 🏷{t['tag_name']}" if t.get('tag_name') else ""
    return (
        f"{emoji_s} {hbold(t['title'])}{tag}\n"
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
                f"🎉 Ты присоединился к команде {hbold(team['name'])}!\n\n"
                f"Используй /mytasks чтобы увидеть свои задачи или открой полный трекер ниже.",
                reply_markup=webapp_button()
            )
            return
        else:
            await message.answer("❌ Неверная ссылка-приглашение.")
            return

    # Check if user is in any team
    teams = await db.get_user_teams(user['id'])
    if not teams:
        await message.answer(
            f"👋 Привет, {hbold(user['first_name'] or 'there')}!\n\n"
            f"🔒 Этот бот работает только по приглашениям.\n\n"
            f"Попроси коллегу, который уже в команде, отправить тебе ссылку-приглашение "
            f"(команда /invite), или создай свою команду:\n\n"
            f"/newteam &lt;название команды&gt;\n\n"
            f"Например: /newteam Моя Команда"
        )
        return

    text = (
        f"👋 Привет, {hbold(user['first_name'] or 'there')}!\n\n"
        f"Я — твой командный таск-трекер. Вот что я умею:\n\n"
        f"🏢 {hbold('/newteam')} — Создать новую команду\n"
        f"   └ Используй, чтобы начать новый проект с нуля\n\n"
        f"👥 {hbold('/myteams')} — Список твоих команд\n"
        f"   └ Посмотри, в каких командах ты состоишь\n\n"
        f"📋 {hbold('/mytasks')} — Твои активные задачи\n"
        f"   └ Все задачи, назначенные на тебя\n\n"
        f"➕ {hbold('/newtask')} — Создать задачу через диалог\n"
        f"   └ Бот спросит название, исполнителя, дедлайн и тег\n\n"
        f"📊 {hbold('/stats')} — Аналитика команды\n"
        f"   └ Статистика по задачам и загрузке участников\n\n"
        f"📈 {hbold('/myefficiency')} — Твоя личная эффективность\n"
        f"   └ Сколько задач выполнено за неделю, история\n\n"
        f"🔗 {hbold('/invite')} — Получить ссылку-приглашение\n"
        f"   └ Пригласи коллег в свою команду\n\n"
        f"✅ {hbold('/done')} — Отметить задачу выполненной\n"
        f"   └ Используй: /done &lt;номер задачи&gt;\n\n"
        f"❌ {hbold('/cancel')} — Отменить текущее действие\n\n"
        f"Или открой полный трекер кнопкой ниже 👇"
    )
    await message.answer(text, reply_markup=webapp_button("🚀 Открыть Таск-Трекер"))


# ── /newteam ──

@router.message(Command("newteam"))
async def cmd_newteam(message: types.Message):
    user = await ensure_user(message)
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /newteam <название команды>\n\nПример: /newteam Мой Стартап")
        return

    team_name = args[1].strip()
    team = await db.create_team(team_name, user['id'])
    bot_info = await bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=join_{team['invite_code']}"

    await message.answer(
        f"✅ Команда {hbold(team['name'])} создана!\n\n"
        f"📎 Ссылка-приглашение:\n{hcode(invite_link)}\n\n"
        f"Отправь эту ссылку коллегам, чтобы они могли присоединиться."
    )


# ── /myteams ──

@router.message(Command("myteams"))
async def cmd_myteams(message: types.Message):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer(
            "Ты пока не состоишь ни в одной команде.\n\n"
            "Создай команду: /newteam &lt;название&gt;\n"
            "Или попроси коллегу прислать ссылку-приглашение."
        )
        return

    lines = [f"👥 {hbold('Твои команды')}:\n"]
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
        await message.answer("Ты не состоишь ни в одной команде. Создай: /newteam &lt;название&gt;")
        return

    bot_info = await bot.get_me()

    if len(teams) == 1:
        t = teams[0]
        invite_link = f"https://t.me/{bot_info.username}?start=join_{t['invite_code']}"
        await message.answer(
            f"📎 Ссылка-приглашение для {hbold(t['name'])}:\n{hcode(invite_link)}"
        )
    else:
        buttons = [
            [InlineKeyboardButton(text=t['name'], callback_data=f"invite_{t['id']}")]
            for t in teams
        ]
        await message.answer(
            "Для какой команды?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )


@router.callback_query(F.data.startswith("invite_"))
async def cb_invite(callback: types.CallbackQuery):
    team_id = int(callback.data.split("_")[1])
    user = await ensure_user(callback.from_user)

    teams = await db.get_user_teams(user['id'])
    team = next((t for t in teams if t['id'] == team_id), None)
    if not team:
        await callback.answer("Команда не найдена")
        return

    bot_info = await bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=join_{team['invite_code']}"
    await callback.message.edit_text(
        f"📎 Ссылка-приглашение для {hbold(team['name'])}:\n{hcode(invite_link)}"
    )
    await callback.answer()


# ── /help ──

@router.message(Command("help"))
async def cmd_help(message: types.Message):
    text = (
        f"📖 {hbold('Полная инструкция по таск-трекеру')}\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🚀 {hbold('Начало работы')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"1. Создай команду: /newteam Название\n"
        f"2. Получи ссылку: /invite\n"
        f"3. Отправь ссылку коллегам — они нажимают и автоматически попадают в команду\n"
        f"4. Открой трекер кнопкой внизу или через меню\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"➕ {hbold('Создание задач')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"{hbold('/newtask')} — запускает диалог:\n"
        f"  Бот спросит название → исполнителя → дедлайн → тег → приоритет\n\n"

        f"{hbold('/newtask Название @username')} — быстрое создание:\n"
        f"  Задача сразу назначается на человека с этим @username\n"
        f"  Пример: /newtask Сделать лендинг @ivan\n\n"

        f"{hbold('/newtask Название')} — создать без исполнителя:\n"
        f"  Потом назначишь через веб-трекер\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 {hbold('Управление задачами')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"{hbold('/mytasks')} — все задачи, назначенные на тебя\n\n"

        f"{hbold('/done 5')} — отметить задачу #5 как выполненную\n"
        f"  Номер задачи виден в /mytasks\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔔 {hbold('Уведомления')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"Бот автоматически пишет тебе:\n"
        f"  📌 Когда тебе назначили задачу\n"
        f"  🔄 Когда изменился статус твоей задачи\n"
        f"  🟡 За 24 часа до дедлайна\n"
        f"  🔴 За 1 час до дедлайна\n"
        f"  🔴 Когда задача просрочена\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 {hbold('Аналитика')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"{hbold('/stats')} — статистика команды:\n"
        f"  Задачи по статусам, загрузка каждого участника\n\n"

        f"{hbold('/myefficiency')} — твоя личная эффективность:\n"
        f"  Сколько задач назначено/выполнено за неделю\n"
        f"  История по неделям, среднее за 4 недели\n"
        f"  Видишь только ты — никто другой не видит\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 {hbold('Веб-трекер (мини-апп)')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"Открывается кнопкой внизу. Там:\n"
        f"  📌 {hbold('Kanban')} — доска с колонками, перетаскивай задачи\n"
        f"  📋 {hbold('Список')} — фильтры по статусу, тегу, исполнителю\n"
        f"  📊 {hbold('Команда')} — аналитика и загрузка команды\n"
        f"  👤 {hbold('Я')} — только твои задачи и твоя эффективность\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏷 {hbold('Теги проектов')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"Теги помогают группировать задачи по проектам.\n"
        f"Создавай теги при создании задачи или в веб-трекере.\n"
        f"В списке можно фильтровать по тегу — видишь только задачи нужного проекта.\n\n"

        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ {hbold('Все команды')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"/start — главное меню\n"
        f"/newteam &lt;название&gt; — создать команду\n"
        f"/myteams — мои команды\n"
        f"/invite — ссылка-приглашение\n"
        f"/newtask — создать задачу\n"
        f"/mytasks — мои задачи\n"
        f"/done &lt;id&gt; — отметить выполненной\n"
        f"/stats — аналитика команды\n"
        f"/myefficiency — моя эффективность\n"
        f"/cancel — отменить текущее действие\n"
        f"/help — эта справка"
    )
    await message.answer(text, reply_markup=webapp_button("🚀 Открыть трекер"))

@router.message(Command("newtask"))
async def cmd_newtask(message: types.Message, state: FSMContext):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer("Сначала присоединись к команде или создай свою: /newteam <название>")
        return

    args = message.text.split(maxsplit=1)
    raw_args = args[1].strip() if len(args) > 1 else ""

    # Quick syntax: /newtask Title @username
    import re
    mention_match = re.search(r'@(\w+)', raw_args)
    quick_title = re.sub(r'@\w+', '', raw_args).strip() if mention_match else ""

    if mention_match and quick_title:
        username = mention_match.group(1)
        if len(teams) > 1:
            await state.update_data(
                user_id=user['id'], teams=teams,
                quick_title=quick_title, quick_username=username
            )
            buttons = [[InlineKeyboardButton(text=t['name'], callback_data=f"quick_team_{t['id']}")]
                       for t in teams]
            await message.answer(
                f"В какую команду добавить задачу «{quick_title}»?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
            )
            return
        await _quick_create_task(message, user, teams[0]['id'], quick_title, username)
        return

    # Store teams in state
    await state.update_data(user_id=user['id'], teams=teams)

    if len(teams) == 1:
        await state.update_data(team_id=teams[0]['id'], team_name=teams[0]['name'])
    else:
        buttons = [[InlineKeyboardButton(text=t['name'], callback_data=f"fsm_team_{t['id']}")]
                   for t in teams]
        await message.answer(
            "В какую команду добавить задачу?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        await state.set_state(TaskCreation.waiting_title)
        return

    await message.answer(
        "📝 Напиши название задачи:\n\n"
        "Или /cancel чтобы отменить\n\n"
        "<i>Совет: можно сразу написать /newtask Название @username</i>",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(TaskCreation.waiting_title)


async def _quick_create_task(message, user, team_id, title, username):
    """Create task quickly with @username mention."""
    members = await db.get_team_members(team_id)
    found = next(
        (m for m in members if (m.get('username') or '').lower() == username.lower()),
        None
    )
    if not found:
        found = next(
            (m for m in members if username.lower() in (m.get('first_name') or '').lower()),
            None
        )
    if not found:
        names = ', '.join(
            f"@{m['username']}" if m.get('username') else m.get('first_name', '?')
            for m in members
        )
        await message.answer(
            f"❌ Пользователь @{username} не найден в команде.\n\n"
            f"Участники: {names}\n\n"
            f"Используй /newtask для диалога."
        )
        return

    task = await db.create_task(
        team_id=team_id,
        title=title,
        creator_id=user['id'],
        assignee_id=found['id'],
        priority='medium',
    )
    await db.record_task_assigned(found['id'], team_id)

    assignee_name = found.get('first_name') or f"@{found.get('username')}"
    await message.answer(
        f"✅ Задача создана!\n\n"
        f"📋 {hbold(task['title'])}\n"
        f"👤 Исполнитель: {assignee_name}\n"
        f"🟡 Приоритет: medium\n\n"
        f"Открой трекер чтобы добавить дедлайн и тег:",
        reply_markup=webapp_button("🚀 Открыть трекер")
    )
    if found['id'] != user['id']:
        await notify_task_assigned(task, user.get('first_name') or 'Кто-то')


@router.callback_query(F.data.startswith("quick_team_"))
async def cb_quick_team(callback: types.CallbackQuery, state: FSMContext):
    team_id = int(callback.data.split("_")[2])
    data = await state.get_data()
    user = await ensure_user(callback.from_user)
    await state.clear()
    await _quick_create_task(
        callback.message, user, team_id,
        data['quick_title'], data['quick_username']
    )
    await callback.answer()


@router.callback_query(F.data.startswith("fsm_team_"), TaskCreation.waiting_title)
async def fsm_select_team(callback: types.CallbackQuery, state: FSMContext):
    team_id = int(callback.data.split("_")[2])
    data = await state.get_data()
    teams = data.get('teams', [])
    team = next((t for t in teams if t['id'] == team_id), None)
    if not team:
        await callback.answer("Команда не найдена")
        return
    await state.update_data(team_id=team_id, team_name=team['name'])
    await callback.message.edit_text(
        f"Команда: {hbold(team['name'])}\n\n"
        f"📝 Напиши название задачи:\n\n"
        f"Или /cancel чтобы отменить"
    )
    await callback.answer()


@router.message(TaskCreation.waiting_title, F.text)
async def fsm_got_title(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return  # Let command handlers handle it
    title = message.text.strip()
    if len(title) < 2:
        await message.answer("Название слишком короткое. Попробуй ещё раз:")
        return

    await state.update_data(title=title)
    data = await state.get_data()
    team_id = data.get('team_id')

    # Get team members for assignee selection
    members = await db.get_team_members(team_id)
    await state.update_data(members=members)

    # Build member list text
    lines = [f"👤 Кому назначить задачу {hbold(title)}?\n"]
    for i, m in enumerate(members, 1):
        name = m.get('first_name') or m.get('username') or f"User {m['id']}"
        username = f" (@{m['username']})" if m.get('username') else ""
        lines.append(f"{i}. {name}{username}")
    lines.append("\nНапиши @username или имя участника.")
    lines.append("Или напиши «мне» чтобы назначить на себя.")
    lines.append("Или напиши «никому» чтобы оставить без исполнителя.")

    await message.answer("\n".join(lines))
    await state.set_state(TaskCreation.waiting_assignee)


@router.message(TaskCreation.waiting_assignee, F.text)
async def fsm_got_assignee(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return
    text = message.text.strip().lower()
    data = await state.get_data()
    members = data.get('members', [])
    user_id = data.get('user_id')

    assignee_id = None
    assignee_name = "Без исполнителя"

    if text in ('мне', 'me', 'себе', 'я'):
        assignee_id = user_id
        me = next((m for m in members if m['id'] == user_id), None)
        assignee_name = me.get('first_name') or 'Ты' if me else 'Ты'
    elif text in ('никому', 'none', 'нет', '-'):
        assignee_id = None
    else:
        # Search by username or first name
        query = text.lstrip('@')
        found = None
        for m in members:
            if (m.get('username') or '').lower() == query:
                found = m
                break
            if (m.get('first_name') or '').lower() == query:
                found = m
                break
        if not found:
            # Try partial match
            for m in members:
                if query in (m.get('first_name') or '').lower():
                    found = m
                    break
                if query in (m.get('username') or '').lower():
                    found = m
                    break
        if found:
            assignee_id = found['id']
            assignee_name = found.get('first_name') or found.get('username') or 'Участник'
        else:
            names = ', '.join(
                m.get('first_name') or m.get('username') or '?' for m in members
            )
            await message.answer(
                f"❌ Участник не найден.\n\n"
                f"Участники команды: {names}\n\n"
                f"Попробуй ещё раз или напиши «никому»:"
            )
            return

    await state.update_data(assignee_id=assignee_id, assignee_name=assignee_name)

    await message.answer(
        f"✅ Исполнитель: {hbold(assignee_name)}\n\n"
        f"⏰ Укажи дедлайн в формате:\n"
        f"<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        f"Например: <code>25.12.2025 18:00</code>\n\n"
        f"Или напиши «нет» чтобы пропустить дедлайн."
    )
    await state.set_state(TaskCreation.waiting_deadline)


@router.message(TaskCreation.waiting_deadline, F.text)
async def fsm_got_deadline(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return
    text = message.text.strip().lower()
    deadline = None

    if text not in ('нет', 'no', 'skip', '-', 'пропустить'):
        # Parse datetime: DD.MM.YYYY HH:MM or DD.MM.YYYY
        import re
        dt_match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2})', text)
        d_match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})$', text)
        if dt_match:
            d, mo, y, h, mi = dt_match.groups()
            try:
                from datetime import datetime
                dt = datetime(int(y), int(mo), int(d), int(h), int(mi))
                deadline = dt.strftime('%Y-%m-%d %H:%M')
            except ValueError:
                await message.answer(
                    "❌ Неверная дата. Попробуй ещё раз:\n"
                    "Формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
                    "Например: <code>25.12.2025 18:00</code>"
                )
                return
        elif d_match:
            d, mo, y = d_match.groups()
            try:
                from datetime import datetime
                dt = datetime(int(y), int(mo), int(d), 23, 59)
                deadline = dt.strftime('%Y-%m-%d %H:%M')
            except ValueError:
                await message.answer(
                    "❌ Неверная дата. Попробуй ещё раз:\n"
                    "Формат: <code>ДД.ММ.ГГГГ</code>"
                )
                return
        else:
            await message.answer(
                "❌ Не понял формат. Попробуй:\n"
                "<code>25.12.2025 18:00</code> или <code>25.12.2025</code>\n"
                "Или напиши «нет» чтобы пропустить."
            )
            return

    await state.update_data(deadline=deadline)

    # Ask for tag
    data = await state.get_data()
    team_id = data.get('team_id')
    tags = await db.get_team_tags(team_id)

    if tags:
        lines = [f"🏷 Выбери тег проекта или напиши новый:\n"]
        for t in tags:
            lines.append(f"• {t['name']}")
        lines.append("\nИли напиши «нет» чтобы пропустить.")
        await message.answer("\n".join(lines))
    else:
        await message.answer(
            "🏷 Напиши тег проекта (например: <code>Backend</code>, <code>Design</code>)\n\n"
            "Или напиши «нет» чтобы пропустить."
        )
    await state.set_state(TaskCreation.waiting_tag)


@router.message(TaskCreation.waiting_tag, F.text)
async def fsm_got_tag(message: types.Message, state: FSMContext):
    if message.text.startswith('/'):
        return
    text = message.text.strip()
    data = await state.get_data()
    team_id = data.get('team_id')
    tag_id = None

    if text.lower() not in ('нет', 'no', 'skip', '-', 'пропустить'):
        tag = await db.get_or_create_tag(team_id, text)
        tag_id = tag['id']

    await state.update_data(tag_id=tag_id)

    # Ask for priority
    buttons = [
        [InlineKeyboardButton(text="🔴 Высокий", callback_data="fsm_priority_high")],
        [InlineKeyboardButton(text="🟡 Средний", callback_data="fsm_priority_medium")],
        [InlineKeyboardButton(text="🔵 Низкий", callback_data="fsm_priority_low")],
    ]
    await message.answer(
        "⚡ Выбери приоритет задачи:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await state.set_state(TaskCreation.waiting_priority)


@router.callback_query(F.data.startswith("fsm_priority_"), TaskCreation.waiting_priority)
async def fsm_got_priority(callback: types.CallbackQuery, state: FSMContext):
    priority = callback.data.split("_")[2]
    data = await state.get_data()

    # Create the task
    task = await db.create_task(
        team_id=data['team_id'],
        title=data['title'],
        creator_id=data['user_id'],
        assignee_id=data.get('assignee_id'),
        deadline=data.get('deadline'),
        tag_id=data.get('tag_id'),
        priority=priority,
    )

    # Record weekly stat
    if data.get('assignee_id'):
        await db.record_task_assigned(data['assignee_id'], data['team_id'])

    # Build confirmation
    p_emoji = priority_emoji(priority)
    dl_text = f"\n⏰ Дедлайн: {_fmt_deadline(data.get('deadline'))}" if data.get('deadline') else ""
    tag_text = ""
    if data.get('tag_id'):
        tags = await db.get_team_tags(data['team_id'])
        tag = next((t for t in tags if t['id'] == data['tag_id']), None)
        if tag:
            tag_text = f"\n🏷 Тег: {tag['name']}"

    await callback.message.edit_text(
        f"✅ Задача создана!\n\n"
        f"📋 {hbold(task['title'])}\n"
        f"👤 Исполнитель: {data.get('assignee_name', 'Без исполнителя')}\n"
        f"{p_emoji} Приоритет: {priority}"
        f"{dl_text}{tag_text}\n\n"
        f"Открой трекер для деталей:",
        reply_markup=webapp_button("🚀 Открыть трекер")
    )

    # Notify assignee
    if data.get('assignee_id') and data['assignee_id'] != data['user_id']:
        await notify_task_assigned(task, callback.from_user.first_name or 'Кто-то')

    await state.clear()
    await callback.answer()


@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current = await state.get_state()
    if current:
        await state.clear()
        await message.answer("❌ Создание задачи отменено.", reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer("Нечего отменять.")


@router.callback_query(F.data.startswith("selectteam_"))
async def cb_selectteam(callback: types.CallbackQuery):
    """Legacy — kept for compatibility."""
    await callback.answer()


@router.callback_query(F.data.startswith("assign_"))
async def cb_assign(callback: types.CallbackQuery):
    """Legacy — kept for compatibility."""
    await callback.answer()


@router.callback_query(F.data.startswith("createin_"))
async def cb_createin(callback: types.CallbackQuery):
    """Legacy — kept for compatibility."""
    await callback.answer()


# ── /mytasks ──

@router.message(Command("mytasks"))
async def cmd_mytasks(message: types.Message):
    user = await ensure_user(message)
    tasks = await db.get_my_tasks(user['id'])

    if not tasks:
        await message.answer("🎉 У тебя нет активных задач!")
        return

    lines = [f"📋 {hbold('Твои активные задачи')}:\n"]
    for t in tasks:
        emoji_p = priority_emoji(t['priority'])
        emoji_s = status_emoji(t['status'])
        dl = f" • ⏰ {_fmt_deadline(t['deadline'])}" if t.get('deadline') else ""
        tag = f" 🏷{t['tag_name']}" if t.get('tag_name') else ""
        lines.append(f"{emoji_s} {t['title']}{tag} {emoji_p}{dl}")
        lines.append(f"   └ {t.get('team_name', '')} #{t['id']}")

    lines.append(f"\nВсего: {len(tasks)} задач")
    await message.answer("\n".join(lines), reply_markup=webapp_button())


# ── /done ──

@router.message(Command("done"))
async def cmd_done(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.answer("Использование: /done <номер задачи>\n\nПример: /done 5")
        return

    task_id = int(args[1].strip())
    user = await ensure_user(message)
    task = await db.get_task(task_id)

    if not task:
        await message.answer("Задача не найдена.")
        return

    if not await db.is_team_member(task['team_id'], user['id']):
        await message.answer("Ты не состоишь в команде этой задачи.")
        return

    await db.update_task(task_id, status='done')
    await message.answer(f"✅ Выполнено: {hbold(task['title'])}")


# ── /stats ──

@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    user = await ensure_user(message)
    teams = await db.get_user_teams(user['id'])

    if not teams:
        await message.answer("Сначала присоединись к команде.")
        return

    team = teams[0]  # Default to first team
    stats = await db.get_team_stats(team['id'])

    by_status = stats['by_status']
    total = stats['total']

    text = (
        f"📊 {hbold(team['name'])} — Аналитика\n\n"
        f"Всего задач: {hbold(str(total))}\n"
        f"Выполнено: {hbold(str(stats['completion_rate']) + '%')}\n"
        f"Просрочено: {'🔴 ' + str(stats['overdue']) if stats['overdue'] else '✅ 0'}\n\n"
        f"{hbold('По статусам:')}\n"
        f"  📋 Backlog: {by_status.get('backlog', 0)}\n"
        f"  📌 To Do: {by_status.get('todo', 0)}\n"
        f"  🔧 В работе: {by_status.get('in-progress', 0)}\n"
        f"  👀 Ревью: {by_status.get('review', 0)}\n"
        f"  ✅ Готово: {by_status.get('done', 0)}\n\n"
        f"{hbold('Загрузка команды:')}\n"
    )
    for m in stats['members']:
        bar_len = min(m['total'], 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        text += f"  {m['first_name'] or '?'}: [{bar}] {m['done']}/{m['total']}\n"

    text += "\nПолная аналитика в трекере:"
    await message.answer(text, reply_markup=webapp_button())


# ── /myefficiency ──

@router.message(Command("myefficiency"))
async def cmd_myefficiency(message: types.Message):
    user = await ensure_user(message)
    eff = await db.get_my_efficiency(user['id'])

    rate = eff['completion_rate']
    rate_bar_len = min(int(rate / 10), 10)
    rate_bar = "█" * rate_bar_len + "░" * (10 - rate_bar_len)

    # Weekly history chart
    history = eff.get('history', [])
    history_text = ""
    if history:
        history_text = f"\n\n{hbold('История по неделям')} (последние {len(history)}):\n"
        for w in reversed(history):
            from datetime import date
            try:
                d = date.fromisoformat(w['week_start'])
                week_label = d.strftime('%d.%m')
            except Exception:
                week_label = w['week_start']
            completed = w.get('completed') or 0
            assigned = w.get('assigned') or 0
            bar = "█" * min(completed, 10)
            history_text += f"  {week_label}: {bar} {completed}/{assigned}\n"

    text = (
        f"📈 {hbold('Твоя эффективность')}\n\n"
        f"📅 Эта неделя:\n"
        f"  Назначено: {eff['assigned_this_week']}\n"
        f"  Выполнено: {eff['completed_this_week']}\n"
        f"  Просрочено: {eff['overdue_this_week']}\n"
        f"  Процент: [{rate_bar}] {rate}%\n\n"
        f"📊 Среднее за 4 недели: {eff['avg_rate_4w']}%\n\n"
        f"🏆 Всего выполнено за всё время: {eff['total_completed_alltime']}\n"
        f"⚡ Активных задач сейчас: {eff['active_tasks']}"
        f"{history_text}"
    )
    await message.answer(text, reply_markup=webapp_button("📊 Открыть трекер"))


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
        deadline_text = f"\n⏰ Дедлайн: {task['deadline']}" if task.get('deadline') else ""
        await send_notification(
            task['assignee_telegram_id'],
            f"📌 {hbold(assigner_name)} назначил(а) на тебя задачу:\n\n"
            f"{hbold(task['title'])}\n"
            f"Приоритет: {priority_emoji(task['priority'])} {task['priority']}"
            f"{deadline_text}",
            task['id']
        )


async def notify_status_changed(task: dict, changer_name: str, old_status: str):
    """Notify assignee when task status changes."""
    if task.get('assignee_telegram_id'):
        await send_notification(
            task['assignee_telegram_id'],
            f"🔄 {hbold(changer_name)} изменил(а) статус твоей задачи:\n\n"
            f"{hbold(task['title'])}\n"
            f"{status_emoji(old_status)} {old_status} → {status_emoji(task['status'])} {task['status']}",
            task['id']
        )


async def check_deadlines():
    """Background task: check for approaching deadlines and notify."""
    last_digest_date = None  # Track last morning digest date

    while True:
        try:
            from datetime import datetime, timezone, timedelta

            # Moscow timezone (UTC+3)
            MSK = timezone(timedelta(hours=3))
            now_msk = datetime.now(MSK)
            hour_msk = now_msk.hour
            today_msk = now_msk.date()

            # ── Spawn recurring tasks (check daily at 00:05 MSK) ──
            if hour_msk == 0:
                try:
                    created_ids = await db.spawn_recurring_tasks()
                    if created_ids:
                        logger.info(f"Spawned {len(created_ids)} recurring tasks")
                except Exception as e:
                    logger.error(f"Recurring task spawn error: {e}")

            # ── Morning digest at 10:00 MSK ──
            if hour_msk == 10 and last_digest_date != today_msk:
                last_digest_date = today_msk
                await _send_morning_digest()

            # ── Quiet hours: no deadline notifications 23:00–10:00 MSK ──
            if hour_msk >= 23 or hour_msk < 10:
                await asyncio.sleep(1800)
                continue

            db_conn = await db.get_db()
            try:
                # Tasks overdue (notify once)
                cursor = await db_conn.execute("""
                    SELECT t.*, u.telegram_id as assignee_telegram_id
                    FROM tasks t
                    LEFT JOIN users u ON t.assignee_id = u.id
                    WHERE t.deadline < datetime('now')
                    AND t.status != 'done'
                    AND t.notified_overdue = 0
                    AND u.telegram_id IS NOT NULL
                """)
                overdue = await cursor.fetchall()

                for task in overdue:
                    task_dict = dict(task)
                    await send_notification(
                        task_dict['assignee_telegram_id'],
                        f"🔴 {hbold('Задача просрочена, братан!')}:\n\n"
                        f"📋 {hbold(task_dict['title'])}\n"
                        f"⏰ Дедлайн был: {_fmt_deadline(task_dict['deadline'])}\n"
                        f"Приоритет: {priority_emoji(task_dict['priority'])} {task_dict['priority']}\n\n"
                        f"Давай, закрывай — или обнови дедлайн в трекере 👇",
                        task_dict['id']
                    )
                    await db_conn.execute(
                        "UPDATE tasks SET notified_overdue=1 WHERE id=?", (task_dict['id'],)
                    )

                # Tasks due in 24 hours (notify once)
                cursor = await db_conn.execute("""
                    SELECT t.*, u.telegram_id as assignee_telegram_id
                    FROM tasks t
                    LEFT JOIN users u ON t.assignee_id = u.id
                    WHERE t.deadline BETWEEN datetime('now') AND datetime('now', '+24 hours')
                    AND t.status != 'done'
                    AND t.notified_24h = 0
                    AND u.telegram_id IS NOT NULL
                """)
                due_24h = await cursor.fetchall()

                for task in due_24h:
                    task_dict = dict(task)
                    await send_notification(
                        task_dict['assignee_telegram_id'],
                        f"🟡 {hbold('У тебя осталось 24 часа, брат, не забыл?')}\n\n"
                        f"📋 {hbold(task_dict['title'])}\n"
                        f"⏰ Дедлайн: {_fmt_deadline(task_dict['deadline'])}\n"
                        f"Приоритет: {priority_emoji(task_dict['priority'])} {task_dict['priority']}",
                        task_dict['id']
                    )
                    await db_conn.execute(
                        "UPDATE tasks SET notified_24h=1 WHERE id=?", (task_dict['id'],)
                    )

                # Tasks due in 1 hour (notify once)
                cursor = await db_conn.execute("""
                    SELECT t.*, u.telegram_id as assignee_telegram_id
                    FROM tasks t
                    LEFT JOIN users u ON t.assignee_id = u.id
                    WHERE t.deadline BETWEEN datetime('now') AND datetime('now', '+1 hour')
                    AND t.status != 'done'
                    AND t.notified_1h = 0
                    AND u.telegram_id IS NOT NULL
                """)
                due_1h = await cursor.fetchall()

                for task in due_1h:
                    task_dict = dict(task)
                    await send_notification(
                        task_dict['assignee_telegram_id'],
                        f"🔴 {hbold('Братик, до дедлайна 1 час, пора работать.')}\n\n"
                        f"📋 {hbold(task_dict['title'])}\n"
                        f"⏰ Дедлайн: {_fmt_deadline(task_dict['deadline'])}\n"
                        f"Приоритет: {priority_emoji(task_dict['priority'])} {task_dict['priority']}",
                        task_dict['id']
                    )
                    await db_conn.execute(
                        "UPDATE tasks SET notified_1h=1 WHERE id=?", (task_dict['id'],)
                    )

                await db_conn.commit()

            finally:
                await db_conn.close()

        except Exception as e:
            logger.error(f"Deadline check error: {e}")

        # Check every 30 minutes
        await asyncio.sleep(1800)


async def _send_morning_digest():
    """Send morning task digest to all users at 10:00 MSK."""
    try:
        db_conn = await db.get_db()
        try:
            # Get all users who have active tasks
            cursor = await db_conn.execute("""
                SELECT DISTINCT u.telegram_id, u.first_name, u.id as user_id
                FROM users u
                JOIN tasks t ON t.assignee_id = u.id
                WHERE t.status != 'done'
                AND u.telegram_id IS NOT NULL
            """)
            users_with_tasks = await cursor.fetchall()

            for user_row in users_with_tasks:
                user_dict = dict(user_row)
                telegram_id = user_dict['telegram_id']
                first_name = user_dict['first_name'] or 'брат'
                user_id = user_dict['user_id']

                # Get their active tasks ordered by deadline
                cursor2 = await db_conn.execute("""
                    SELECT t.*, tg.name as tag_name
                    FROM tasks t
                    LEFT JOIN tags tg ON t.tag_id = tg.id
                    WHERE t.assignee_id = ? AND t.status != 'done'
                    ORDER BY
                        CASE WHEN t.deadline IS NULL THEN 1 ELSE 0 END,
                        t.deadline ASC,
                        CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
                    LIMIT 10
                """, (user_id,))
                tasks_list = await cursor2.fetchall()

                if not tasks_list:
                    continue

                lines = [f"☀️ {hbold(f'Доброе утро, {first_name}!')} Вот твои задачи на сегодня:\n"]

                overdue_count = 0
                for t in tasks_list:
                    td = dict(t)
                    p_emoji = priority_emoji(td['priority'])
                    s_emoji = status_emoji(td['status'])
                    tag = f" 🏷{td['tag_name']}" if td.get('tag_name') else ""

                    if td.get('deadline'):
                        from datetime import datetime, timezone, timedelta
                        MSK = timezone(timedelta(hours=3))
                        try:
                            dl_dt = datetime.strptime(td['deadline'], '%Y-%m-%d %H:%M')
                            now_msk = datetime.now(MSK).replace(tzinfo=None)
                            diff = dl_dt - now_msk
                            if diff.total_seconds() < 0:
                                dl_str = f"🔴 ПРОСРОЧЕНО ({_fmt_deadline(td['deadline'])})"
                                overdue_count += 1
                            elif diff.days == 0:
                                hours = int(diff.total_seconds() // 3600)
                                dl_str = f"🟠 сегодня в {dl_dt.strftime('%H:%M')} (через {hours}ч)"
                            elif diff.days == 1:
                                dl_str = f"🟡 завтра {dl_dt.strftime('%H:%M')}"
                            else:
                                dl_str = f"⏰ {_fmt_deadline(td['deadline'])}"
                        except Exception:
                            dl_str = f"⏰ {_fmt_deadline(td['deadline'])}"
                    else:
                        dl_str = "без дедлайна"

                    lines.append(f"{s_emoji} {hbold(td['title'])}{tag}")
                    lines.append(f"   {p_emoji} {dl_str}\n")

                if overdue_count:
                    lines.append(f"⚠️ Просрочено задач: {overdue_count} — разберись сегодня!")
                else:
                    lines.append("Удачного дня, давай! 💪")

                try:
                    await bot.send_message(telegram_id, "\n".join(lines),
                                           reply_markup=webapp_button("📋 Открыть трекер"))
                    await asyncio.sleep(0.05)  # Avoid Telegram rate limit
                except Exception as e:
                    logger.error(f"Morning digest error for {telegram_id}: {e}")

        finally:
            await db_conn.close()
    except Exception as e:
        logger.error(f"Morning digest global error: {e}")


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
        types.BotCommand(command="start", description="Начало работы / Помощь"),
        types.BotCommand(command="newtask", description="Создать задачу (диалог или @username)"),
        types.BotCommand(command="mytasks", description="Мои активные задачи"),
        types.BotCommand(command="done", description="Отметить задачу выполненной"),
        types.BotCommand(command="myefficiency", description="Моя эффективность за неделю"),
        types.BotCommand(command="stats", description="Аналитика команды"),
        types.BotCommand(command="myteams", description="Мои команды"),
        types.BotCommand(command="newteam", description="Создать команду"),
        types.BotCommand(command="invite", description="Получить ссылку-приглашение"),
        types.BotCommand(command="help", description="Подробная инструкция"),
        types.BotCommand(command="cancel", description="Отменить текущее действие"),
    ])

    logger.info("Bot started!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
