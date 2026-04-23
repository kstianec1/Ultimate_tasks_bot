"""
Main entry point — runs both the Telegram bot and the API server concurrently.
"""

import os
import asyncio
import logging
from dotenv import load_dotenv

load_dotenv()

from aiohttp import web
import database as db
from server import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))


async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set! Add it to .env file.")
        return

    # Init database
    await db.init_db()
    logger.info("Database initialized")

    # Import bot module (needs BOT_TOKEN to be set)
    from bot import dp, bot, check_deadlines
    from aiogram.types import MenuButtonWebApp, WebAppInfo

    # Set Web App menu button
    if WEBAPP_URL:
        try:
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="Task Tracker",
                    web_app=WebAppInfo(url=WEBAPP_URL)
                )
            )
            logger.info(f"Menu button set: {WEBAPP_URL}")
        except Exception as e:
            logger.warning(f"Could not set menu button: {e}")

    # Set bot commands
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start", description="Начало работы / Помощь"),
        BotCommand(command="newtask", description="Создать задачу (диалог или @username)"),
        BotCommand(command="mytasks", description="Мои активные задачи"),
        BotCommand(command="done", description="Отметить задачу выполненной"),
        BotCommand(command="myefficiency", description="Моя эффективность за неделю"),
        BotCommand(command="stats", description="Аналитика команды"),
        BotCommand(command="myteams", description="Мои команды"),
        BotCommand(command="newteam", description="Создать команду"),
        BotCommand(command="invite", description="Получить ссылку-приглашение"),
        BotCommand(command="help", description="Подробная инструкция"),
        BotCommand(command="cancel", description="Отменить текущее действие"),
    ])

    # Start deadline checker in background
    asyncio.create_task(check_deadlines())

    # Create aiohttp app
    app = create_app()

    # Start aiohttp server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    logger.info(f"API server started on {HOST}:{PORT}")

    # Start bot polling
    logger.info("Starting bot polling...")
    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
