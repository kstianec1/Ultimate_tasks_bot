"""
Database module — SQLite with aiosqlite.
Tables: users, teams, team_members, tasks, notifications.
"""

import aiosqlite
import os
from datetime import datetime, timezone
from typing import Optional

DB_PATH = os.getenv("DB_PATH", "tracker.db")


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db():
    """Create tables if they don't exist."""
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                invite_code TEXT UNIQUE NOT NULL,
                owner_id INTEGER NOT NULL REFERENCES users(id),
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS team_members (
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role TEXT DEFAULT 'member',  -- 'owner' | 'admin' | 'member'
                joined_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (team_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                status TEXT DEFAULT 'todo',  -- backlog|todo|in-progress|review|done
                priority TEXT DEFAULT 'medium',  -- low|medium|high
                assignee_id INTEGER REFERENCES users(id),
                creator_id INTEGER NOT NULL REFERENCES users(id),
                deadline TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                task_id INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
                type TEXT NOT NULL,  -- assigned|status_changed|deadline|comment
                message TEXT NOT NULL,
                is_read INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_team ON tasks(team_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee_id);
            CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_read);
        """)
        await db.commit()
    finally:
        await db.close()


# ── User operations ──

async def get_or_create_user(telegram_id: int, username: str = None,
                              first_name: str = None, last_name: str = None) -> dict:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        )
        row = await cursor.fetchone()
        if row:
            # Update info
            await db.execute(
                "UPDATE users SET username=?, first_name=?, last_name=? WHERE telegram_id=?",
                (username, first_name, last_name, telegram_id)
            )
            await db.commit()
            cursor = await db.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
            row = await cursor.fetchone()
            return dict(row)
        else:
            await db.execute(
                "INSERT INTO users (telegram_id, username, first_name, last_name) VALUES (?,?,?,?)",
                (telegram_id, username, first_name, last_name)
            )
            await db.commit()
            cursor = await db.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
            row = await cursor.fetchone()
            return dict(row)
    finally:
        await db.close()


async def get_user_by_telegram_id(telegram_id: int) -> Optional[dict]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


# ── Team operations ──

async def create_team(name: str, owner_id: int) -> dict:
    import secrets
    invite_code = secrets.token_urlsafe(8)
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO teams (name, invite_code, owner_id) VALUES (?,?,?)",
            (name, invite_code, owner_id)
        )
        team_id = (await (await db.execute("SELECT last_insert_rowid()")).fetchone())[0]
        await db.execute(
            "INSERT INTO team_members (team_id, user_id, role) VALUES (?,?,?)",
            (team_id, owner_id, 'owner')
        )
        await db.commit()
        cursor = await db.execute("SELECT * FROM teams WHERE id=?", (team_id,))
        return dict(await cursor.fetchone())
    finally:
        await db.close()


async def join_team(invite_code: str, user_id: int) -> Optional[dict]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM teams WHERE invite_code=?", (invite_code,))
        team = await cursor.fetchone()
        if not team:
            return None
        team = dict(team)
        # Check if already member
        cursor = await db.execute(
            "SELECT 1 FROM team_members WHERE team_id=? AND user_id=?",
            (team['id'], user_id)
        )
        if await cursor.fetchone():
            return team  # already a member
        await db.execute(
            "INSERT INTO team_members (team_id, user_id, role) VALUES (?,?,?)",
            (team['id'], user_id, 'member')
        )
        await db.commit()
        return team
    finally:
        await db.close()


async def get_user_teams(user_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT t.*, tm.role FROM teams t
            JOIN team_members tm ON t.id = tm.team_id
            WHERE tm.user_id = ?
            ORDER BY t.created_at DESC
        """, (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_team_members(team_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT u.*, tm.role FROM users u
            JOIN team_members tm ON u.id = tm.user_id
            WHERE tm.team_id = ?
            ORDER BY tm.role, u.first_name
        """, (team_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def is_team_member(team_id: int, user_id: int) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT 1 FROM team_members WHERE team_id=? AND user_id=?",
            (team_id, user_id)
        )
        return bool(await cursor.fetchone())
    finally:
        await db.close()


# ── Task operations ──

async def create_task(team_id: int, title: str, creator_id: int,
                      description: str = '', status: str = 'todo',
                      priority: str = 'medium', assignee_id: int = None,
                      deadline: str = None) -> dict:
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO tasks (team_id, title, description, status, priority, assignee_id, creator_id, deadline)
            VALUES (?,?,?,?,?,?,?,?)
        """, (team_id, title, description, status, priority, assignee_id, creator_id, deadline))
        await db.commit()
        task_id = (await (await db.execute("SELECT last_insert_rowid()")).fetchone())[0]
        return await get_task(task_id)
    finally:
        await db.close()


async def get_task(task_id: int) -> Optional[dict]:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT t.*, u.first_name as assignee_name, u.telegram_id as assignee_telegram_id
            FROM tasks t
            LEFT JOIN users u ON t.assignee_id = u.id
            WHERE t.id = ?
        """, (task_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def get_team_tasks(team_id: int, status: str = None,
                         priority: str = None, assignee_id: int = None) -> list:
    db = await get_db()
    try:
        query = """
            SELECT t.*, u.first_name as assignee_name, u.telegram_id as assignee_telegram_id
            FROM tasks t
            LEFT JOIN users u ON t.assignee_id = u.id
            WHERE t.team_id = ?
        """
        params = [team_id]
        if status:
            query += " AND t.status = ?"
            params.append(status)
        if priority:
            query += " AND t.priority = ?"
            params.append(priority)
        if assignee_id:
            query += " AND t.assignee_id = ?"
            params.append(assignee_id)
        query += " ORDER BY CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, t.created_at DESC"
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def update_task(task_id: int, **kwargs) -> Optional[dict]:
    allowed = {'title', 'description', 'status', 'priority', 'assignee_id', 'deadline'}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return await get_task(task_id)
    updates['updated_at'] = datetime.now(timezone.utc).isoformat()
    set_clause = ', '.join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [task_id]
    db = await get_db()
    try:
        await db.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
        await db.commit()
        return await get_task(task_id)
    finally:
        await db.close()


async def delete_task(task_id: int) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


async def get_my_tasks(user_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT t.*, teams.name as team_name
            FROM tasks t
            JOIN teams ON t.team_id = teams.id
            WHERE t.assignee_id = ? AND t.status != 'done'
            ORDER BY CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                     t.deadline ASC NULLS LAST
        """, (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_overdue_tasks(team_id: int = None) -> list:
    """Get tasks past their deadline that aren't done."""
    db = await get_db()
    try:
        query = """
            SELECT t.*, u.first_name as assignee_name, u.telegram_id as assignee_telegram_id
            FROM tasks t
            LEFT JOIN users u ON t.assignee_id = u.id
            WHERE t.deadline < datetime('now') AND t.status != 'done'
        """
        params = []
        if team_id:
            query += " AND t.team_id = ?"
            params.append(team_id)
        query += " ORDER BY t.deadline ASC"
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


# ── Notifications ──

async def create_notification(user_id: int, task_id: int, ntype: str, message: str):
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO notifications (user_id, task_id, type, message) VALUES (?,?,?,?)",
            (user_id, task_id, ntype, message)
        )
        await db.commit()
    finally:
        await db.close()


async def get_unread_notifications(user_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM notifications WHERE user_id=? AND is_read=0 ORDER BY created_at DESC LIMIT 20",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def mark_notifications_read(user_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (user_id,))
        await db.commit()
    finally:
        await db.close()


# ── Stats ──

async def get_team_stats(team_id: int) -> dict:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) as total FROM tasks WHERE team_id=?", (team_id,))
        total = (await cursor.fetchone())['total']

        cursor = await db.execute(
            "SELECT status, COUNT(*) as cnt FROM tasks WHERE team_id=? GROUP BY status", (team_id,)
        )
        by_status = {r['status']: r['cnt'] for r in await cursor.fetchall()}

        cursor = await db.execute(
            "SELECT priority, COUNT(*) as cnt FROM tasks WHERE team_id=? GROUP BY priority", (team_id,)
        )
        by_priority = {r['priority']: r['cnt'] for r in await cursor.fetchall()}

        cursor = await db.execute("""
            SELECT u.id, u.first_name, COUNT(t.id) as total,
                   SUM(CASE WHEN t.status='done' THEN 1 ELSE 0 END) as done
            FROM users u
            JOIN team_members tm ON u.id = tm.user_id
            LEFT JOIN tasks t ON t.assignee_id = u.id AND t.team_id = ?
            WHERE tm.team_id = ?
            GROUP BY u.id
        """, (team_id, team_id))
        members = [dict(r) for r in await cursor.fetchall()]

        overdue = await get_overdue_tasks(team_id)

        return {
            'total': total,
            'by_status': by_status,
            'by_priority': by_priority,
            'members': members,
            'overdue': len(overdue),
            'completion_rate': round(by_status.get('done', 0) / total * 100) if total else 0
        }
    finally:
        await db.close()
