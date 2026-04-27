"""
Database module — SQLite with aiosqlite.
Tables: users, teams, team_members, tasks, notifications, tags, task_tags, weekly_stats.
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
                role TEXT DEFAULT 'member',
                joined_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (team_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY,
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                color TEXT DEFAULT '#6c5ce7',
                UNIQUE(team_id, name)
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                status TEXT DEFAULT 'todo',
                priority TEXT DEFAULT 'medium',
                assignee_id INTEGER REFERENCES users(id),
                creator_id INTEGER NOT NULL REFERENCES users(id),
                deadline TEXT,
                tag_id INTEGER REFERENCES tags(id) ON DELETE SET NULL,
                notified_24h INTEGER DEFAULT 0,
                notified_1h INTEGER DEFAULT 0,
                notified_overdue INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                task_id INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
                type TEXT NOT NULL,
                message TEXT NOT NULL,
                is_read INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS weekly_stats (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                week_start TEXT NOT NULL,
                tasks_assigned INTEGER DEFAULT 0,
                tasks_completed INTEGER DEFAULT 0,
                tasks_overdue INTEGER DEFAULT 0,
                avg_completion_hours REAL DEFAULT 0,
                UNIQUE(user_id, team_id, week_start)
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_team ON tasks(team_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_tag ON tasks(tag_id);
            CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_read);
            CREATE INDEX IF NOT EXISTS idx_weekly_stats_user ON weekly_stats(user_id, week_start);

            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                text TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS recurring_tasks (
                id INTEGER PRIMARY KEY,
                team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                priority TEXT DEFAULT 'medium',
                assignee_id INTEGER REFERENCES users(id),
                tag_id INTEGER REFERENCES tags(id) ON DELETE SET NULL,
                recurrence TEXT NOT NULL,
                recurrence_day INTEGER,
                estimated_hours REAL DEFAULT 0,
                last_created TEXT,
                created_by INTEGER NOT NULL REFERENCES users(id),
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_comments_task ON comments(task_id);
            CREATE INDEX IF NOT EXISTS idx_activity_task ON activity_log(task_id);
            CREATE INDEX IF NOT EXISTS idx_recurring_team ON recurring_tasks(team_id);
        """)
        await db.commit()

        # Migrations for existing DBs
        migrations = [
            "ALTER TABLE tasks ADD COLUMN notified_24h INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN notified_1h INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN notified_overdue INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN tag_id INTEGER REFERENCES tags(id) ON DELETE SET NULL",
            "ALTER TABLE tasks ADD COLUMN estimated_hours REAL DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN status_changed_at TEXT",
            "ALTER TABLE tasks ADD COLUMN recurrence TEXT",
            "ALTER TABLE tasks ADD COLUMN recurrence_parent_id INTEGER",
        ]
        for sql in migrations:
            try:
                await db.execute(sql)
                await db.commit()
            except Exception:
                pass  # Column already exists
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


async def get_user_by_username(username: str) -> Optional[dict]:
    """Find user by @username (without @)."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM users WHERE LOWER(username)=LOWER(?)", (username.lstrip('@'),)
        )
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
        cursor = await db.execute(
            "SELECT 1 FROM team_members WHERE team_id=? AND user_id=?",
            (team['id'], user_id)
        )
        if await cursor.fetchone():
            return team
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


# ── Tag operations ──

async def get_team_tags(team_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM tags WHERE team_id=? ORDER BY name", (team_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_or_create_tag(team_id: int, name: str, color: str = '#6c5ce7') -> dict:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM tags WHERE team_id=? AND LOWER(name)=LOWER(?)", (team_id, name)
        )
        row = await cursor.fetchone()
        if row:
            return dict(row)
        await db.execute(
            "INSERT INTO tags (team_id, name, color) VALUES (?,?,?)",
            (team_id, name, color)
        )
        await db.commit()
        cursor = await db.execute(
            "SELECT * FROM tags WHERE team_id=? AND LOWER(name)=LOWER(?)", (team_id, name)
        )
        return dict(await cursor.fetchone())
    finally:
        await db.close()


async def delete_tag(tag_id: int, team_id: int) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute(
            "DELETE FROM tags WHERE id=? AND team_id=?", (tag_id, team_id)
        )
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


# ── Task operations ──

async def create_task(team_id: int, title: str, creator_id: int,
                      description: str = '', status: str = 'todo',
                      priority: str = 'medium', assignee_id: int = None,
                      deadline: str = None, tag_id: int = None) -> dict:
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO tasks (team_id, title, description, status, priority,
                               assignee_id, creator_id, deadline, tag_id)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (team_id, title, description, status, priority,
              assignee_id, creator_id, deadline, tag_id))
        await db.commit()
        task_id = (await (await db.execute("SELECT last_insert_rowid()")).fetchone())[0]
        return await get_task(task_id)
    finally:
        await db.close()


async def get_task(task_id: int) -> Optional[dict]:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT t.*,
                   u.first_name as assignee_name,
                   u.telegram_id as assignee_telegram_id,
                   u.username as assignee_username,
                   tg.name as tag_name,
                   tg.color as tag_color
            FROM tasks t
            LEFT JOIN users u ON t.assignee_id = u.id
            LEFT JOIN tags tg ON t.tag_id = tg.id
            WHERE t.id = ?
        """, (task_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def get_team_tasks(team_id: int, status: str = None,
                         priority: str = None, assignee_id: int = None,
                         tag_id: int = None) -> list:
    db = await get_db()
    try:
        query = """
            SELECT t.*,
                   u.first_name as assignee_name,
                   u.telegram_id as assignee_telegram_id,
                   tg.name as tag_name,
                   tg.color as tag_color
            FROM tasks t
            LEFT JOIN users u ON t.assignee_id = u.id
            LEFT JOIN tags tg ON t.tag_id = tg.id
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
        if tag_id:
            query += " AND t.tag_id = ?"
            params.append(tag_id)
        query += " ORDER BY CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, t.created_at DESC"
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def update_task(task_id: int, **kwargs) -> Optional[dict]:
    allowed = {'title', 'description', 'status', 'priority', 'assignee_id', 'deadline', 'tag_id', 'estimated_hours'}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return await get_task(task_id)
    updates['updated_at'] = datetime.now(timezone.utc).isoformat()
    if 'deadline' in updates:
        updates['notified_24h'] = 0
        updates['notified_1h'] = 0
        updates['notified_overdue'] = 0
    set_clause = ', '.join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [task_id]
    db = await get_db()
    try:
        await db.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
        await db.commit()
        # Update weekly stats if task completed
        if updates.get('status') == 'done':
            task = await get_task(task_id)
            if task and task.get('assignee_id'):
                await _record_task_completed(task['assignee_id'], task['team_id'])
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
            SELECT t.*, teams.name as team_name,
                   tg.name as tag_name, tg.color as tag_color
            FROM tasks t
            JOIN teams ON t.team_id = teams.id
            LEFT JOIN tags tg ON t.tag_id = tg.id
            WHERE t.assignee_id = ? AND t.status != 'done'
            ORDER BY CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                     t.deadline ASC NULLS LAST
        """, (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_overdue_tasks(team_id: int = None) -> list:
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


# ── Weekly stats ──

def _get_week_start() -> str:
    """Get Monday of current week as ISO string."""
    from datetime import date, timedelta
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()


async def _record_task_completed(user_id: int, team_id: int):
    """Increment completed tasks for current week."""
    week_start = _get_week_start()
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO weekly_stats (user_id, team_id, week_start, tasks_completed)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id, team_id, week_start)
            DO UPDATE SET tasks_completed = tasks_completed + 1
        """, (user_id, team_id, week_start))
        await db.commit()
    finally:
        await db.close()


async def record_task_assigned(user_id: int, team_id: int):
    """Increment assigned tasks for current week."""
    week_start = _get_week_start()
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO weekly_stats (user_id, team_id, week_start, tasks_assigned)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id, team_id, week_start)
            DO UPDATE SET tasks_assigned = tasks_assigned + 1
        """, (user_id, team_id, week_start))
        await db.commit()
    finally:
        await db.close()


async def get_my_weekly_stats(user_id: int, weeks: int = 8) -> list:
    """Get personal weekly stats for the last N weeks."""
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT ws.*, t.name as team_name
            FROM weekly_stats ws
            JOIN teams t ON ws.team_id = t.id
            WHERE ws.user_id = ?
            ORDER BY ws.week_start DESC
            LIMIT ?
        """, (user_id, weeks * 10))  # multiple teams
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_my_efficiency(user_id: int) -> dict:
    """Get personal efficiency metrics."""
    db = await get_db()
    try:
        # Current week
        week_start = _get_week_start()
        cursor = await db.execute("""
            SELECT
                SUM(tasks_assigned) as assigned,
                SUM(tasks_completed) as completed,
                SUM(tasks_overdue) as overdue
            FROM weekly_stats
            WHERE user_id = ? AND week_start = ?
        """, (user_id, week_start))
        current = dict(await cursor.fetchone())

        # Last 4 weeks average
        cursor = await db.execute("""
            SELECT
                AVG(tasks_completed * 1.0 / NULLIF(tasks_assigned, 0)) as avg_rate,
                SUM(tasks_completed) as total_completed,
                SUM(tasks_assigned) as total_assigned
            FROM weekly_stats
            WHERE user_id = ? AND week_start >= date(?, '-28 days')
        """, (user_id, week_start))
        avg = dict(await cursor.fetchone())

        # All-time
        cursor = await db.execute("""
            SELECT COUNT(*) as total_done
            FROM tasks
            WHERE assignee_id = ? AND status = 'done'
        """, (user_id,))
        alltime = dict(await cursor.fetchone())

        # Active tasks
        cursor = await db.execute("""
            SELECT COUNT(*) as active
            FROM tasks
            WHERE assignee_id = ? AND status != 'done'
        """, (user_id,))
        active = dict(await cursor.fetchone())

        # Weekly history (last 8 weeks)
        cursor = await db.execute("""
            SELECT week_start,
                   SUM(tasks_assigned) as assigned,
                   SUM(tasks_completed) as completed
            FROM weekly_stats
            WHERE user_id = ?
            GROUP BY week_start
            ORDER BY week_start DESC
            LIMIT 8
        """, (user_id,))
        history = [dict(r) for r in await cursor.fetchall()]

        assigned_this_week = current.get('assigned') or 0
        completed_this_week = current.get('completed') or 0
        rate = round(completed_this_week / assigned_this_week * 100) if assigned_this_week else 0

        return {
            'week_start': week_start,
            'assigned_this_week': assigned_this_week,
            'completed_this_week': completed_this_week,
            'overdue_this_week': current.get('overdue') or 0,
            'completion_rate': rate,
            'avg_rate_4w': round((avg.get('avg_rate') or 0) * 100),
            'total_completed_alltime': alltime.get('total_done') or 0,
            'active_tasks': active.get('active') or 0,
            'history': history,
        }
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

        # By tag
        cursor = await db.execute("""
            SELECT tg.id, tg.name, tg.color, COUNT(t.id) as cnt
            FROM tags tg
            LEFT JOIN tasks t ON t.tag_id = tg.id AND t.team_id = ?
            WHERE tg.team_id = ?
            GROUP BY tg.id
        """, (team_id, team_id))
        by_tag = [dict(r) for r in await cursor.fetchall()]

        overdue = await get_overdue_tasks(team_id)

        return {
            'total': total,
            'by_status': by_status,
            'by_priority': by_priority,
            'by_tag': by_tag,
            'members': members,
            'overdue': len(overdue),
            'completion_rate': round(by_status.get('done', 0) / total * 100) if total else 0
        }
    finally:
        await db.close()


# ── Comments ──

async def add_comment(task_id: int, user_id: int, text: str) -> dict:
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO comments (task_id, user_id, text) VALUES (?,?,?)",
            (task_id, user_id, text)
        )
        await db.commit()
        comment_id = (await (await db.execute("SELECT last_insert_rowid()")).fetchone())[0]
        cursor = await db.execute("""
            SELECT c.*, u.first_name, u.username
            FROM comments c JOIN users u ON c.user_id = u.id
            WHERE c.id = ?
        """, (comment_id,))
        return dict(await cursor.fetchone())
    finally:
        await db.close()


async def get_task_comments(task_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT c.*, u.first_name, u.username
            FROM comments c JOIN users u ON c.user_id = u.id
            WHERE c.task_id = ?
            ORDER BY c.created_at ASC
        """, (task_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def delete_comment(comment_id: int, user_id: int) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute(
            "DELETE FROM comments WHERE id=? AND user_id=?", (comment_id, user_id)
        )
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


# ── Activity Log ──

async def log_activity(task_id: int, user_id: int, action: str,
                        old_value: str = None, new_value: str = None):
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO activity_log (task_id, user_id, action, old_value, new_value) VALUES (?,?,?,?,?)",
            (task_id, user_id, action, old_value, new_value)
        )
        await db.commit()
    finally:
        await db.close()


async def get_task_activity(task_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT a.*, u.first_name, u.username
            FROM activity_log a JOIN users u ON a.user_id = u.id
            WHERE a.task_id = ?
            ORDER BY a.created_at ASC
        """, (task_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


# ── Recurring Tasks ──

async def create_recurring_task(team_id: int, title: str, description: str,
                                  priority: str, assignee_id: int, tag_id: int,
                                  recurrence: str, recurrence_day: int,
                                  estimated_hours: float, created_by: int) -> dict:
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO recurring_tasks
            (team_id, title, description, priority, assignee_id, tag_id,
             recurrence, recurrence_day, estimated_hours, created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (team_id, title, description, priority, assignee_id, tag_id,
              recurrence, recurrence_day, estimated_hours, created_by))
        await db.commit()
        rid = (await (await db.execute("SELECT last_insert_rowid()")).fetchone())[0]
        cursor = await db.execute("SELECT * FROM recurring_tasks WHERE id=?", (rid,))
        return dict(await cursor.fetchone())
    finally:
        await db.close()


async def get_team_recurring_tasks(team_id: int) -> list:
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT r.*, u.first_name as assignee_name, tg.name as tag_name
            FROM recurring_tasks r
            LEFT JOIN users u ON r.assignee_id = u.id
            LEFT JOIN tags tg ON r.tag_id = tg.id
            WHERE r.team_id = ? AND r.is_active = 1
            ORDER BY r.created_at DESC
        """, (team_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def delete_recurring_task(recurring_id: int, team_id: int) -> bool:
    db = await get_db()
    try:
        cursor = await db.execute(
            "UPDATE recurring_tasks SET is_active=0 WHERE id=? AND team_id=?",
            (recurring_id, team_id)
        )
        await db.commit()
        return cursor.rowcount > 0
    finally:
        await db.close()


async def spawn_recurring_tasks():
    """Check and create due recurring tasks. Called from background loop."""
    from datetime import datetime, date, timedelta
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM recurring_tasks WHERE is_active=1"
        )
        recurring = await cursor.fetchall()
        today = date.today()
        created = []

        for r in recurring:
            r = dict(r)
            last = date.fromisoformat(r['last_created']) if r.get('last_created') else None
            recurrence = r['recurrence']
            day = r.get('recurrence_day') or 1

            should_create = False
            if recurrence == 'daily':
                should_create = last != today
            elif recurrence == 'weekly':
                # day 0=Mon..6=Sun
                should_create = today.weekday() == day and last != today
            elif recurrence == 'monthly':
                should_create = today.day == day and (not last or last.month != today.month)

            if not should_create:
                continue

            # Calculate deadline: end of today
            from datetime import time as dtime
            deadline = datetime.combine(today, dtime(23, 59)).strftime('%Y-%m-%d %H:%M')

            await db.execute("""
                INSERT INTO tasks (team_id, title, description, status, priority,
                                   assignee_id, creator_id, deadline, tag_id,
                                   estimated_hours, recurrence, recurrence_parent_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (r['team_id'], r['title'], r['description'], 'todo', r['priority'],
                  r['assignee_id'], r['created_by'], deadline, r['tag_id'],
                  r['estimated_hours'], recurrence, r['id']))

            await db.execute(
                "UPDATE recurring_tasks SET last_created=? WHERE id=?",
                (today.isoformat(), r['id'])
            )
            task_id = (await (await db.execute("SELECT last_insert_rowid()")).fetchone())[0]
            created.append(task_id)

        await db.commit()
        return created
    finally:
        await db.close()


# ── Burndown & Time-in-status ──

async def get_burndown_data(team_id: int, days: int = 14) -> dict:
    """Get burndown chart data: tasks created vs completed per day."""
    db = await get_db()
    try:
        from datetime import date, timedelta
        today = date.today()
        start = today - timedelta(days=days)

        cursor = await db.execute("""
            SELECT date(created_at) as day, COUNT(*) as created
            FROM tasks
            WHERE team_id=? AND date(created_at) >= ?
            GROUP BY day ORDER BY day
        """, (team_id, start.isoformat()))
        created_by_day = {r['day']: r['created'] for r in await cursor.fetchall()}

        cursor = await db.execute("""
            SELECT date(updated_at) as day, COUNT(*) as done
            FROM tasks
            WHERE team_id=? AND status='done' AND date(updated_at) >= ?
            GROUP BY day ORDER BY day
        """, (team_id, start.isoformat()))
        done_by_day = {r['day']: r['done'] for r in await cursor.fetchall()}

        # Total open tasks at start of period
        cursor = await db.execute("""
            SELECT COUNT(*) as cnt FROM tasks
            WHERE team_id=? AND date(created_at) < ?
            AND (status != 'done' OR date(updated_at) >= ?)
        """, (team_id, start.isoformat(), start.isoformat()))
        initial_open = (await cursor.fetchone())['cnt']

        days_list = []
        running_open = initial_open
        for i in range(days + 1):
            d = (start + timedelta(days=i)).isoformat()
            created = created_by_day.get(d, 0)
            done = done_by_day.get(d, 0)
            running_open = running_open + created - done
            days_list.append({'date': d, 'open': max(running_open, 0), 'done': done, 'created': created})

        return {'days': days_list, 'period_days': days}
    finally:
        await db.close()


async def get_time_in_status(team_id: int) -> list:
    """Get average time tasks spend in each status based on activity log."""
    db = await get_db()
    try:
        # Get all status change events for team tasks
        cursor = await db.execute("""
            SELECT a.task_id, a.old_value as from_status, a.new_value as to_status,
                   a.created_at
            FROM activity_log a
            JOIN tasks t ON a.task_id = t.id
            WHERE t.team_id = ? AND a.action = 'status_changed'
            ORDER BY a.task_id, a.created_at
        """, (team_id,))
        events = [dict(r) for r in await cursor.fetchall()]

        # Calculate time spent in each status
        from datetime import datetime
        status_times = {}  # status -> list of hours
        task_status_start = {}  # task_id -> (status, start_time)

        for e in events:
            task_id = e['task_id']
            from_s = e['from_status']
            to_s = e['to_status']
            try:
                ts_str = e['created_at'].replace('Z', '').split('+')[0]
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                continue

            if task_id in task_status_start:
                prev_status, prev_time = task_status_start[task_id]
                hours = (ts - prev_time).total_seconds() / 3600
                if hours >= 0:  # Ignore negative values from out-of-order events
                    if prev_status not in status_times:
                        status_times[prev_status] = []
                    status_times[prev_status].append(hours)

            task_status_start[task_id] = (to_s, ts)

        result = []
        for status, times in status_times.items():
            if times:
                result.append({
                    'status': status,
                    'avg_hours': round(sum(times) / len(times), 1),
                    'count': len(times)
                })
        result.sort(key=lambda x: ['backlog','todo','in-progress','review','done'].index(x['status'])
                    if x['status'] in ['backlog','todo','in-progress','review','done'] else 99)
        return result
    finally:
        await db.close()


async def update_task_with_log(task_id: int, user_id: int, **kwargs) -> Optional[dict]:
    """Update task and log changes to activity_log."""
    old_task = await get_task(task_id)
    if not old_task:
        return None

    updated = await update_task(task_id, **kwargs)

    # Log each changed field
    loggable = {'status', 'priority', 'assignee_id', 'deadline', 'title', 'tag_id', 'estimated_hours'}
    for field in loggable:
        if field in kwargs and kwargs[field] != old_task.get(field):
            old_val = str(old_task.get(field) or '')
            new_val = str(kwargs[field] or '')
            if field == 'status':
                action = 'status_changed'
            elif field == 'assignee_id':
                action = 'assignee_changed'
            else:
                action = f'{field}_changed'
            await log_activity(task_id, user_id, action, old_val, new_val)

            # Track status_changed_at
            if field == 'status':
                db = await get_db()
                try:
                    await db.execute(
                        "UPDATE tasks SET status_changed_at=datetime('now') WHERE id=?",
                        (task_id,)
                    )
                    await db.commit()
                finally:
                    await db.close()

    return updated
