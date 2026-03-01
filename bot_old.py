# -*- coding: utf-8 -*-
"""
云际会议 · 授权码下发机器人 (HUI_1)

职责：
  1. 管理员将主机器人发来的 #YUNJICODE:XXXX 消息转发给本机器人 → 自动入库
  2. 授权用户点「领取授权码」→ 从本地库取一个码发给用户
  3. 授权用户点「查询授权码」→ 查看本地库的码（已使用 / 未使用）

本机器人与主平台无任何直接通信，所有数据来源于本地数据库。
"""
import asyncio
import logging
import os
import re

import psycopg2
import psycopg2.extras
from datetime import datetime

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

load_dotenv()

# ============================================================
#  配置
# ============================================================
BOT_TOKEN    = os.getenv('BOT_TOKEN', '')
OWNER_ID     = int(os.getenv('OWNER_TELEGRAM_ID', '0'))
ADMIN_IDS    = {int(x) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()}
ADMIN_IDS.add(OWNER_ID)
DATABASE_URL = os.getenv('DATABASE_URL', '')


def _auto_instance_name(token: str) -> str:
    """从 Bot Token 自动获取 bot username 并生成实例名"""
    import json
    from urllib.request import urlopen
    try:
        resp = urlopen(f'https://api.telegram.org/bot{token}/getMe', timeout=10)
        data = json.loads(resp.read())
        if data.get('ok'):
            username = data['result'].get('username', '')
            if username:
                inst = re.sub(r'_?bot$', '', username.lower()).replace('-', '_')
                inst = re.sub(r'[^a-z0-9_]', '', inst)
                if inst:
                    return inst
    except Exception:
        pass
    bot_id = token.split(':')[0] if ':' in token else 'default'
    return f'bot_{bot_id}'


_env_instance = os.getenv('BOT_INSTANCE', '').strip().lower().replace('-', '_')
BOT_INSTANCE = _env_instance if _env_instance else _auto_instance_name(BOT_TOKEN)

TBL_USERS = f'users_{BOT_INSTANCE}'
TBL_CODES = f'auth_code_pool_{BOT_INSTANCE}'

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ============================================================
#  数据库
# ============================================================
# 清理 DATABASE_URL 中 psycopg2 不支持的参数
import re as _re
_clean_db_url = _re.sub(r'[&?]channel_binding=[^&]*', '', DATABASE_URL)

class DB:
    def _conn(self):
        return psycopg2.connect(_clean_db_url)

    def _cur(self, conn):
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def __init__(self):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {TBL_USERS} (
                telegram_id BIGINT PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                first_seen  TEXT NOT NULL,
                role        TEXT DEFAULT NULL
            )
        ''')
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {TBL_CODES} (
                pool_id     SERIAL PRIMARY KEY,
                code        TEXT UNIQUE NOT NULL,
                status      TEXT NOT NULL DEFAULT 'available',
                assigned_to BIGINT DEFAULT NULL,
                assigned_at TEXT DEFAULT NULL,
                note        TEXT DEFAULT '',
                added_at    TEXT NOT NULL DEFAULT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')
            )
        ''')
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='{TBL_USERS}' AND column_name='role'
        """)
        if not cur.fetchone():
            cur.execute(f'ALTER TABLE {TBL_USERS} ADD COLUMN role TEXT DEFAULT NULL')
        if OWNER_ID:
            cur.execute(
                f"INSERT INTO {TBL_USERS} (telegram_id, username, first_name, first_seen, role) "
                "VALUES (%s, '', 'ROOT', %s, 'root') "
                "ON CONFLICT(telegram_id) DO UPDATE SET role='root'",
                (OWNER_ID, datetime.now().isoformat())
            )
        conn.commit()
        conn.close()

    def track_user(self, tid, username=None, first_name=None):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(
                f'INSERT INTO {TBL_USERS} (telegram_id, username, first_name, first_seen) '
                'VALUES (%s, %s, %s, %s) '
                'ON CONFLICT(telegram_id) DO UPDATE SET username=%s, first_name=%s',
                (tid, username, first_name, datetime.now().isoformat(), username, first_name)
            )
            conn.commit()
        finally:
            conn.close()

    def get_role(self, tid):
        if tid == OWNER_ID:
            return 'root'
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f'SELECT role FROM {TBL_USERS} WHERE telegram_id=%s', (tid,))
            row = cur.fetchone()
            return row['role'] if row else None
        finally:
            conn.close()

    def is_authorized(self, tid):
        return self.get_role(tid) in ('root', 'admin')

    def get_user_info(self, tid):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f'SELECT * FROM {TBL_USERS} WHERE telegram_id=%s', (tid,))
            return cur.fetchone()
        finally:
            conn.close()

    def get_all_users(self):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f'SELECT * FROM {TBL_USERS} ORDER BY first_seen DESC')
            return cur.fetchall()
        finally:
            conn.close()

    def get_admins(self):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f"SELECT * FROM {TBL_USERS} WHERE role='admin' ORDER BY first_seen")
            return cur.fetchall()
        finally:
            conn.close()

    def bind_admin(self, tid, username=None, first_name=None):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f"SELECT COUNT(*) as c FROM {TBL_USERS} WHERE role='admin'")
            if cur.fetchone()['c'] >= 2:
                return 'max'
            cur.execute(f"SELECT role FROM {TBL_USERS} WHERE telegram_id=%s", (tid,))
            row = cur.fetchone()
            if row and row['role'] == 'root':
                return 'is_root'
            if row and row['role'] == 'admin':
                return 'already'
            cur.execute(
                f"INSERT INTO {TBL_USERS} (telegram_id, username, first_name, first_seen, role) "
                "VALUES (%s, %s, %s, %s, 'admin') "
                f"ON CONFLICT(telegram_id) DO UPDATE SET role='admin', "
                f"username=COALESCE(EXCLUDED.username, {TBL_USERS}.username), "
                f"first_name=COALESCE(EXCLUDED.first_name, {TBL_USERS}.first_name)",
                (tid, username or '', first_name or '', datetime.now().isoformat())
            )
            conn.commit()
            return 'ok'
        finally:
            conn.close()

    def unbind_admin(self, tid):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f"UPDATE {TBL_USERS} SET role=NULL WHERE telegram_id=%s AND role='admin'", (tid,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def add_code(self, code, note=''):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(
                f'INSERT INTO {TBL_CODES} (code, note) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                (code.strip().upper(), note)
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    def assign_code(self, tid):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(
                f"SELECT pool_id, code FROM {TBL_CODES} WHERE status='available' ORDER BY pool_id LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            cur.execute(
                f"UPDATE {TBL_CODES} SET status='assigned', assigned_to=%s, assigned_at=%s WHERE pool_id=%s",
                (tid, datetime.now().isoformat(), row['pool_id'])
            )
            conn.commit()
            return row['code']
        finally:
            conn.close()

    def recall_code(self, pool_id, operator_id):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            if operator_id == OWNER_ID:
                cur.execute(
                    f"UPDATE {TBL_CODES} SET status='available', assigned_to=NULL, assigned_at=NULL "
                    f"WHERE pool_id=%s AND status='assigned'",
                    (pool_id,)
                )
            else:
                cur.execute(
                    f"UPDATE {TBL_CODES} SET status='available', assigned_to=NULL, assigned_at=NULL "
                    f"WHERE pool_id=%s AND assigned_to=%s AND status='assigned'",
                    (pool_id, operator_id)
                )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def delete_code(self, code):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(
                f"DELETE FROM {TBL_CODES} WHERE code=%s AND status='available'",
                (code.upper(),)
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def stats(self):
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {TBL_CODES}")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {TBL_CODES} WHERE status='available'")
            available = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {TBL_CODES} WHERE status='assigned'")
            assigned = cur.fetchone()[0]
            return {'total': total, 'available': available, 'assigned': assigned}
        finally:
            conn.close()

    def list_available(self, limit=50):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f"SELECT * FROM {TBL_CODES} WHERE status='available' ORDER BY pool_id LIMIT %s", (limit,))
            return cur.fetchall()
        finally:
            conn.close()

    def list_assigned(self, tid=None, limit=50):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            if tid:
                cur.execute(
                    f"SELECT acp.*, u.first_name, u.username FROM {TBL_CODES} acp "
                    f"LEFT JOIN {TBL_USERS} u ON acp.assigned_to=u.telegram_id "
                    "WHERE acp.status='assigned' AND acp.assigned_to=%s ORDER BY acp.assigned_at DESC LIMIT %s",
                    (tid, limit)
                )
            else:
                cur.execute(
                    f"SELECT acp.*, u.first_name, u.username FROM {TBL_CODES} acp "
                    f"LEFT JOIN {TBL_USERS} u ON acp.assigned_to=u.telegram_id "
                    "WHERE acp.status='assigned' ORDER BY acp.assigned_at DESC LIMIT %s",
                    (limit,)
                )
            return cur.fetchall()
        finally:
            conn.close()

    def list_all(self, limit=30):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(f"SELECT * FROM {TBL_CODES} ORDER BY pool_id DESC LIMIT %s", (limit,))
            return cur.fetchall()
        finally:
            conn.close()

    def batch_assign(self, n, operator_id):
        conn = self._conn()
        try:
            cur = self._cur(conn)
            cur.execute(
                f"SELECT pool_id, code FROM {TBL_CODES} WHERE status='available' ORDER BY pool_id LIMIT %s", (n,)
            )
            rows = cur.fetchall()
            if not rows:
                return []
            ids = [r['pool_id'] for r in rows]
            placeholders = ','.join(['%s'] * len(ids))
            cur.execute(
                f"UPDATE {TBL_CODES} SET status='assigned', assigned_to=%s, assigned_at=%s WHERE pool_id IN ({placeholders})",
                [operator_id, datetime.now().isoformat()] + ids
            )
            conn.commit()
            return [r['code'] for r in rows]
        finally:
            conn.close()


db = DB()


def main_kb(role=None):
    if role in ('root', 'admin'):
        return ReplyKeyboardMarkup(
            [['📤 授权码（已使用）', '📦 授权码（未使用）']],
            resize_keyboard=True,
            is_persistent=True,
        )
    return None


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)
    role = db.get_role(user.id)

    # 非 ROOT 用户自动绑定为 admin
    if not role:
        result = db.bind_admin(user.id, user.username, user.first_name)
        if result == 'ok':
            role = 'admin'
        elif result == 'max':
            await update.message.reply_text(
                '☁️ <b>云际会议</b>\n━━━━━━━━━━━━━━━\n\n'
                f'👋 你好，{user.first_name}！\n\n'
                '⛔ 当前授权名额已满，请联系管理员。',
                parse_mode='HTML',
            )
            return

    msg = (
        '☁️ <b>云际会议</b>\n━━━━━━━━━━━━━━━\n\n'
        f'👋 欢迎，{user.first_name}！\n\n'
        '🎫 <b>领取授权码</b> — 获取一个会议授权码\n'
        '🔍 <b>查询授权码</b> — 查看已使用 / 未使用\n\n'
        '📌 <b>使用说明</b>\n━━━━━━━━━━━━━━━\n'
        '🟢 创建会议：<code>授权码 + 房间号</code>\n'
        '🔵 加入会议：<code>创建者授权码 + 房间号</code>\n\n'
        '⏰ 第一次开房间后开始计时\n'
        '🔑 一码一房间，会议结束后可再次开房间'
    )

    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=main_kb(role))


async def query_assigned_direct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """底部键盘 — 查看已使用授权码"""
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)
    if not db.is_authorized(user.id):
        await update.message.reply_text(
            '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n\n'
            f'<code>{user.id}</code>', parse_mode='HTML')
        return
    role = db.get_role(user.id)
    rows = db.list_assigned(tid=None if role == 'root' else user.id)
    if not rows:
        await update.message.reply_text(
            '📤 <b>已使用 0 个</b>\n━━━━━━━━━━━━━━━\n\n暂无已使用的授权码',
            parse_mode='HTML', reply_markup=main_kb(role))
        return
    msg = f'📤 <b>已使用 {len(rows)} 个</b>\n━━━━━━━━━━━━━━━\n'
    buttons = []
    for row in rows:
        code_val = row['code']
        at_time = row.get('assigned_at', '') or ''
        if at_time:
            at_time = at_time[:16].replace('T', ' ')
        if row['assigned_to'] and row['assigned_to'] != 0:
            fname = row.get('first_name') or str(row['assigned_to'])
            uname = f"@{row['username']}" if row.get('username') else ''
            who = f"{fname}{(' '+uname) if uname else ''}"
        else:
            who = '管理员取出'
        label = f'📤 {code_val}  →  {who}'
        if at_time:
            label += f'  ⏰{at_time}'
        buttons.append([
            InlineKeyboardButton(label, callback_data='noop'),
            InlineKeyboardButton('回收', callback_data=f'recall:{row["pool_id"]}'),
        ])
    await update.message.reply_text(msg, parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons))


async def query_available_direct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """底部键盘 — 查看未使用授权码"""
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)
    if not db.is_authorized(user.id):
        await update.message.reply_text(
            '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n\n'
            f'<code>{user.id}</code>', parse_mode='HTML')
        return
    role = db.get_role(user.id)
    rows = db.list_available(50)
    s = db.stats()
    msg = f'📦 <b>未使用授权码</b>\n━━━━━━━━━━━━━━━\n\n共 <b>{s["available"]}</b> 个\n\n'
    if rows:
        line_codes = [f'<code>{r["code"]}</code>' for r in rows]
        for i in range(0, len(line_codes), 3):
            msg += '  '.join(line_codes[i:i+3]) + '\n'
    else:
        msg += '暂无未使用的授权码\n'
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=main_kb(role))


async def claim_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)

    if not db.is_authorized(user.id):
        await update.message.reply_text(
            '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n\n'
            f'<code>{user.id}</code>',
            parse_mode='HTML',
        )
        return

    code = db.assign_code(user.id)
    if not code:
        await update.message.reply_text(
            '❌ <b>暂无可用授权码</b>\n\n请联系管理员补充。',
            parse_mode='HTML',
            reply_markup=main_kb(db.get_role(user.id)),
        )
        return

    s = db.stats()
    await update.message.reply_text(
        '✅ <b>领取成功！</b>\n━━━━━━━━━━━━━━━\n\n'
        f'🔑 授权码：<code>{code}</code>\n\n'
        '📌 创建会议：<code>授权码 + 房间号</code>\n'
        '📌 加入会议：<code>创建者授权码 + 房间号</code>\n\n'
        f'📦 剩余未使用：<b>{s["available"]}</b> 个',
        parse_mode='HTML',
        reply_markup=main_kb(db.get_role(user.id)),
    )


async def query_codes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.track_user(user.id, user.username, user.first_name)

    if not db.is_authorized(user.id):
        await update.message.reply_text(
            '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n\n'
            f'<code>{user.id}</code>',
            parse_mode='HTML',
        )
        return

    role = db.get_role(user.id)
    s = db.stats()

    # 已分发（使用中）
    assigned = db.list_assigned(tid=None if role == 'root' else user.id)
    # 未使用
    available = db.list_available(50)

    msg = '📋 <b>授权码查询</b>\n━━━━━━━━━━━━━━━\n\n'

    # —— 使用中 ——
    if assigned:
        msg += f'🔴 <b>使用中（{len(assigned)} 个）</b>\n'
        for row in assigned:
            code_val = row['code']
            at_time = row.get('assigned_at', '') or ''
            if at_time:
                at_time = at_time[:16].replace('T', ' ')
            if row['assigned_to'] and row['assigned_to'] != 0:
                fname = row.get('first_name') or str(row['assigned_to'])
                uname = f"@{row['username']}" if row.get('username') else ''
                who = f"{fname}{(' ' + uname) if uname else ''}"
            else:
                who = '管理员取出'
            time_str = f'  ⏰{at_time}' if at_time else ''
            msg += f'  <code>{code_val}</code>  →  {who}{time_str}\n'
        msg += '\n'
    else:
        msg += '🔴 <b>使用中（0 个）</b>\n  暂无\n\n'

    # —— 未使用 ——
    if available:
        msg += f'🟢 <b>未使用（{s["available"]} 个）</b>\n'
        line_codes = [f'<code>{r["code"]}</code>' for r in available]
        for i in range(0, len(line_codes), 3):
            msg += '  ' + '  '.join(line_codes[i:i + 3]) + '\n'
        msg += '\n'
    else:
        msg += '🟢 <b>未使用（0 个）</b>\n  暂无\n\n'

    msg += f'📊 总计 <b>{s["total"]}</b> 个 | 使用中 <b>{s["assigned"]}</b> | 未使用 <b>{s["available"]}</b>'

    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=main_kb(role))


async def _show_assigned(query, uid):
    role = db.get_role(uid)
    rows = db.list_assigned(tid=None if role == 'root' else uid)

    if not rows:
        await query.edit_message_text(
            '📤 当前没有已使用的授权码',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('« 返回', callback_data='query_back')]])
        )
        return

    msg = f'📤 <b>已使用 {len(rows)} 个</b>\n━━━━━━━━━━━━━━━\n'
    buttons = []
    for row in rows:
        code_val = row['code']
        at_time = row.get('assigned_at', '') or ''
        if at_time:
            at_time = at_time[:16].replace('T', ' ')  # 只显示到分钟
        if row['assigned_to'] and row['assigned_to'] != 0:
            fname = row.get('first_name') or str(row['assigned_to'])
            uname = f"@{row['username']}" if row.get('username') else ''
            who = f"{fname}{(' '+uname) if uname else ''}"
        else:
            who = '管理员取出'
        label = f'📤 {code_val}  →  {who}'
        if at_time:
            label += f'  ⏰{at_time}'
        buttons.append([
            InlineKeyboardButton(label, callback_data='noop'),
            InlineKeyboardButton('回收', callback_data=f'recall:{row["pool_id"]}'),
        ])

    buttons.append([InlineKeyboardButton('« 返回', callback_data='query_back')])
    await query.edit_message_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(buttons))


async def _show_available(query, uid):
    rows = db.list_available(50)
    s = db.stats()

    msg = (
        f'📦 <b>未使用授权码</b>\n━━━━━━━━━━━━━━━\n\n'
        f'共 <b>{s["available"]}</b> 个\n\n'
    )
    if rows:
        # 一排3个显示
        line_codes = []
        for row in rows:
            line_codes.append(f'<code>{row["code"]}</code>')
        for i in range(0, len(line_codes), 3):
            msg += '  '.join(line_codes[i:i+3]) + '\n'
    else:
        msg += '暂无未使用的授权码\n'

    await query.edit_message_text(msg, parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('« 返回', callback_data='query_back')]]))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ''
    uid = query.from_user.id

    if data == 'noop':
        return

    if data == 'query_assigned':
        await _show_assigned(query, uid)
        return

    if data == 'query_available':
        await _show_available(query, uid)
        return

    if data == 'query_back':
        s = db.stats()
        msg = (
            f'📋 <b>授权码总览</b>\n'
            f'总数：<b>{s["total"]}</b> 个\n'
            f'未使用：<b>{s["available"]}</b> 个\n'
            f'已使用：<b>{s["assigned"]}</b> 个'
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton('📤 已使用', callback_data='query_assigned'),
            InlineKeyboardButton('📦 未使用', callback_data='query_available'),
        ]])
        await query.edit_message_text(msg, parse_mode='HTML', reply_markup=kb)
        return

    if data.startswith('recall:'):
        try:
            pool_id = int(data.split(':')[1])
        except (IndexError, ValueError):
            await query.edit_message_text('❌ 无效操作')
            return
        ok = db.recall_code(pool_id, uid)
        if ok:
            s = db.stats()
            await query.edit_message_text(
                f'✅ <b>回收成功</b>\n📦 未使用：<b>{s["available"]}</b> 个',
                parse_mode='HTML'
            )
        else:
            await query.edit_message_text('❌ 回收失败（该码不属于您或已回收）')


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or '').strip()

    if uid in ADMIN_IDS and '#YUNJICODE:' in text:
        found = re.findall(r'#YUNJICODE:([A-Za-z0-9_\-]+)', text)
        if found:
            ok_list, dup_list = [], []
            for code in found:
                if db.add_code(code.upper(), note='主机器人下发'):
                    ok_list.append(code.upper())
                else:
                    dup_list.append(code.upper())
            s = db.stats()
            lines = []
            if ok_list:
                lines.append(f'✅ 入库 {len(ok_list)} 个：' + ', '.join(f'<code>{c}</code>' for c in ok_list))
            if dup_list:
                lines.append(f'⚠️ 重复跳过 {len(dup_list)} 个：' + ', '.join(f'<code>{c}</code>' for c in dup_list))
            lines.append(f'📦 当前可分发：<b>{s["available"]}</b> 个')
            await update.message.reply_text('\n'.join(lines), parse_mode='HTML')
            return

    if text == '📤 授权码（已使用）':
        await query_assigned_direct(update, context)
    elif text == '📦 授权码（未使用）':
        await query_available_direct(update, context)
    else:
        role = db.get_role(uid)
        if role:
            await update.message.reply_text('请使用下方按钮操作 👇', reply_markup=main_kb(role))
        else:
            await update.message.reply_text(
                '⛔ 您尚未被授权，请联系管理员绑定您的 ID：\n\n'
                f'<code>{uid}</code>',
                parse_mode='HTML',
            )





async def unbind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    role = db.get_role(user.id)
    if role == 'root':
        await update.message.reply_text('⚠️ ROOT 无法解绑自己')
        return
    if role != 'admin':
        await update.message.reply_text('⛔ 您未被绑定')
        return
    ok = db.unbind_admin(user.id)
    await update.message.reply_text('✅ 已解除绑定' if ok else '❌ 解绑失败')





async def on_error(update, context):
    logger.exception('Unhandled exception', exc_info=context.error)


def main():
    if not BOT_TOKEN:
        raise RuntimeError('BOT_TOKEN 未设置')
    asyncio.set_event_loop(asyncio.new_event_loop())
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', start_cmd))
    app.add_handler(CommandHandler('unbind', unbind_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)
    logger.info('☁️ 授权码下发机器人启动...')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()