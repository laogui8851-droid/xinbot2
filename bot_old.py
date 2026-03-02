# -*- coding: utf-8 -*-
"""
云际会议 · 授权码管理机器人

功能：
  1. 接收主机器人转发 #YUNJICODE:XXXX → 入库（未使用）
  2. 📤 授权码（已使用）  — 显示: 授权码 · 剩余时间 + 释放房间按钮
  3. 📦 授权码（未使用）  — 显示: 全部授权码 + 有效期
  4. 绑定 — 首次 /start 显示介绍 + 绑定按钮（最多2个admin）
  5. ROOT 可踢 admin；只有 admin / root 才能操作
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta
from urllib.request import urlopen

import psycopg2
import psycopg2.extras
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

# ═══════════════════════════════════════
#  配置
# ═══════════════════════════════════════
BOT_TOKEN           = os.getenv('BOT_TOKEN', '')
OWNER_ID            = int(os.getenv('OWNER_TELEGRAM_ID', '0'))
DATABASE_URL        = os.getenv('DATABASE_URL', '')
CODE_DURATION_HOURS = int(os.getenv('CODE_DURATION_HOURS', '12'))

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


# ── 实例名（自动从 Token 获取）──
def _auto_instance(token: str) -> str:
    try:
        data = json.loads(
            urlopen(f'https://api.telegram.org/bot{token}/getMe', timeout=10).read()
        )
        if data.get('ok'):
            name = re.sub(r'_?bot$', '', data['result'].get('username', '').lower())
            name = re.sub(r'[^a-z0-9_]', '', name.replace('-', '_'))
            if name:
                return name
    except Exception:
        pass
    return f'bot_{token.split(":")[0]}' if ':' in token else 'default'


_env_inst = os.getenv('BOT_INSTANCE', '').strip().lower().replace('-', '_')
INSTANCE  = _env_inst or _auto_instance(BOT_TOKEN)
TBL_USERS = f'users_{INSTANCE}'
TBL_CODES = f'auth_code_pool_{INSTANCE}'
_db_url   = re.sub(r'[&?]channel_binding=[^&]*', '', DATABASE_URL)


# ═══════════════════════════════════════
#  数据库
# ═══════════════════════════════════════
class DB:

    def _c(self):
        return psycopg2.connect(_db_url)

    def init(self):
        conn = self._c()
        cur = conn.cursor()
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {TBL_USERS} (
                telegram_id  BIGINT PRIMARY KEY,
                username     TEXT,
                first_name   TEXT,
                first_seen   TEXT NOT NULL,
                role         TEXT DEFAULT NULL
            )
        ''')
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {TBL_CODES} (
                pool_id      SERIAL PRIMARY KEY,
                code         TEXT UNIQUE NOT NULL,
                status       TEXT NOT NULL DEFAULT 'available',
                assigned_to  BIGINT DEFAULT NULL,
                assigned_at  TEXT DEFAULT NULL,
                note         TEXT DEFAULT '',
                added_at     TEXT NOT NULL DEFAULT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')
            )
        ''')
        cur.execute(f"""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='{TBL_USERS}' AND column_name='role'
        """)
        if not cur.fetchone():
            cur.execute(f'ALTER TABLE {TBL_USERS} ADD COLUMN role TEXT DEFAULT NULL')
        if OWNER_ID:
            cur.execute(
                f"INSERT INTO {TBL_USERS} (telegram_id,username,first_name,first_seen,role) "
                f"VALUES (%s,'','ROOT',%s,'root') "
                f"ON CONFLICT(telegram_id) DO UPDATE SET role='root'",
                (OWNER_ID, datetime.now().isoformat()),
            )
        conn.commit()
        conn.close()

    # ── 用户 ──
    def track(self, tid, username=None, first_name=None):
        conn = self._c()
        try:
            conn.cursor().execute(
                f'INSERT INTO {TBL_USERS} (telegram_id,username,first_name,first_seen) '
                f'VALUES (%s,%s,%s,%s) '
                f'ON CONFLICT(telegram_id) DO UPDATE SET username=%s,first_name=%s',
                (tid, username, first_name, datetime.now().isoformat(), username, first_name),
            )
            conn.commit()
        finally:
            conn.close()

    def role(self, tid):
        if tid == OWNER_ID:
            return 'root'
        conn = self._c()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f'SELECT role FROM {TBL_USERS} WHERE telegram_id=%s', (tid,))
            r = cur.fetchone()
            return r['role'] if r else None
        finally:
            conn.close()

    def is_auth(self, tid):
        return self.role(tid) in ('root', 'admin')

    # ── 绑定 / 解绑 ──
    def bind(self, tid, username=None, first_name=None):
        conn = self._c()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT COUNT(*) AS c FROM {TBL_USERS} WHERE role='admin'")
            if cur.fetchone()['c'] >= 2:
                return 'max'
            cur.execute(f'SELECT role FROM {TBL_USERS} WHERE telegram_id=%s', (tid,))
            row = cur.fetchone()
            if row and row['role'] == 'root':
                return 'is_root'
            if row and row['role'] == 'admin':
                return 'already'
            cur.execute(
                f"INSERT INTO {TBL_USERS} (telegram_id,username,first_name,first_seen,role) "
                f"VALUES (%s,%s,%s,%s,'admin') "
                f"ON CONFLICT(telegram_id) DO UPDATE SET role='admin',"
                f"username=COALESCE(EXCLUDED.username,{TBL_USERS}.username),"
                f"first_name=COALESCE(EXCLUDED.first_name,{TBL_USERS}.first_name)",
                (tid, username or '', first_name or '', datetime.now().isoformat()),
            )
            conn.commit()
            return 'ok'
        finally:
            conn.close()

    def unbind(self, tid):
        conn = self._c()
        try:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE {TBL_USERS} SET role=NULL WHERE telegram_id=%s AND role='admin'",
                (tid,),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def admins(self):
        conn = self._c()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM {TBL_USERS} WHERE role='admin' ORDER BY first_seen")
            return cur.fetchall()
        finally:
            conn.close()

    # ── 授权码 ──
    def add_code(self, code, note=''):
        conn = self._c()
        try:
            cur = conn.cursor()
            cur.execute(
                f'INSERT INTO {TBL_CODES} (code,note) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                (code.strip().upper(), note),
            )
            conn.commit()
            return cur.rowcount > 0
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

    def available(self, limit=50):
        conn = self._c()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                f"SELECT * FROM {TBL_CODES} WHERE status='available' ORDER BY pool_id LIMIT %s",
                (limit,),
            )
            return cur.fetchall()
        finally:
            conn.close()

    def assigned(self, tid=None, limit=50):
        conn = self._c()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            base = (
                f"SELECT c.*, u.first_name, u.username FROM {TBL_CODES} c "
                f"LEFT JOIN {TBL_USERS} u ON c.assigned_to=u.telegram_id "
                f"WHERE c.status='assigned' "
            )
            if tid:
                cur.execute(base + "AND c.assigned_to=%s ORDER BY c.assigned_at DESC LIMIT %s", (tid, limit))
            else:
                cur.execute(base + "ORDER BY c.assigned_at DESC LIMIT %s", (limit,))
            return cur.fetchall()
        finally:
            conn.close()

    def recall(self, pool_id, operator_id):
        conn = self._c()
        try:
            cur = conn.cursor()
            if operator_id == OWNER_ID:
                cur.execute(
                    f"UPDATE {TBL_CODES} SET status='available',assigned_to=NULL,assigned_at=NULL "
                    f"WHERE pool_id=%s AND status='assigned'",
                    (pool_id,),
                )
            else:
                cur.execute(
                    f"UPDATE {TBL_CODES} SET status='available',assigned_to=NULL,assigned_at=NULL "
                    f"WHERE pool_id=%s AND assigned_to=%s AND status='assigned'",
                    (pool_id, operator_id),
                )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def stats(self):
        conn = self._c()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {TBL_CODES}")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {TBL_CODES} WHERE status='available'")
            avail = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {TBL_CODES} WHERE status='assigned'")
            used = cur.fetchone()[0]
            return {'total': total, 'available': avail, 'assigned': used}
        finally:
            conn.close()


db = DB()


# ═══════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════
def _remain(assigned_at: str) -> str:
    """计算剩余时间"""
    try:
        start   = datetime.fromisoformat(assigned_at)
        expires = start + timedelta(hours=CODE_DURATION_HOURS)
        delta   = expires - datetime.now()
        if delta.total_seconds() <= 0:
            return '已过期'
        h = int(delta.total_seconds() // 3600)
        m = int((delta.total_seconds() % 3600) // 60)
        return f'{h}小时{m}分' if h else f'{m}分钟'
    except Exception:
        return '未知'


def _kb():
    """底部常驻键盘 — 仅2个按钮"""
    return ReplyKeyboardMarkup(
        [
            ['📤 授权码（已使用）', '📦 授权码（未使用）'],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


# ═══════════════════════════════════════
#  /start
# ═══════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    db.track(u.id, u.username, u.first_name)
    role = db.role(u.id)

    if role in ('root', 'admin'):
        s = db.stats()
        await update.message.reply_text(
            f'☁️ <b>云际会议</b>\n━━━━━━━━━━━━━━━\n\n'
            f'👋 欢迎回来，{u.first_name}！\n\n'
            f'📊 授权码：共 <b>{s["total"]}</b> · '
            f'已使用 <b>{s["assigned"]}</b> · '
            f'未使用 <b>{s["available"]}</b>\n\n'
            f'👇 点击下方按钮查看',
            parse_mode='HTML', reply_markup=_kb(),
        )
        return

    await update.message.reply_text(
        '☁️ <b>云际会议 · 授权码管理</b>\n'
        '━━━━━━━━━━━━━━━\n\n'
        '📌 <b>使用说明</b>\n\n'
        '  🟢 <b>创建会议</b>：授权码 + 房间号\n'
        '  🔵 <b>加入会议</b>：创建者授权码 + 房间号\n\n'
        '  ⏰ 第一次进入房间后授权码开始计时\n'
        '  🔑 一码一房间，会议结束后可再次开设\n\n'
        '━━━━━━━━━━━━━━━\n'
        '点击下方按钮绑定为管理员（名额有限）',
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('🔗 绑定', callback_data='bind')]
        ]),
    )


# ═══════════════════════════════════════
#  /unbind — ROOT 踢 admin
# ═══════════════════════════════════════
async def cmd_unbind(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if db.role(u.id) != 'root':
        await update.message.reply_text('⛔ 只有 ROOT 才能解绑')
        return

    admins = db.admins()
    if not admins:
        await update.message.reply_text('当前没有绑定的管理员')
        return

    buttons = []
    for a in admins:
        name = a.get('first_name') or str(a['telegram_id'])
        uname = f" @{a['username']}" if a.get('username') else ''
        buttons.append([
            InlineKeyboardButton(f'❌ 踢出 {name}{uname}', callback_data=f'kick:{a["telegram_id"]}')
        ])

    await update.message.reply_text(
        '👥 <b>当前管理员</b>\n\n点击踢出：',
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ═══════════════════════════════════════
#  底部键盘 — 已使用 / 未使用
# ═══════════════════════════════════════
async def show_used(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    db.track(u.id, u.username, u.first_name)
    if not db.is_auth(u.id):
        await update.message.reply_text('⛔ 未绑定，请先 /start 后点击「绑定」')
        return

    role = db.role(u.id)
    rows = db.assigned(tid=None if role == 'root' else u.id)

    if not rows:
        await update.message.reply_text(
            '📤 <b>已使用 0 个</b>\n━━━━━━━━━━━━━━━\n\n暂无',
            parse_mode='HTML', reply_markup=_kb(),
        )
        return

    msg = f'📤 <b>已使用 {len(rows)} 个</b>\n━━━━━━━━━━━━━━━\n'
    buttons = []
    for r in rows:
        at     = r.get('assigned_at') or ''
        remain = _remain(at) if at else f'{CODE_DURATION_HOURS}小时'
        buttons.append([
            InlineKeyboardButton(f'🔑 {r["code"]}  ⏳ {remain}', callback_data='noop'),
            InlineKeyboardButton('🏠 释放房间', callback_data=f'end:{r["pool_id"]}'),
        ])

    await update.message.reply_text(
        msg, parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def show_unused(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    db.track(u.id, u.username, u.first_name)
    if not db.is_auth(u.id):
        await update.message.reply_text('⛔ 未绑定，请先 /start 后点击「绑定」')
        return

    rows = db.available(50)
    s = db.stats()

    msg = f'📦 <b>未使用 {s["available"]} 个</b>\n━━━━━━━━━━━━━━━\n\n'
    if rows:
        for r in rows:
            msg += f'  <code>{r["code"]}</code>  {CODE_DURATION_HOURS}小时\n'
    else:
        msg += '暂无\n'

    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=_kb())


# show_query 已移除 — 仅保留「已使用」和「未使用」两个按钮


# ═══════════════════════════════════════
#  回调按钮
# ═══════════════════════════════════════
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = q.from_user.id
    data = q.data or ''
    await q.answer()

    if data == 'noop':
        return

    # ── 绑定 ──
    if data == 'bind':
        result = db.bind(uid, q.from_user.username, q.from_user.first_name)
        msgs = {
            'ok':      ('✅ <b>绑定成功！</b>\n\n欢迎，你已成为管理员。', True),
            'max':     ('⛔ 管理员名额已满（最多 2 个），请联系 ROOT。', False),
            'already': ('✅ 你已经是管理员了。', True),
            'is_root': ('👑 你是 ROOT，无需绑定。', True),
        }
        text, show_kb = msgs.get(result, ('❌ 绑定失败', False))
        await q.edit_message_text(text, parse_mode='HTML')
        if show_kb:
            await q.message.reply_text('👇 使用下方按钮操作', reply_markup=_kb())
        return

    # ── 结束（回收）──
    if data.startswith('end:'):
        pool_id = int(data.split(':')[1])
        ok = db.recall(pool_id, uid)
        if ok:
            s = db.stats()
            await q.edit_message_text(
                f'✅ <b>房间已释放</b>\n\n'
                f'授权码已回收至未使用\n'
                f'📦 未使用：<b>{s["available"]}</b> 个',
                parse_mode='HTML',
            )
        else:
            await q.edit_message_text('❌ 操作失败（不属于您或已回收）')
        return

    # ── ROOT 踢人 ──
    if data.startswith('kick:'):
        if uid != OWNER_ID:
            await q.edit_message_text('⛔ 只有 ROOT 才能踢人')
            return
        target = int(data.split(':')[1])
        ok = db.unbind(target)
        await q.edit_message_text(
            f'✅ 已解绑 <code>{target}</code>' if ok else '❌ 解绑失败',
            parse_mode='HTML',
        )
        return


# ═══════════════════════════════════════
#  文本消息
# ═══════════════════════════════════════
async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u    = update.effective_user
    text = (update.message.text or '').strip()

    # ── 接收主机器人授权码 ──
    if db.is_auth(u.id) and '#YUNJICODE:' in text:
        found = re.findall(r'#YUNJICODE:([A-Za-z0-9_\-]+)', text)
        if found:
            ok_list, dup_list = [], []
            for c in found:
                (ok_list if db.add_code(c.upper(), '主机器人下发') else dup_list).append(c.upper())
            s = db.stats()
            lines = []
            if ok_list:
                lines.append(f'✅ 入库 {len(ok_list)} 个：' +
                             ', '.join(f'<code>{c}</code>' for c in ok_list))
            if dup_list:
                lines.append(f'⚠️ 重复 {len(dup_list)} 个：' +
                             ', '.join(f'<code>{c}</code>' for c in dup_list))
            lines.append(f'📦 未使用：<b>{s["available"]}</b> 个')
            await update.message.reply_text('\n'.join(lines), parse_mode='HTML')
            return

    # ── 底部键盘 ──
    if text == '📤 授权码（已使用）':
        await show_used(update, ctx)
    elif text == '📦 授权码（未使用）':
        await show_unused(update, ctx)
    else:
        role = db.role(u.id)
        if role:
            await update.message.reply_text('👇 请使用下方按钮', reply_markup=_kb())
        else:
            await update.message.reply_text('⛔ 未绑定，请先 /start 后点击「绑定」')


# ═══════════════════════════════════════
#  错误处理 & 启动
# ═══════════════════════════════════════
async def on_error(update, ctx):
    log.exception('Unhandled', exc_info=ctx.error)


def main():
    if not BOT_TOKEN:
        raise RuntimeError('BOT_TOKEN 未设置')
    db.init()
    asyncio.set_event_loop(asyncio.new_event_loop())
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('unbind', cmd_unbind))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)
    log.info('☁️ 授权码管理机器人启动...')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
