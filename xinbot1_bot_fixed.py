# -*- coding: utf-8 -*-

import os

import re

import logging

import secrets

import asyncio

import random

from datetime import datetime, timedelta

import aiohttp

import psycopg2

import psycopg2.extras


from io import BytesIO  # 补充二维码生成所需导入

from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton  # noqa: F401

from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, filters

from telegram.error import TimedOut, NetworkError



load_dotenv(override=True)





logging.basicConfig(

    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',

    level=logging.INFO,

)

logger = logging.getLogger(__name__)



BOT_TOKEN = os.getenv('BOT_TOKEN', '').strip()

ADMIN_IDS = {int(x) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()}

ROOT_IDS = {int(x) for x in os.getenv('ROOT_IDS', '').split(',') if x.strip().isdigit()}

ADMIN_IDS |= ROOT_IDS                  # root 同时拥有管理员权限

SUPPORT_CONTACT = os.getenv('SUPPORT_CONTACT', '@Ghost_Mecc').strip()

PURCHASE_ENTRY = os.getenv('PURCHASE_ENTRY', 'https://t.me/CloudMeeting_bot').strip()

JOIN_CODE_EXPIRE_HOURS = int(os.getenv('JOIN_CODE_EXPIRE_HOURS', '72'))

AGENT_CODE_EXPIRE_HOURS = int(os.getenv('AGENT_CODE_EXPIRE_HOURS', str(24 * 365 * 3)))

AGENT_CODE_MAX_USES = int(os.getenv('AGENT_CODE_MAX_USES', '999999'))

TRON_WALLET = os.getenv('TRON_WALLET', 'TBuJoMsi8JnMVwHD1xLqfjXV5zvonpQPNT').strip()

TRON_BACKUP = os.getenv('TRON_BACKUP', 'TDzwjibk274qbT6iVqZEi4cVWu7AuFe4Xy').strip()

TRONGRID_API_KEY = os.getenv('TRONGRID_API_KEY', '').strip()

TRONGRID_URL = os.getenv('TRONGRID_URL', 'https://api.trongrid.io').strip()

PURCHASE_ORDER_TIMEOUT_MIN = int(os.getenv('PURCHASE_ORDER_TIMEOUT_MIN', '10'))

MEET_API_URL = os.getenv('MEET_API_URL', 'https://api-small-breeze-1910.fly.dev').strip()

BOT_API_KEY = os.getenv('BOT_API_KEY', '').strip()

DATABASE_URL = os.getenv('DATABASE_URL', '').strip()



BTN_JOIN_DIST = '📢 云际会议资讯'

BTN_BUY_AUTH = '🛒 预授权码购买'

BTN_DIST_QUERY = '📋 用户ID查询'

BTN_HELP = '📘 平台使用说明'

BTN_JOIN_AGENT = '📢 咨询官方客服'



USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'



# 授权码未被首次绑定时使用的占位过期时间（实际由首次绑定触发计时）

_SENTINEL_EXPIRES = '9999-12-31T00:00:00'



# 提前定义钱包地址获取函数，避免执行顺序问题

def _get_tron_wallet_main() -> str:

    return db.get_setting('wallet_main', TRON_WALLET)



def _get_tron_wallet_backup() -> str:

    return db.get_setting('wallet_backup', TRON_BACKUP)





class DB:

    def _conn(self):

        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

        return conn



    def __init__(self):

        self._init()



    def _init(self):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('''

                CREATE TABLE IF NOT EXISTS join_codes (

                    code TEXT PRIMARY KEY,

                    created_at TEXT NOT NULL,

                    expires_at TEXT NOT NULL,

                    duration_hours INTEGER NOT NULL DEFAULT 72,

                    max_uses INTEGER NOT NULL DEFAULT 1,

                    used_count INTEGER NOT NULL DEFAULT 0,

                    status TEXT NOT NULL DEFAULT 'active',

                    issuer_telegram_id BIGINT

                )

            ''')

            cur.execute('''

                CREATE TABLE IF NOT EXISTS agents (

                    telegram_id BIGINT PRIMARY KEY,

                    username TEXT,

                    first_name TEXT,

                    joined_at TEXT NOT NULL,

                    join_code TEXT NOT NULL,

                    parent_telegram_id BIGINT,

                    invite_code TEXT,

                    bot_token TEXT,

                    forced_level TEXT,

                    local_db_path TEXT,

                    bot_instance TEXT

                )

            ''')

            cur.execute('''

                CREATE TABLE IF NOT EXISTS purchase_orders (

                    order_id BIGSERIAL PRIMARY KEY,

                    buyer_telegram_id BIGINT NOT NULL,

                    agent_level TEXT,

                    code_count INTEGER NOT NULL,

                    unit_price REAL NOT NULL,

                    usdt_amount REAL NOT NULL,

                    status TEXT NOT NULL DEFAULT 'pending',

                    txid TEXT,

                    created_at TEXT NOT NULL,

                    completed_at TEXT

                )

            ''')

            cur.execute('''

                CREATE TABLE IF NOT EXISTS system_settings (

                    key TEXT PRIMARY KEY,

                    value TEXT NOT NULL,

                    updated_at TEXT NOT NULL

                )

            ''')

            cur.execute('''

                CREATE TABLE IF NOT EXISTS admin_accounts (

                    admin_id BIGINT PRIMARY KEY,

                    added_by BIGINT,

                    added_at TEXT NOT NULL,

                    level INTEGER NOT NULL DEFAULT 1

                )

            ''')

            # 兼容旧表：已存在时补 level 列

            cur.execute('''

                ALTER TABLE admin_accounts ADD COLUMN IF NOT EXISTS level INTEGER NOT NULL DEFAULT 1

            ''')

            cur.execute('''

                CREATE TABLE IF NOT EXISTS admin_denied_perms (

                    admin_id BIGINT NOT NULL,

                    perm TEXT NOT NULL,

                    PRIMARY KEY (admin_id, perm)

                )

            ''')

            cur.execute('''

                CREATE TABLE IF NOT EXISTS buy_packages (

                    id BIGSERIAL PRIMARY KEY,

                    code_count INTEGER NOT NULL,

                    total_price REAL NOT NULL,

                    sort_order INTEGER NOT NULL DEFAULT 0,

                    enabled INTEGER NOT NULL DEFAULT 1

                )

            ''')

            cur.execute("ALTER TABLE agents ADD COLUMN IF NOT EXISTS bot_instance TEXT")

            cur.execute('''

                CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_invite_code ON agents(invite_code)

            ''')

            cur.execute('''

                CREATE INDEX IF NOT EXISTS idx_agents_parent ON agents(parent_telegram_id)

            ''')

            cur.execute('''

                CREATE INDEX IF NOT EXISTS idx_purchase_orders_status ON purchase_orders(status)

            ''')

            conn.commit()

        except Exception as e:

            logger.error(f"数据库初始化失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def _generate_code(self) -> str:

        return 'K' + secrets.token_hex(4).upper()



    def create_join_code(self, hours: int, max_uses: int = 1, issuer_telegram_id: int | None = None) -> str:

        now = datetime.now()

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            code = self._generate_code()

            cur.execute('SELECT 1 FROM join_codes WHERE code = %s', (code,))

            while cur.fetchone():

                code = self._generate_code()

                cur.execute('SELECT 1 FROM join_codes WHERE code = %s', (code,))

            cur.execute(

                '''

                INSERT INTO join_codes(code, created_at, expires_at, duration_hours, max_uses, used_count, status, issuer_telegram_id)

                VALUES (%s, %s, %s, %s, %s, 0, %s, %s)

                ''',

                (code, now.isoformat(), _SENTINEL_EXPIRES, hours, max_uses, 'active', issuer_telegram_id),

            )

            conn.commit()

            return code

        except Exception as e:

            logger.error(f"创建加入码失败: {e}")

            if conn:

                conn.rollback()

            raise

        finally:

            if conn:

                conn.close()



    def verify_and_use_join_code(self, code: str) -> tuple[bool, str, int | None]:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM join_codes WHERE code = %s', (code,))

            row = cur.fetchone()

            if not row:

                return False, '加入码不存在', None

            if row['status'] != 'active':

                return False, '加入码不可用', None

            is_sentinel = row['expires_at'] == _SENTINEL_EXPIRES

            if not is_sentinel and datetime.now() > datetime.fromisoformat(row['expires_at']):

                return False, '加入码已过期', None

            if row['used_count'] >= row['max_uses']:

                return False, '加入码已达使用上限', None

            now = datetime.now()

            if is_sentinel and row['used_count'] == 0:

                expires = now + timedelta(hours=row['duration_hours'])

                cur.execute(

                    'UPDATE join_codes SET used_count = used_count + 1, expires_at = %s WHERE code = %s',

                    (expires.isoformat(), code),

                )

            else:

                cur.execute(

                    'UPDATE join_codes SET used_count = used_count + 1 WHERE code = %s',

                    (code,),

                )

            conn.commit()

            return True, 'ok', row['issuer_telegram_id']

        except Exception as e:

            logger.error(f"验证加入码失败: {e}")

            if conn:

                conn.rollback()

            return False, f'系统错误: {str(e)[:50]}', None

        finally:

            if conn:

                conn.close()



    def peek_join_code(self, code: str) -> tuple[bool, str]:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM join_codes WHERE code = %s', (code,))

            row = cur.fetchone()

            if not row:

                return False, '加入码不存在'

            if row['status'] != 'active':

                return False, '加入码不可用'

            if row['expires_at'] != _SENTINEL_EXPIRES and datetime.now() > datetime.fromisoformat(row['expires_at']):

                return False, '加入码已过期'

            if row['used_count'] >= row['max_uses']:

                return False, '加入码已达使用上限'

            return True, 'ok'

        except Exception as e:

            logger.error(f"查询加入码状态失败: {e}")

            return False, f'系统错误: {str(e)[:50]}'

        finally:

            if conn:

                conn.close()



    def ensure_agent_invite_code(self, telegram_id: int) -> str:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT invite_code FROM agents WHERE telegram_id = %s', (telegram_id,))

            row = cur.fetchone()

            if row and row['invite_code']:

                code = row['invite_code']

                cur.execute('SELECT issuer_telegram_id FROM join_codes WHERE code = %s', (code,))

                code_row = cur.fetchone()

                if code_row and code_row['issuer_telegram_id'] == telegram_id:

                    return code

        except Exception as e:

            logger.error(f"查询代理邀请码失败: {e}")

        finally:

            if conn:

                conn.close()

        

        # 创建新的邀请码

        code = self.create_join_code(

            hours=AGENT_CODE_EXPIRE_HOURS,

            max_uses=AGENT_CODE_MAX_USES,

            issuer_telegram_id=telegram_id,

        )

        conn2 = None

        try:

            conn2 = self._conn()

            cur2 = conn2.cursor()

            cur2.execute('UPDATE agents SET invite_code = %s WHERE telegram_id = %s', (code, telegram_id))

            conn2.commit()

        except Exception as e:

            logger.error(f"更新代理邀请码失败: {e}")

            if conn2:

                conn2.rollback()

        finally:

            if conn2:

                conn2.close()

        return code



    def bind_agent(self, telegram_id: int, username: str, first_name: str, code: str, parent_telegram_id: int | None, bot_token: str):

        now = datetime.now().isoformat()

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                '''

                INSERT INTO agents(telegram_id, username, first_name, joined_at, join_code, parent_telegram_id, bot_token)

                VALUES(%s, %s, %s, %s, %s, %s, %s)

                ON CONFLICT(telegram_id) DO UPDATE SET

                    username=EXCLUDED.username,

                    first_name=EXCLUDED.first_name,

                    joined_at=EXCLUDED.joined_at,

                    join_code=EXCLUDED.join_code,

                    parent_telegram_id=EXCLUDED.parent_telegram_id,

                    bot_token=EXCLUDED.bot_token

                ''',

                (telegram_id, username, first_name, now, code, parent_telegram_id, bot_token),

            )

            conn.commit()

        except Exception as e:

            logger.error(f"绑定代理失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def get_agent(self, telegram_id: int):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM agents WHERE telegram_id = %s', (telegram_id,))

            return cur.fetchone()

        except Exception as e:

            logger.error(f"获取代理信息失败: {e}")

            return None

        finally:

            if conn:

                conn.close()



    def find_agent_by_bot_token(self, bot_token: str):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM agents WHERE bot_token = %s LIMIT 1', (bot_token,))

            return cur.fetchone()

        except Exception as e:

            logger.error(f"通过Token查找代理失败: {e}")

            return None

        finally:

            if conn:

                conn.close()



    def update_agent_bot_token(self, telegram_id: int, bot_token: str):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('UPDATE agents SET bot_token = %s WHERE telegram_id = %s', (bot_token, telegram_id))

            conn.commit()

        except Exception as e:

            logger.error(f"更新代理Token失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def push_codes_to_agent_db(self, buyer_telegram_id: int, codes: list) -> int:
        """将授权码写入代理机器人的 auth_code_pool_{instance} 表"""
        conn = None
        try:
            agent = self.get_agent(buyer_telegram_id)
            if not agent:
                return 0
            instance = agent.get('bot_instance') or ''
            if not instance:
                return 0
            tbl = f'auth_code_pool_{instance}'
            conn = self._conn()
            cur = conn.cursor()
            pushed = 0
            for code in codes:
                try:
                    cur.execute(
                        f'INSERT INTO {tbl} (code, status, note, added_at) '
                        f"VALUES (%s, 'available', '管理员下发', TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')) "
                        f'ON CONFLICT(code) DO NOTHING',
                        (code,),
                    )
                    pushed += 1
                except Exception:
                    pass
            conn.commit()
            return pushed
        except Exception as e:
            logger.error(f'写入代理DB失败: {e}')
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()

    def set_agent_bot_instance(self, telegram_id: int, bot_instance: str):
        """设置代理的 bot_instance"""
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('UPDATE agents SET bot_instance = %s WHERE telegram_id = %s', (bot_instance, telegram_id))
            conn.commit()
        except Exception as e:
            logger.error(f'设置bot_instance失败: {e}')
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def create_agent_table(self, instance: str):
        """为代理机器人自动创建 auth_code_pool_{instance} 和 users_{instance} 表"""
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            tbl_codes = f'auth_code_pool_{instance}'
            tbl_users = f'users_{instance}'
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {tbl_codes} (
                    pool_id     SERIAL PRIMARY KEY,
                    code        TEXT UNIQUE NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'available',
                    assigned_to BIGINT DEFAULT NULL,
                    assigned_at TEXT DEFAULT NULL,
                    note        TEXT DEFAULT '',
                    added_at    TEXT NOT NULL DEFAULT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS')
                )
            ''')
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {tbl_users} (
                    telegram_id BIGINT PRIMARY KEY,
                    username    TEXT,
                    first_name  TEXT,
                    first_seen  TEXT NOT NULL,
                    role        TEXT DEFAULT NULL
                )
            ''')
            conn.commit()
            logger.info(f'已创建/确认表: {tbl_codes}, {tbl_users}')
        except Exception as e:
            logger.error(f'创建代理表失败: {e}')
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def count_agent_codes(self, instance: str) -> dict:
        """查询某个代理机器人的授权码统计（直接读表）"""
        conn = None
        try:
            tbl = f'auth_code_pool_{instance}'
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(f'SELECT COUNT(*) AS cnt FROM {tbl}')
            total = cur.fetchone()['cnt']
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE status = 'available'")
            avail = cur.fetchone()['cnt']
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE status = 'assigned'")
            assigned = cur.fetchone()['cnt']
            return {'total': total, 'available': avail, 'assigned': assigned}
        except Exception:
            return {'total': 0, 'available': 0, 'assigned': 0}
        finally:
            if conn:
                conn.close()

    def count_all_agent_codes(self) -> dict:
        """统计所有代理机器人的授权码（遍历agents表查每个instance）"""
        agents = self.list_all_agents(limit=200)
        result = {}
        for a in agents:
            inst = a.get('bot_instance') or ''
            if inst:
                result[a['telegram_id']] = self.count_agent_codes(inst)
        return result

    def list_agent_codes(self, instance: str, limit: int = 200) -> list[dict]:
        """列出某个代理机器人的所有授权码（从本地 auth_code_pool_{instance} 表读取）"""
        conn = None
        try:
            tbl = f'auth_code_pool_{instance}'
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(f'SELECT code, status, note, added_at FROM {tbl} ORDER BY added_at DESC LIMIT %s', (limit,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            if conn:
                conn.close()

    def record_auth_code(self, code: str, owner_telegram_id: int, expires_minutes: int = 1440, note: str = '', order_id: int | None = None):
        """将生成的授权码记录到本地数据库"""
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO auth_codes (code, owner_telegram_id, expires_minutes, note, order_id, created_at) '
                'VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
                (code, owner_telegram_id, expires_minutes, note, order_id, datetime.now().isoformat()),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"记录授权码到本地失败: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def get_auth_codes_by_owner(self, owner_telegram_id: int, limit: int = 200) -> list:
        """查询某代理的所有授权码记录"""
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                'SELECT * FROM auth_codes WHERE owner_telegram_id = %s ORDER BY created_at DESC LIMIT %s',
                (owner_telegram_id, limit),
            )
            return cur.fetchall()
        except Exception as e:
            logger.error(f"查询授权码记录失败: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_all_auth_codes(self, limit: int = 200) -> list:
        """查询所有授权码记录"""
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT * FROM auth_codes ORDER BY created_at DESC LIMIT %s', (limit,))
            return cur.fetchall()
        except Exception as e:
            logger.error(f"查询全部授权码记录失败: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_auth_code_stats(self) -> dict:
        """全平台授权码统计"""
        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) as total FROM auth_codes')
            total = cur.fetchone()['total']
            cur.execute('SELECT owner_telegram_id, COUNT(*) as cnt FROM auth_codes GROUP BY owner_telegram_id ORDER BY cnt DESC')
            by_agent = cur.fetchall()
            return {'total': total, 'by_agent': by_agent}
        except Exception as e:
            logger.error(f"统计授权码失败: {e}")
            return {'total': 0, 'by_agent': []}
        finally:
            if conn:
                conn.close()



    def get_parent_agent(self, telegram_id: int):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM agents WHERE telegram_id = %s', (telegram_id,))

            return cur.fetchone()

        except Exception as e:

            logger.error(f"获取上级代理失败: {e}")

            return None

        finally:

            if conn:

                conn.close()



    def list_all_agents(self, limit: int = 50):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM agents ORDER BY joined_at DESC LIMIT %s', (limit,))

            return cur.fetchall()

        except Exception as e:

            logger.error(f"列出所有代理失败: {e}")

            return []

        finally:

            if conn:

                conn.close()



    def list_codes(self, limit: int = 20):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM join_codes ORDER BY created_at DESC LIMIT %s', (limit,))

            return cur.fetchall()

        except Exception as e:

            logger.error(f"列出加入码失败: {e}")

            return []

        finally:

            if conn:

                conn.close()



    def add_buy_package(self, code_count: int, total_price: float) -> int:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                'INSERT INTO buy_packages(code_count, total_price, sort_order, enabled) VALUES (%s, %s, %s, 1) RETURNING id',

                (code_count, total_price, code_count),

            )

            row = cur.fetchone()

            conn.commit()

            return int(row['id'])

        except Exception as e:

            logger.error(f"添加购买套餐失败: {e}")

            if conn:

                conn.rollback()

            return -1

        finally:

            if conn:

                conn.close()



    def list_buy_packages(self, only_enabled: bool = True):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            sql = 'SELECT * FROM buy_packages'

            if only_enabled:

                sql += ' WHERE enabled=1'

            sql += ' ORDER BY sort_order ASC, code_count ASC'

            cur.execute(sql)

            return cur.fetchall()

        except Exception as e:

            logger.error(f"列出购买套餐失败: {e}")

            return []

        finally:

            if conn:

                conn.close()



    def delete_buy_package(self, pkg_id: int) -> bool:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('DELETE FROM buy_packages WHERE id=%s', (pkg_id,))

            conn.commit()

            return cur.rowcount > 0

        except Exception as e:

            logger.error(f"删除购买套餐失败: {e}")

            if conn:

                conn.rollback()

            return False

        finally:

            if conn:

                conn.close()



    def get_buy_package(self, pkg_id: int):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM buy_packages WHERE id=%s', (pkg_id,))

            return cur.fetchone()

        except Exception as e:

            logger.error(f"获取套餐信息失败: {e}")

            return None

        finally:

            if conn:

                conn.close()



    def create_purchase_order(self, buyer_telegram_id: int, code_count: int, unit_price: float, usdt_amount: float) -> int:

        now = datetime.now().isoformat()

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                '''

                INSERT INTO purchase_orders(buyer_telegram_id, code_count, unit_price, usdt_amount, status, created_at)

                VALUES (%s, %s, %s, %s, 'pending', %s) RETURNING order_id

                ''',

                (buyer_telegram_id, code_count, unit_price, usdt_amount, now),

            )

            row = cur.fetchone()

            conn.commit()

            return int(row['order_id'])

        except Exception as e:

            logger.error(f"创建采购订单失败: {e}")

            if conn:

                conn.rollback()

            return -1

        finally:

            if conn:

                conn.close()



    def get_pending_purchase_orders(self):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute("SELECT * FROM purchase_orders WHERE status='pending' ORDER BY order_id ASC")

            return cur.fetchall()

        except Exception as e:

            logger.error(f"获取待处理订单失败: {e}")

            return []

        finally:

            if conn:

                conn.close()



    def get_user_pending_purchase_order(self, buyer_telegram_id: int):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                "SELECT * FROM purchase_orders WHERE buyer_telegram_id = %s AND status='pending' ORDER BY order_id DESC LIMIT 1",

                (buyer_telegram_id,),

            )

            return cur.fetchone()

        except Exception as e:

            logger.error(f"获取用户待处理订单失败: {e}")

            return None

        finally:

            if conn:

                conn.close()



    def get_user_purchase_stats(self, buyer_telegram_id: int) -> dict:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                "SELECT COALESCE(SUM(code_count),0) AS total_count, COALESCE(MAX(code_count),0) AS max_single "

                "FROM purchase_orders WHERE buyer_telegram_id = %s AND status='completed'",

                (buyer_telegram_id,),

            )

            row = cur.fetchone()

            return {

                'total_count': int(row['total_count']) if row else 0,

                'max_single': int(row['max_single']) if row else 0,

            }

        except Exception as e:

            logger.error(f"获取用户采购统计失败: {e}")

            return {'total_count': 0, 'max_single': 0}

        finally:

            if conn:

                conn.close()



    def complete_purchase_order(self, order_id: int, txid: str):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                "UPDATE purchase_orders SET status='completed', txid=%s, completed_at=%s WHERE order_id=%s",

                (txid, datetime.now().isoformat(), order_id),

            )

            conn.commit()

        except Exception as e:

            logger.error(f"完成采购订单失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def expire_purchase_orders(self, timeout_min: int = 10) -> list[dict]:

        cutoff = (datetime.now() - timedelta(minutes=timeout_min)).isoformat()

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                "SELECT order_id, buyer_telegram_id, code_count, usdt_amount FROM purchase_orders WHERE status='pending' AND created_at < %s",

                (cutoff,),

            )

            rows = cur.fetchall()

            if not rows:

                return []

            cur.execute(

                "UPDATE purchase_orders SET status='expired' WHERE status='pending' AND created_at < %s",

                (cutoff,),

            )

            conn.commit()

            return [dict(r) for r in rows]

        except Exception as e:

            logger.error(f"过期采购订单失败: {e}")

            if conn:

                conn.rollback()

            return []

        finally:

            if conn:

                conn.close()



    def set_setting(self, key: str, value: str):

        now = datetime.now().isoformat()

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                '''

                INSERT INTO system_settings(key, value, updated_at)

                VALUES (%s, %s, %s)

                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at

                ''',

                (key, value, now),

            )

            conn.commit()

        except Exception as e:

            logger.error(f"设置系统参数失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def get_setting(self, key: str, default: str = '') -> str:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT value FROM system_settings WHERE key=%s', (key,))

            row = cur.fetchone()

            return (row['value'] if row else default) or default

        except Exception as e:

            logger.error(f"获取系统参数失败: {e}")

            return default

        finally:

            if conn:

                conn.close()



    def add_admin(self, admin_id: int, added_by: int, level: int = 1):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                'INSERT INTO admin_accounts(admin_id, added_by, added_at, level) VALUES (%s, %s, %s, %s) '

                'ON CONFLICT(admin_id) DO UPDATE SET added_by=EXCLUDED.added_by, added_at=EXCLUDED.added_at, level=EXCLUDED.level',

                (admin_id, added_by, datetime.now().isoformat(), level),

            )

            conn.commit()

        except Exception as e:

            logger.error(f"添加管理员失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def remove_admin(self, admin_id: int):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('DELETE FROM admin_accounts WHERE admin_id = %s', (admin_id,))

            conn.commit()

            return cur.rowcount > 0

        except Exception as e:

            logger.error(f"删除管理员失败: {e}")

            if conn:

                conn.rollback()

            return False

        finally:

            if conn:

                conn.close()



    def list_extra_admin_ids(self) -> set[int]:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT admin_id FROM admin_accounts')

            return {int(r['admin_id']) for r in cur.fetchall()}

        except Exception as e:

            logger.error(f"列出额外管理员失败: {e}")

            return set()

        finally:

            if conn:

                conn.close()



    def is_extra_admin(self, admin_id: int) -> bool:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT 1 FROM admin_accounts WHERE admin_id = %s LIMIT 1', (admin_id,))

            return bool(cur.fetchone())

        except Exception as e:

            logger.error(f"检查管理员权限失败: {e}")

            return False

        finally:

            if conn:

                conn.close()



    def get_admin_level(self, admin_id: int) -> int:

        """返回管理员级别：1=一级，2=二级，0=不是管理员"""

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT level FROM admin_accounts WHERE admin_id = %s LIMIT 1', (admin_id,))

            row = cur.fetchone()

            return int(row['level']) if row else 0

        except Exception as e:

            logger.error(f"获取管理员级别失败: {e}")

            return 0

        finally:

            if conn:

                conn.close()



    def is_supervisor_admin(self, admin_id: int) -> bool:

        """是否为二级管理"""

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT 1 FROM admin_accounts WHERE admin_id = %s AND level >= 2 LIMIT 1', (admin_id,))

            return bool(cur.fetchone())

        except Exception as e:

            logger.error(f"检查二级管理员失败: {e}")

            return False

        finally:

            if conn:

                conn.close()



    def list_supervisor_ids(self) -> set[int]:

        """返回所有二级管理的 ID 集合"""

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT admin_id FROM admin_accounts WHERE level >= 2')

            return {int(r['admin_id']) for r in cur.fetchall()}

        except Exception as e:

            logger.error(f"列出二级管理员失败: {e}")

            return set()

        finally:

            if conn:

                conn.close()



    def list_extra_admin_ids_by_level(self, level: int = 1) -> set[int]:

        """按级别列出管理员ID，level=0 表示返回所有"""

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            if level == 0:

                cur.execute('SELECT admin_id FROM admin_accounts')

            else:

                cur.execute('SELECT admin_id FROM admin_accounts WHERE level = %s', (level,))

            return {int(r['admin_id']) for r in cur.fetchall()}

        except Exception as e:

            logger.error(f"按级别列出管理员失败: {e}")

            return set()

        finally:

            if conn:

                conn.close()



    def list_join_codes_by_issuer(self, issuer_id: int, limit: int = 50):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                'SELECT * FROM join_codes WHERE issuer_telegram_id = %s ORDER BY created_at DESC LIMIT %s',

                (issuer_id, limit),

            )

            return cur.fetchall()

        except Exception as e:

            logger.error(f"列出管理员创建的加入码失败: {e}")

            return []

        finally:

            if conn:

                conn.close()



    def join_code_issuer_stats(self, issuer_id: int) -> dict:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                'SELECT status, used_count, max_uses FROM join_codes WHERE issuer_telegram_id = %s',

                (issuer_id,),

            )

            rows = cur.fetchall()

            total = len(rows)

            active = sum(1 for r in rows if r['status'] == 'active')

            used = sum(r['used_count'] for r in rows)

            maxuses = sum(r['max_uses'] for r in rows)

            return {'total': total, 'active': active, 'used': used, 'max': maxuses}

        except Exception as e:

            logger.error(f"统计管理员加入码失败: {e}")

            return {'total': 0, 'active': 0, 'used': 0, 'max': 0}

        finally:

            if conn:

                conn.close()



    def overall_join_code_stats(self) -> dict:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT status, used_count, max_uses FROM join_codes')

            rows = cur.fetchall()

            total = len(rows)

            active = sum(1 for r in rows if r['status'] == 'active')

            used = sum(r['used_count'] for r in rows)

            return {'total': total, 'active': active, 'used': used}

        except Exception as e:

            logger.error(f"统计整体加入码失败: {e}")

            return {'total': 0, 'active': 0, 'used': 0}

        finally:

            if conn:

                conn.close()



    def delete_agent(self, telegram_id: int) -> bool:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('DELETE FROM purchase_orders WHERE buyer_telegram_id = %s', (telegram_id,))

            cur.execute('DELETE FROM join_codes WHERE issuer_telegram_id = %s', (telegram_id,))

            cur.execute('DELETE FROM agents WHERE telegram_id = %s', (telegram_id,))

            deleted = cur.rowcount > 0

            conn.commit()

            return deleted

        except Exception as e:

            logger.error(f"删除代理失败: {e}")

            if conn:

                conn.rollback()

            return False

        finally:

            if conn:

                conn.close()



    def get_agent_info(self, telegram_id: int):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT * FROM agents WHERE telegram_id = %s', (telegram_id,))

            return cur.fetchone()

        except Exception as e:

            logger.error(f"获取代理详情失败: {e}")

            return None

        finally:

            if conn:

                conn.close()



    def deny_perm(self, admin_id: int, perm: str):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                'INSERT INTO admin_denied_perms(admin_id, perm) VALUES(%s,%s) ON CONFLICT DO NOTHING',

                (admin_id, perm)

            )

            conn.commit()

        except Exception as e:

            logger.error(f"禁用管理员权限失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def allow_perm(self, admin_id: int, perm: str):

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute(

                'DELETE FROM admin_denied_perms WHERE admin_id=%s AND perm=%s',

                (admin_id, perm)

            )

            conn.commit()

        except Exception as e:

            logger.error(f"启用管理员权限失败: {e}")

            if conn:

                conn.rollback()

        finally:

            if conn:

                conn.close()



    def get_denied_perms(self, admin_id: int) -> set:

        conn = None

        try:

            conn = self._conn()

            cur = conn.cursor()

            cur.execute('SELECT perm FROM admin_denied_perms WHERE admin_id=%s', (admin_id,))

            return {r['perm'] for r in cur.fetchall()}

        except Exception as e:

            logger.error(f"获取禁用权限失败: {e}")

            return set()

        finally:

            if conn:

                conn.close()



db = DB()

_processed_txids: set[str] = set()

_monitor_task: asyncio.Task | None = None

_app_bot = None  # 提前初始化，避免引用错误





async def _root_silent_notify(operator_id: int, action_label: str, detail: str):

    """静默通知所有 ROOT 及二级管理：某管理员执行了某操作。"""

    if not _app_bot:

        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    text = (

        f'🔔 <b>{action_label}</b>\n\n'

        f'操作人：<code>{operator_id}</code>\n'

        f'{detail}\n'

        f'时间：{now_str}'

    )

    # ROOT + 二级管理 都收通知

    notify_ids = ROOT_IDS | db.list_supervisor_ids()

    for rid in notify_ids:

        if rid == operator_id:

            continue

        try:

            await _app_bot.send_message(chat_id=rid, text=text, parse_mode='HTML')

        except Exception as e:

            logger.warning(f"通知ROOT失败 (ID:{rid}): {e}")





def _api_headers() -> dict:

    headers = {'Content-Type': 'application/json'}

    if BOT_API_KEY:

        headers['X-API-Secret'] = BOT_API_KEY

    return headers





async def _create_remote_auth_code(owner_telegram_id: int, expires_minutes: int = 1440, note: str = '') -> str | None:

    """创建授权码，成功返回码字符串，失败返回 None"""

    try:

        url = f"{MEET_API_URL}/codes/create"

        payload = {

            'count': 1,

            'expire_minutes': expires_minutes,

            'assigned_to': owner_telegram_id,

            'note': note or '总代机器人采购',

        }

        async with aiohttp.ClientSession() as session:

            async with session.post(url, json=payload, headers=_api_headers(), timeout=aiohttp.ClientTimeout(total=15)) as resp:

                if resp.status == 200:

                    data = await resp.json()

                    codes = data.get('codes', [])

                    code = codes[0]['code'] if codes else None

                    if code:

                        db.record_auth_code(code, owner_telegram_id, expires_minutes, note)

                    return code

                logger.error(f"创建远程授权码失败: HTTP {resp.status}")

                return None

    except Exception as e:

        logger.error(f"创建远程授权码异常: {e}")

        return None





async def _delete_remote_auth_codes(telegram_id: int, count: int) -> tuple[int, str]:

    """删除某代理的指定数量未使用授权码，返回 (deleted, 剩余可用)"""

    codes = await _get_remote_code_list(telegram_id)

    available = [c for c in codes if not c.get('in_use') and c.get('status') not in ('expired', 'used')]

    to_delete = available[:count]

    if not to_delete:

        return 0, '0'

    code_strs = [c.get('code') for c in to_delete if c.get('code')]

    deleted = 0

    if code_strs:

        try:

            async with aiohttp.ClientSession() as session:

                async with session.delete(

                    f'{MEET_API_URL}/codes',

                    json={'codes': code_strs},

                    headers=_api_headers(),

                    timeout=aiohttp.ClientTimeout(total=15),

                ) as resp:

                    if resp.status == 200:

                        data = await resp.json()

                        deleted = data.get('deleted', 0)

                    else:

                        logger.error(f"批量删除授权码失败: HTTP {resp.status}")

        except Exception as e:

            logger.error(f"批量删除授权码异常: {e}")

    remaining = len(available) - deleted

    return deleted, str(max(remaining, 0))





async def _get_remote_code_stats(telegram_id: int) -> tuple[int, int]:

    """查询 API 中某代理的授权码总数和可用数"""

    codes = await _get_remote_code_list(telegram_id)

    total = len(codes)

    avail = sum(1 for c in codes if not c.get('in_use') and c.get('status') in ('available', 'assigned', None, ''))

    if total == 0:

        avail = 0

    return total, avail





async def _get_remote_code_list(telegram_id: int) -> list[dict]:

    """从 API 拉取某 telegramId 下的所有授权码及状态"""

    try:

        async with aiohttp.ClientSession() as session:

            async with session.get(

                f'{MEET_API_URL}/codes',

                params={'assigned_to': str(telegram_id), 'limit': '500'},

                headers=_api_headers(),

                timeout=aiohttp.ClientTimeout(total=15),

            ) as resp:

                if resp.status == 200:

                    data = await resp.json()

                    return data.get('codes', [])

                logger.error(f"获取远程授权码列表失败: HTTP {resp.status}")

    except Exception as e:

        logger.error(f"获取远程授权码列表异常: {e}")

    return []





async def _get_all_remote_codes() -> list[dict]:

    """从 API 拉取全部授权码"""

    try:

        async with aiohttp.ClientSession() as session:

            async with session.get(

                f'{MEET_API_URL}/codes',

                params={'limit': '500'},

                headers=_api_headers(),

                timeout=aiohttp.ClientTimeout(total=15),

            ) as resp:

                if resp.status == 200:

                    data = await resp.json()

                    return data.get('codes', [])

                logger.error(f"获取全部授权码失败: HTTP {resp.status}")

    except Exception as e:

        logger.error(f"获取全部授权码异常: {e}")

    return []





def _classify_codes(codes: list[dict]) -> tuple[int, int, int]:

    """对码列表分类，返回 (使用中, 未使用, 已过期)"""

    now = datetime.now().astimezone()

    in_use = idle = expired = 0

    for c in codes:

        ea = c.get('expires_at') or ''

        is_exp = False

        if ea:

            try:

                exp = datetime.fromisoformat(str(ea).replace('Z', '+00:00'))

                if exp <= now:

                    expired += 1

                    is_exp = True

            except Exception as e:

                logger.warning(f"解析过期时间失败 {ea}: {e}")

                pass

        if not is_exp:

            if int(c.get('in_use') or 0) == 1:

                in_use += 1

            else:

                idle += 1

    return in_use, idle, expired





async def _fulfill_purchase_order(order_row: dict) -> tuple[bool, int, list[str]]:

    ok_count = 0

    codes: list[str] = []

    buyer_id = int(order_row['buyer_telegram_id'])

    code_count = int(order_row['code_count'])

    for _ in range(code_count):

        code = await _create_remote_auth_code(owner_telegram_id=buyer_id, expires_minutes=1440, note='总代采购入库')

        if code:

            ok_count += 1

            codes.append(code)

        else:

            logger.error(f"采购订单#{order_row['order_id']} 授权码生成失败 (第{ok_count+1}个)")

    # 写入代理的 auth_code_pool_{instance} 表，供查询用
    if codes:
        db.push_codes_to_agent_db(buyer_id, codes)

    return ok_count == code_count, ok_count, codes





async def _fetch_trc20_transfers(address: str, limit: int = 20) -> list[dict]:

    url = f"{TRONGRID_URL}/v1/accounts/{address}/transactions/trc20"

    params = {

        'only_to': 'true',

        'limit': limit,

        'order_by': 'block_timestamp,desc',

        'contract_address': USDT_CONTRACT,

    }

    headers = {}

    if TRONGRID_API_KEY:

        headers['TRON-PRO-API-KEY'] = TRONGRID_API_KEY

    try:

        async with aiohttp.ClientSession() as session:

            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:

                if resp.status != 200:

                    logger.error(f"获取TRC20转账失败: HTTP {resp.status}")

                    return []

                data = await resp.json()

                return data.get('data', [])

    except Exception as e:

        logger.error(f"获取TRC20转账异常: {e}")

        return []





async def _match_purchase_orders_from_wallet(address: str):

    if not address:

        return

    transfers = await _fetch_trc20_transfers(address, limit=20)

    pending_rows = db.get_pending_purchase_orders()

    if not pending_rows:

        return



    for tx in transfers:

        txid = tx.get('transaction_id', '')

        if not txid or txid in _processed_txids:

            continue

        _processed_txids.add(txid)



        token_info = tx.get('token_info', {})

        if token_info.get('address') != USDT_CONTRACT:

            continue

        if (tx.get('to') or '').lower() != address.lower():

            continue



        decimals = int(token_info.get('decimals', 6))

        raw_value = int(tx.get('value', '0'))

        amount = raw_value / (10 ** decimals)



        matched = None

        for row in pending_rows:

            if abs(float(row['usdt_amount']) - amount) < 0.00005:

                matched = row

                break

        if not matched:

            continue



        full_ok, ok_count, codes = await _fulfill_purchase_order(matched)

        if full_ok:

            db.complete_purchase_order(int(matched['order_id']), txid)

            buyer_id = int(matched['buyer_telegram_id'])

            # 直接推码到代理克隆机器人本地DB

            pushed = db.push_codes_to_agent_db(buyer_id, codes)

            try:

                codes_text = '\n'.join(f'<code>{c}</code>' for c in codes)

                if pushed == ok_count:

                    push_note = f'✅ 已自动下发到你的机器人（{pushed} 个）'

                else:

                    push_note = ('\n\n📌 <b>请将此消息转发给你的克隆机器人自动入库：</b>\n'

                                 + '\n'.join(f'#YUNJICODE:{c}' for c in codes))

                await _app_bot.send_message(

                    chat_id=buyer_id,

                    text=(

                        f"✅ 支付已确认，授权码已生成\n\n"

                        f"📋 订单号: #{matched['order_id']}\n"

                        f"📦 数量: {ok_count} 个\n"

                        f"💰 金额: {float(matched['usdt_amount']):.4f} USDT\n"

                        f"🔗 交易哈希: <code>{txid[:24]}...</code>\n\n"

                        f"🔑 <b>授权码列表：</b>\n"

                        f"{codes_text}\n\n"

                        f"{push_note}"

                    ),

                    parse_mode='HTML',

                )

            except Exception as e:

                logger.error(f"发送授权码给买家失败 (ID:{buyer_id}): {e}")

            # 通知 root：有充值到账

            root_notify = (

                f'💰 <b>充值到账通知</b>\n\n'

                f'买家Telegram ID：<code>{buyer_id}</code>\n'

                f'订单号：#{matched["order_id"]}\n'

                f'授权码数量：{ok_count} 个\n'

                f'到账金额：{float(matched["usdt_amount"]):.4f} USDT\n'

                f'收款地址：<code>{address}</code>\n'

                f'交易哈希：<code>{txid}</code>\n'

                f'时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'

            )

            for root_id in ROOT_IDS:

                try:

                    await _app_bot.send_message(chat_id=root_id, text=root_notify, parse_mode='HTML')

                except Exception as e:

                    logger.warning(f"通知ROOT到账失败 (ID:{root_id}): {e}")

        else:

            logger.error(f"采购订单#{matched['order_id']} 入库不完整: {ok_count}/{matched['code_count']}")





async def _purchase_monitor_loop():

    """采购订单监控循环"""

    logger.info("采购监控任务已启动")

    while True:

        try:

            # 过期未支付订单

            expired_rows = db.expire_purchase_orders(PURCHASE_ORDER_TIMEOUT_MIN)

            for row in expired_rows:

                try:

                    await _app_bot.send_message(

                        chat_id=int(row['buyer_telegram_id']),

                        text=(

                            f"⌛ 采购订单已过期\n\n"

                            f"📋 订单号: #{row['order_id']}\n"

                            f"📦 数量: {row['code_count']} 个\n"

                            f"💰 应付金额: {float(row['usdt_amount']):.4f} USDT\n\n"

                            f"订单有效期 {PURCHASE_ORDER_TIMEOUT_MIN} 分钟，请重新下单。"

                        ),

                    )

                except Exception as e:

                    logger.warning(f"通知订单过期失败 (ID:{row['buyer_telegram_id']}): {e}")

            

            # 检查钱包到账

            main_wallet = _get_tron_wallet_main()

            backup_wallet = _get_tron_wallet_backup()

            await _match_purchase_orders_from_wallet(main_wallet)

            if backup_wallet and backup_wallet != main_wallet:

                await _match_purchase_orders_from_wallet(backup_wallet)

                

        except Exception as e:

            logger.error(f'采购监听异常: {e}', exc_info=True)

        await asyncio.sleep(20)





async def _post_init(app: Application):

    """应用初始化后执行"""

    global _monitor_task, _app_bot

    _app_bot = app.bot

    if _monitor_task is None or _monitor_task.done():

        _monitor_task = asyncio.create_task(_purchase_monitor_loop())

    logger.info("机器人初始化完成")





async def _post_stop(app: Application):

    """应用停止前执行"""

    global _monitor_task

    logger.info("机器人正在停止...")

    if _monitor_task and not _monitor_task.done():

        _monitor_task.cancel()

        try:

            await _monitor_task

            logger.info("采购监控任务已停止")

        except asyncio.CancelledError:

            logger.info("采购监控任务已取消")

        except Exception as e:

            logger.error(f"停止采购监控任务异常: {e}")





def keyboard(user_id: int | None = None):

    """生成回复键盘"""

    rows = [

        [BTN_BUY_AUTH, BTN_DIST_QUERY],

        [BTN_HELP, BTN_JOIN_AGENT],

        [BTN_JOIN_DIST],

    ]

    return ReplyKeyboardMarkup(

        rows,

        resize_keyboard=True,

        one_time_keyboard=False,

        is_persistent=True,

    )





async def _reply_with_retry(

    update: Update,

    context: ContextTypes.DEFAULT_TYPE,

    text: str,

    parse_mode: str | None = None,

    reply_markup=None,

    retries: int = 1,

):

    """带重试的消息发送"""

    last_err = None

    for _ in range(retries + 1):

        try:

            if update.message:

                await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)

            elif update.effective_chat:

                await context.bot.send_message(

                    chat_id=update.effective_chat.id,

                    text=text,

                    parse_mode=parse_mode,

                    reply_markup=reply_markup,

                )

            return

        except (TimedOut, NetworkError) as err:

            last_err = err

            await asyncio.sleep(0.8)



    if last_err:

        logger.warning(f'send_message重试失败: {last_err}')

        try:

            if update.effective_chat:

                await context.bot.send_message(

                    chat_id=update.effective_chat.id,

                    text='⚠️ 网络波动，消息发送超时，请稍后重试。',

                )

        except Exception:

            pass





def is_admin(user_id: int) -> bool:

    """检查是否为管理员"""

    return user_id in ADMIN_IDS or db.is_extra_admin(user_id)





def is_owner_admin(user_id: int) -> bool:

    """检查是否为ROOT管理员"""

    return user_id in ROOT_IDS





def is_supervisor(user_id: int) -> bool:

    """二级管理：ROOT 或 DB 中 level>=2 的管理员"""

    return user_id in ROOT_IDS or db.is_supervisor_admin(user_id)





def has_perm(user_id: int, perm: str) -> bool:

    """检查管理员权限"""

    if is_supervisor(user_id):

        return True

    if not is_admin(user_id):

        return False

    return perm not in db.get_denied_perms(user_id)





async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    """/start 命令处理"""

    context.user_data['action'] = None

    uid = update.effective_user.id

    text = (

        '☁️ <b>云际会议（官方总）</b>\n\n'

        '本机器人用于分销加入、授权购买分流与分销信息查询。\n\n'

        '请使用下方菜单继续。'

    )

    await update.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard(uid))





# ─── 管理员内联按钮菜单 ────────────────────────────────────────

def _admin_main_menu_kb() -> InlineKeyboardMarkup:

    """管理员主菜单"""

    return InlineKeyboardMarkup([

        [InlineKeyboardButton('🤖 机器人管理', callback_data='adm_cat:agents'),

         InlineKeyboardButton('🎫 授权码管理', callback_data='adm_cat:codes')],

        [InlineKeyboardButton('👑 管理员管理', callback_data='adm_cat:admins'),

         InlineKeyboardButton('📦 套餐管理',   callback_data='adm_cat:packs')],

        [InlineKeyboardButton('💰 收款地址',   callback_data='adm_cat:wallet')],

    ])





def _admin_cat_kb(cat: str) -> InlineKeyboardMarkup | None:

    """管理员分类菜单"""

    cats: dict[str, list] = {

        'agents': [

            [InlineKeyboardButton('➕ 添加出售机器人', callback_data='adm_ask:addagent'),

             InlineKeyboardButton('📋 查看全部机器人', callback_data='adm_do:codes')],

            [InlineKeyboardButton('🗑 删除出售机器人', callback_data='adm_ask:delagent')],

            [InlineKeyboardButton('⬅️ 返回', callback_data='adm_back')],

        ],

        'codes': [

            [InlineKeyboardButton('📤 下发授权码',    callback_data='adm_ask:sendcodes'),

             InlineKeyboardButton('🗑 删除授权码',    callback_data='adm_ask:delcodes')],

            [InlineKeyboardButton('📊 单个机器人授权码', callback_data='adm_ask:agentstats')],

            [InlineKeyboardButton('🔢 全平台授权码统计', callback_data='adm_do:totalcodes')],

            [InlineKeyboardButton('⬅️ 返回', callback_data='adm_back')],

        ],

        'admins': [

            [InlineKeyboardButton('➕ 添加管理员',    callback_data='adm_ask:addadmin'),

             InlineKeyboardButton('🗑 删除管理员',    callback_data='adm_ask:deladmin')],

            [InlineKeyboardButton('📋 管理员列表',    callback_data='adm_do:admins')],

            [InlineKeyboardButton('✏️ 资讯编辑', callback_data='adm_ask:setintro')],

            [InlineKeyboardButton('⬅️ 返回', callback_data='adm_back')],

        ],

        'packs': [

            [InlineKeyboardButton('➕ 添加套餐',   callback_data='adm_ask:addpack'),

             InlineKeyboardButton('📋 查看套餐',   callback_data='adm_do:packs')],

            [InlineKeyboardButton('🗑 删除套餐',   callback_data='adm_ask:delpack'),

             InlineKeyboardButton('✏️ 购买页文案', callback_data='adm_ask:buytext')],

            [InlineKeyboardButton('⬅️ 返回', callback_data='adm_back')],

        ],

        'wallet': [

            [InlineKeyboardButton('🔹 主收款地址',    callback_data='adm_ask:wallet'),

             InlineKeyboardButton('🔸 备用地址',      callback_data='adm_ask:backup')],

            [InlineKeyboardButton('📸 上传主二维码',  callback_data='adm_do:walletqr'),

             InlineKeyboardButton('📸 上传备用二维码',callback_data='adm_do:backupqr')],

            [InlineKeyboardButton('⬅️ 返回', callback_data='adm_back')],

        ],

    }

    rows = cats.get(cat)

    return InlineKeyboardMarkup(rows) if rows else None



_ADM_CAT_NAMES = {

    'agents': '🤖 机器人管理',

    'codes':  '🎫 授权码管理',

    'admins': '👑 管理员管理',

    'packs':  '📦 套餐管理',

    'wallet': '💰 收款地址',

}

# ──────────────────────────────────────────────────────────────





async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    """/help 命令处理"""

    user_id = update.effective_user.id

    if not is_admin(user_id):

        await _reply_with_retry(update, context, '⛔ 使用说明仅管理员可查看',

                                reply_markup=keyboard(user_id), retries=1)

        return

    await _reply_with_retry(

        update, context,

        '👑 <b>管理员操作菜单</b>\n\n请选择操作类别：',

        parse_mode='HTML',

        reply_markup=_admin_main_menu_kb(),

        retries=1,

    )




async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    """/admin 命令处理"""

    user_id = update.effective_user.id

    if not is_admin(user_id):

        await update.message.reply_text('⛔ 权限不足')

        return



    args = context.args or []

    if not args:

        msg = (

            '👑 <b>管理员菜单</b>\n'

            '━━━━━━━━━━━━━━━\n\n'

            '📌 <b>出售机器人管理</b>\n\n'

            '/admin addagent &lt;ID&gt;\n'

            '  └ 直接添加出售机器人\n'

            '/admin codes\n'

            '  └ 查看全部出售机器人\n'

            '/admin delagent &lt;ID&gt;\n'

            '  └ 删除出售机器人\n\n'

            '📌 <b>套餐管理</b>\n\n'

            '/admin addpack &lt;数量&gt; &lt;单价USDT&gt;\n'

            '  └ 添加套餐 例：/admin addpack 20 0.5\n'

            '/admin packs\n'

            '  └ 查看全部套餐\n'

            '/admin delpack &lt;套餐ID&gt;\n'

            '  └ 删除套餐\n'

            '/admin buytext &lt;文案&gt;\n'

            '  └ 设置购买页顶部说明文案\n\n'

            '📌 <b>收款地址</b>\n\n'

            '/admin wallet &lt;TRC20地址&gt;\n'

            '  └ 设置主收款地址\n'

            '/admin backup &lt;TRC20地址&gt;\n'

            '  └ 设置备用收款地址\n'

            '/admin walletqr\n'

            '  └ 上传主钱包二维码图片\n'

            '/admin backupqr\n'

            '  └ 上传备用钱包二维码图片\n'

            '/admin delqr &lt;main|backup&gt;\n'

            '  └ 删除钱包二维码\n\n'

            '📌 <b>权限管理</b>\n\n'

            '/admin addadmin &lt;TelegramID&gt;\n'

            '  └ 添加管理员\n'

            '/admin deladmin &lt;TelegramID&gt;\n'

            '  └ 删除管理员\n'

            '/admin admins\n'

            '  └ 查看管理员列表\n'

            '/admin support @账号\n'

            '  └ 更换客服联系方式\n\n'

            '📌 <b>授权码下发</b>\n\n'

            '/admin sendcodes &lt;代理ID&gt; &lt;数量&gt; &lt;小时&gt;\n'

            '  └ 下发授权码到指定代理机器人 例：/admin sendcodes 123456 10 24\n\n'

            '/admin delcodes &lt;代理ID&gt; &lt;数量&gt;\n'

            '  └ 删除代理机器人可用授权码 例：/admin delcodes 123456 50\n\n'

            '/admin agentcodes &lt;代理ID&gt;\n'

            '  └ 查出售机器人的授权码列表\n\n'

            '/admin agentstats &lt;代理ID&gt;\n'

            '  └ 查出售机器人的授权码使用情况\n\n'

            '/admin totalcodes\n'

            '  └ 查整体授权码数量及使用情况'

        )

        await _reply_with_retry(update, context, msg, parse_mode='HTML', retries=1)

        return



    cmd = args[0].lower()

    if cmd == 'codes':

        await codes_cmd(update, context)



    elif cmd == 'addpack':

        if len(args) < 3:

            await update.message.reply_text('用法：/admin addpack <数量> <单价USDT>\n示例：/admin addpack 20 0.5')

            return

        try:

            cnt = int(args[1])

            unit_price = float(args[2])

            if cnt <= 0 or unit_price <= 0:

                raise ValueError

        except ValueError:

            await update.message.reply_text('❌ 数量和单价必须是大于0的数字')

            return

        

        # 计算总价（数量 * 单价）

        total_price = cnt * unit_price

        # 调用DB方法添加套餐

        pkg_id = db.add_buy_package(cnt, total_price)

        if pkg_id > 0:

            await update.message.reply_text(

                f'✅ 套餐添加成功！\n\n'

                f'套餐ID：{pkg_id}\n'

                f'授权码数量：{cnt} 个\n'

                f'单价：{unit_price:.4f} USDT/个\n'

                f'总价：{total_price:.4f} USDT',

                parse_mode='HTML'

            )

            # 通知ROOT管理员

            await _root_silent_notify(

                user_id,

                '添加购买套餐',

                f'套餐ID：{pkg_id}\n数量：{cnt} 个\n单价：{unit_price} USDT\n总价：{total_price} USDT'

            )

        else:

            await update.message.reply_text('❌ 套餐添加失败，请检查日志或稍后重试')



    # 补全其他常用命令的处理逻辑（保持代码完整性）

    elif cmd == 'packs':

        # 查看所有套餐

        packages = db.list_buy_packages()

        if not packages:

            await update.message.reply_text('📦 暂无可用套餐')

            return

        

        pkg_text = ['📦 <b>当前可用套餐列表</b>\n━━━━━━━━━━━━━━━']

        for pkg in packages:

            pkg_text.append(

                f"\nID：{pkg['id']}\n"

                f"数量：{pkg['code_count']} 个\n"

                f"总价：{pkg['total_price']:.4f} USDT\n"

                f"单价：{pkg['total_price']/pkg['code_count']:.4f} USDT/个"

            )

        await update.message.reply_text('\n'.join(pkg_text), parse_mode='HTML')



    elif cmd == 'delpack':

        # 删除套餐

        if len(args) < 2:

            await update.message.reply_text('用法：/admin delpack <套餐ID>\n示例：/admin delpack 1')

            return

        try:

            pkg_id = int(args[1])

        except ValueError:

            await update.message.reply_text('❌ 套餐ID必须是数字')

            return

        

        success = db.delete_buy_package(pkg_id)

        if success:

            await update.message.reply_text(f'✅ 套餐ID {pkg_id} 删除成功')

            # 通知ROOT管理员

            await _root_silent_notify(

                user_id,

                '删除购买套餐',

                f'套餐ID：{pkg_id}'

            )

        else:

            await update.message.reply_text(f'❌ 套餐ID {pkg_id} 删除失败（可能不存在）')



    elif cmd == 'buytext':

        # 设置购买页文案

        if len(args) < 2:

            await update.message.reply_text('用法：/admin buytext <文案>\n示例：/admin buytext 欢迎购买授权码')

            return

        buy_text = ' '.join(args[1:])

        db.set_setting('buy_page_text', buy_text)

        await update.message.reply_text(f'✅ 购买页文案已更新为：\n{buy_text}')

        await _root_silent_notify(user_id, '修改购买页文案', f'新文案：{buy_text}')



    else:

        # 未知命令提示

        await update.message.reply_text(

            f'❌ 未知命令：{cmd}\n'

            '输入 /admin 查看所有可用命令'

        )



# 补充缺失的 codes_cmd 函数（避免调用报错）

async def codes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    """查看全部出售机器人"""

    agents = db.list_all_agents()

    if not agents:

        await update.message.reply_text('🤖 暂无出售机器人')

        return

    

    agent_text = ['🤖 <b>出售机器人列表</b>\n━━━━━━━━━━━━━━━']

    for idx, agent in enumerate(agents, 1):

        agent_text.append(

            f"\n{idx}. ID：{agent['telegram_id']}\n"

            f"昵称：{agent['first_name'] or '未设置'}\n"

            f"用户名：@{agent['username'] or '未设置'}\n"

            f"加入时间：{agent['joined_at'][:19].replace('T', ' ')}"

        )

    await update.message.reply_text('\n'.join(agent_text[:10]), parse_mode='HTML')  # 限制显示前10个





def _get_buy_copy() -> str:
    new_default = (
        '🛒 <b>预授权码购买</b>\n'
        '━━━━━━━━━━━━━━━\n\n'
        '📋 <b>购买须知</b>\n\n'
        '  1️⃣  购买的授权码自动存入你克隆的机器人\n\n'
        '  2️⃣  授权码从第一次进入会议开始计时\n'
        '        有效时间 <b>12 小时</b>，过期作废\n\n'
        '  3️⃣  授权码 <b>一码一房间</b>\n'
        '        会议结束后可再次开设房间\n\n'
        '━━━━━━━━━━━━━━━\n'
        '📖 <b>使 用 方 法</b>\n'
        '━━━━━━━━━━━━━━━\n\n'
        '🟢 <b>创建会议</b>\n'
        '  👉 输入：<code>授权码 + 房间号</code>\n\n'
        '🔵 <b>加入会议</b>\n'
        '  👉 输入：<code>创建者的授权码 + 创建时的房间号</code>\n'
        '  即可进入同一个房间'
    )
    val = db.get_setting('buy_entry_text', new_default)
    # 自动清除旧版含“代理级别”的默认文案
    if '请选择代理级别' in val:
        return new_default
    return val


async def my_codes_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """代理查询自己的授权码库存"""
    user_id = update.effective_user.id
    await _reply_with_retry(update, context, '⏳ 正在查询授权码…', retries=1)

    try:
        url = f"{MEET_API_URL}/codes"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={'assigned_to': str(user_id), 'limit': '500'}, headers=_api_headers(), timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    await _reply_with_retry(update, context, '❌ 查询失败，请稍后重试', reply_markup=keyboard(), retries=1)
                    return
                data = await resp.json()
                codes = data.get('codes', [])
    except Exception as e:
        await _reply_with_retry(update, context, f'❌ 查询异常：{e}', reply_markup=keyboard(), retries=1)
        return

    if not codes:
        await _reply_with_retry(update, context, '📭 您暂无授权码\n\n请使用「🛒 预授权码购买」购买或联系管理员下发。', reply_markup=keyboard(), retries=1)
        return

    total = len(codes)
    available = 0
    in_use = 0
    expired = 0
    for c in codes:
        expires_at = c.get('expires_at')
        is_in_use = c.get('in_use', False)
        if is_in_use:
            in_use += 1
        elif expires_at and expires_at != '9999-12-31T00:00:00':
            # 有过期时间，检查是否已过期
            try:
                exp_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if exp_dt < datetime.now(exp_dt.tzinfo):
                    expired += 1
                else:
                    available += 1
            except Exception:
                available += 1
        else:
            available += 1  # 未激活 = 可用

    msg = (
        f'📦 <b>我的授权码库存</b>\n'
        f'━━━━━━━━━━━━━━━\n\n'
        f'📊 <b>统计</b>\n'
        f'  总数：<b>{total}</b>\n'
        f'  ✅ 可用：<b>{available}</b>\n'
        f'  🔄 使用中：<b>{in_use}</b>\n'
        f'  ❌ 已过期：<b>{expired}</b>\n\n'
    )

    # 列出可用的码（最多显示30个）
    avail_codes = []
    for c in codes:
        expires_at = c.get('expires_at')
        is_in_use = c.get('in_use', False)
        if is_in_use:
            continue
        if expires_at and expires_at != '9999-12-31T00:00:00':
            try:
                exp_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if exp_dt < datetime.now(exp_dt.tzinfo):
                    continue
            except Exception:
                pass
        avail_codes.append(c.get('code', ''))

    if avail_codes:
        msg += f'🔑 <b>可用授权码</b>（{len(avail_codes)}个）\n'
        for i, code in enumerate(avail_codes[:30], 1):
            msg += f'  {i}. <code>{code}</code>\n'
        if len(avail_codes) > 30:
            msg += f'  … 还有 {len(avail_codes) - 30} 个\n'
    else:
        msg += '🔑 暂无可用授权码\n'

    await _reply_with_retry(update, context, msg, parse_mode='HTML', reply_markup=keyboard(), retries=1)


async def _fetch_agent_code_stats(telegram_id: int) -> dict:
    """查询某代理的授权码统计：总数/可用/使用中/已过期"""
    try:
        url = f"{MEET_API_URL}/codes"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={'assigned_to': str(telegram_id), 'limit': '500'}, headers=_api_headers(), timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
                codes = data.get('codes', [])
    except Exception:
        return {}
    total = len(codes)
    available, in_use, expired = 0, 0, 0
    for c in codes:
        expires_at = c.get('expires_at')
        is_in_use = c.get('in_use', False)
        if is_in_use:
            in_use += 1
        elif expires_at and expires_at != '9999-12-31T00:00:00':
            try:
                exp_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if exp_dt < datetime.now(exp_dt.tzinfo):
                    expired += 1
                else:
                    available += 1
            except Exception:
                available += 1
        else:
            available += 1
    return {'total': total, 'available': available, 'in_use': in_use, 'expired': expired}


async def dist_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员专属：查看所有出售机器人列表"""
    uid = update.effective_user.id
    agents = db.list_all_agents(limit=100)
    if not agents:
        await _reply_with_retry(update, context, '📭 暂无出售机器人', reply_markup=keyboard(uid), retries=1)
        return

    await _reply_with_retry(update, context, '⏳ 查询中…', retries=1)

    all_stats = db.count_all_agent_codes()

    lines = ['🤖 <b>出售机器人列表</b>\n']
    for i, row in enumerate(agents, 1):
        tid = row['telegram_id']
        name = f"@{row['username']}" if row['username'] else (row['first_name'] or str(tid))
        stats = all_stats.get(tid, {'total': 0, 'available': 0, 'assigned': 0})
        total_c = stats['total']
        avail_c = stats['available']
        code_str = f'  🔑 {avail_c}/{total_c}' if total_c else '  🔑 0'
        lines.append(f'{i}. {name} <code>{tid}</code>{code_str}')

    msg = '\n'.join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + '\n…（列表过长，已截断）'
    await _reply_with_retry(update, context, msg, parse_mode='HTML', reply_markup=keyboard(uid), retries=1)


async def buy_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    row = db.get_agent(user.id)
    if not row and not is_admin(user.id):
        await update.message.reply_text(
            '⛔ 仅代理可购买授权码\n\n请先点击「🎉 欢迎入驻云际」完成代理开通。',
            reply_markup=keyboard(),
        )
        return

    pkgs = db.list_buy_packages(only_enabled=True)
    text = _get_buy_copy()
    if not pkgs:
        await update.message.reply_text(
            text + '\n\n⚠️ 暂无可用套餐，请联系管理员添加。',
            parse_mode='HTML', reply_markup=keyboard()
        )
        return

    buttons = []
    for p in pkgs:
        unit = p['total_price'] / p['code_count']
        total = p['total_price']
        label = f"📦 {p['code_count']} 个授权码　·　单价 {unit:.2f} USDT　·　总价 {total:.2f} USDT"
        buttons.append([InlineKeyboardButton(label, callback_data=f"buy_pack_{p['id']}")])
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(text + '\n\n请选择套餐：', parse_mode='HTML', reply_markup=kb)


async def _create_purchase_page(update: Update, context: ContextTypes.DEFAULT_TYPE, count: int, total_price: float, pkg_label: str):
    user = update.effective_user
    pending = db.get_user_pending_purchase_order(user.id)
    if pending:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"⚠️ 您有待支付订单\n\n"
                f"订单号: #{pending['order_id']}\n"
                f"应付: {float(pending['usdt_amount']):.4f} USDT\n"
                f"请先完成或等待超时。"
            ),
        )
        return

    unit_price = round(total_price / count, 6)
    deviation = round(random.uniform(0.0001, 0.0099), 4)
    pay_amount = round(total_price + deviation, 4)
    order_id = db.create_purchase_order(user.id, count, unit_price, pay_amount)

    msg = (
        f"💳 <b>付款页面</b>\n\n"
        f"📋 订单号: #{order_id}\n"
        f"📦 套餐: {pkg_label}\n"
        f"💰 总价: {total_price:.2f} USDT\n"
        f"⭐ 请转账: <b>{pay_amount:.4f} USDT</b>\n\n"
        f"🔹 主钱包:\n<code>{_get_tron_wallet_main()}</code>\n\n"
        f"🔸 备用钱包:\n<code>{_get_tron_wallet_backup()}</code>\n\n"
        f"⏰ 有效期: {PURCHASE_ORDER_TIMEOUT_MIN} 分钟\n"
        f"到账后系统自动入库到您的代理库。"
    )
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', reply_markup=keyboard())

    # 自动生成钱包地址二维码并发送
    try:
        import qrcode
        from io import BytesIO
        from telegram import InputMediaPhoto

        main_addr = _get_tron_wallet_main()
        backup_addr = _get_tron_wallet_backup()

        def _make_qr(data: str) -> BytesIO:
            img = qrcode.make(data, box_size=8, border=2)
            buf = BytesIO()
            img.save(buf, format='PNG')
            buf.seek(0)
            buf.name = 'qr.png'
            return buf

        main_buf = _make_qr(main_addr)
        backup_buf = _make_qr(backup_addr)
        media = [
            InputMediaPhoto(media=main_buf, caption=f'🔹 主钱包二维码\n{main_addr}'),
            InputMediaPhoto(media=backup_buf, caption=f'🔸 备用钱包二维码\n{backup_addr}'),
        ]
        await context.bot.send_media_group(chat_id=chat_id, media=media)
    except Exception as e:
        logger.warning(f'生成钱包二维码失败: {e}')


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ''
    user_id = query.from_user.id

    if data.startswith('buy_pack_'):
        raw = data.replace('buy_pack_', '')
        if not raw.isdigit():
            return
        pkg = db.get_buy_package(int(raw))
        if not pkg:
            await query.message.reply_text('❌ 套餐不存在或已下架，请重新点击购买按钮。')
            return
        unit = pkg['total_price'] / pkg['code_count']
        pkg_label = f"{pkg['code_count']} 个授权码  单价 {unit:.2f} USDT"
        await _create_purchase_page(update, context, int(pkg['code_count']), float(pkg['total_price']), pkg_label)
        return

    # ── 管理员菜单导航 ──────────────────────────────────────
    if not is_admin(user_id):
        return

    if data == 'adm_back':
        await query.edit_message_text(
            '👑 <b>管理员操作菜单</b>\n\n请选择操作类别：',
            parse_mode='HTML',
            reply_markup=_admin_main_menu_kb(),
        )
        return

    if data.startswith('adm_cat:'):
        cat = data[8:]
        kb = _admin_cat_kb(cat)
        if kb is None:
            return
        title = _ADM_CAT_NAMES.get(cat, cat)
        await query.edit_message_text(
            f'👑 <b>{title}</b>\n\n请选择操作：',
            parse_mode='HTML',
            reply_markup=kb,
        )
        return

    if data.startswith('adm_do:'):
        subcmd = data[7:]

        if subcmd == 'codes':
            agents = db.list_all_agents(limit=50)
            if not agents:
                await query.message.reply_text('暂无出售机器人')
                return
            await query.message.reply_text('⏳ 查询中…')
            all_stats = db.count_all_agent_codes()
            msg = '📊 <b>全部出售机器人</b>\n\n'
            for a in agents:
                tid = a['telegram_id']
                inst = a.get('bot_instance') or ''
                uname = f"@{a['username']}" if a['username'] else (a['first_name'] or str(tid))
                st = all_stats.get(tid, {'total': 0, 'available': 0, 'assigned': 0})
                code_str = f'  🔑 可用{st["available"]}/共{st["total"]}'
                inst_str = f'  📦<code>{inst}</code>' if inst else ''
                msg += f'<b>{uname}</b> <code>{tid}</code>{code_str}{inst_str}\n'
            await query.message.reply_text(msg, parse_mode='HTML')

        elif subcmd == 'totalcodes':
            await query.message.reply_text('⏳ 正在统计…')
            agents = db.list_all_agents(limit=200)
            all_stats = db.count_all_agent_codes()
            total_all = sum(s['total'] for s in all_stats.values())
            avail_all = sum(s['available'] for s in all_stats.values())
            assigned_all = sum(s['assigned'] for s in all_stats.values())
            lines = [
                f'📊 <b>整体授权码总览</b>\n',
                f'🔑 总计：{total_all}  🔵可用：{avail_all}  🟢已分配：{assigned_all}\n',
                f'🤖 出售机器人总数：{len(agents)}\n',
                '─' * 20,
            ]
            for a in agents:
                tid = a['telegram_id']
                inst = a.get('bot_instance') or ''
                uname = f"@{a['username']}" if a.get('username') else (a.get('first_name') or str(tid))
                st = all_stats.get(tid, {'total': 0, 'available': 0, 'assigned': 0})
                lines.append(
                    f'🤖 <b>{uname}</b>\n'
                    f'   ID: <code>{tid}</code>\n'
                    f'   📦 实例: <code>{inst}</code>\n'
                    f'   🔑 总计: {st["total"]}  🔵可用: {st["available"]}  🟢已分配: {st["assigned"]}'
                )
            await query.message.reply_text('\n'.join(lines), parse_mode='HTML')

        elif subcmd == 'admins':
            extra_l1 = db.list_extra_admin_ids_by_level(1)
            extra_l2 = db.list_extra_admin_ids_by_level(2)
            all_l1 = sorted((ADMIN_IDS - ROOT_IDS) | extra_l1)
            all_l2 = sorted(extra_l2)
            msg = '👑 <b>管理员列表</b>\n\n'
            if all_l2:
                msg += '🔵 <b>二级管理</b>\n'
                for i in all_l2:
                    name = ''
                    try:
                        chat = await _app_bot.get_chat(i)
                        name = chat.first_name or chat.username or ''
                    except Exception:
                        pass
                    if name:
                        msg += f'• <b>{name}</b> (<code>{i}</code>)\n'
                    else:
                        msg += f'• <code>{i}</code>\n'
                msg += '\n'
            if all_l1:
                msg += '🟡 <b>官方</b>\n'
                for i in all_l1:
                    name = ''
                    try:
                        chat = await _app_bot.get_chat(i)
                        name = chat.first_name or chat.username or ''
                    except Exception:
                        pass
                    if name:
                        msg += f'• <b>{name}</b> (<code>{i}</code>)\n'
                    else:
                        msg += f'• <code>{i}</code>\n'
            if not all_l1 and not all_l2:
                msg += '• 无\n'
            await query.message.reply_text(msg, parse_mode='HTML')

        elif subcmd == 'packs':
            pkgs = db.list_buy_packages(only_enabled=False)
            if not pkgs:
                await query.message.reply_text('暂无套餐')
                return
            lines = ['📦 <b>当前套餐列表</b>\n']
            for p in pkgs:
                st = '✅' if p['enabled'] else '🚫'
                up = p['total_price'] / p['code_count']
                lines.append(f"{st} ID <code>{p['id']}</code>  {p['code_count']}个  单价{up:.2f}  总价<b>{p['total_price']:.2f} USDT</b>")
            await query.message.reply_text('\n'.join(lines), parse_mode='HTML')

        elif subcmd == 'walletqr':
            context.user_data['action'] = 'upload_wallet_qr_main'
            await query.message.reply_text('📸 请发送<b>主钱包二维码</b>图片，发任意文字取消。', parse_mode='HTML')

        elif subcmd == 'backupqr':
            context.user_data['action'] = 'upload_wallet_qr_backup'
            await query.message.reply_text('📸 请发送<b>备用钱包二维码</b>图片，发任意文字取消。', parse_mode='HTML')
        return

    if data.startswith('adm_ask:'):
        subcmd = data[8:]
        prompts = {
            'addagent':   ('wait_adm_addagent',   '➕ 请发送 <b>Bot Token</b>\n例：<code>8562286529:AAGxxx...</code>\n\n• 从 @BotFather 获取\n• 实例名将自动从机器人用户名生成'),
            'delagent':   ('wait_adm_delagent',   '🗑 请发送要删除的出售机器人 <b>Telegram ID</b>：'),
            'sendcodes':  ('wait_adm_sendcodes',  '📤 请发送：<b>代理ID 数量 小时</b>\n例：<code>123456789 10 24</code>'),
            'delcodes':   ('wait_adm_delcodes',   '🗑 请发送：<b>代理ID 数量</b>\n例：<code>123456789 50</code>'),
            'agentcodes': ('wait_adm_agentcodes', '📋 请发送出售机器人 <b>Telegram ID</b>：'),
            'agentstats': ('wait_adm_agentstats', '📊 请发送出售机器人 <b>Telegram ID</b>：'),
            'addadmin':   ('wait_adm_addadmin',   '➕ 请发送要添加的管理员 <b>Telegram ID</b>：'),
            'deladmin':   ('wait_adm_deladmin',   '🗑 请发送要删除的管理员 <b>Telegram ID</b>：'),
            'addpack':    ('wait_adm_addpack',    '➕ 请发送：<b>数量 单价USDT</b>\n例：<code>20 0.5</code>'),
            'delpack':    ('wait_adm_delpack',    '🗑 请发送要删除的<b>套餐 ID</b>：'),
            'buytext':    ('wait_adm_buytext',    '✏️ 请发送购买页说明文案（支持 HTML）：'),
            'setintro':   ('wait_adm_setintro',   '📢 请发送新的<b>云际会议资讯</b>内容：\n\n支持以下格式：\n• <b>纯文本</b>（支持 HTML）\n• <b>图片</b>（可附文字说明）\n• <b>视频</b>（可附文字说明）\n\n发 <code>clear</code> 清空所有资讯内容。'),
            'wallet':     ('wait_adm_wallet',     '🔹 请发送<b>主收款 TRC20 地址</b>：'),
            'backup':     ('wait_adm_backup',     '🔸 请发送<b>备用收款 TRC20 地址</b>：'),
        }
        if subcmd not in prompts:
            return
        action, prompt = prompts[subcmd]
        context.user_data['action'] = action
        await query.message.reply_text(prompt, parse_mode='HTML')
        return


async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    contact = db.get_setting('support_contact', SUPPORT_CONTACT)
    await update.message.reply_text(
        f'📞 官方客服：{contact}',
        reply_markup=keyboard(),
    )


async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理管理员上传图片（钱包二维码 / 云际会议资讯图片）"""
    action = context.user_data.get('action')
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    if action == 'wait_adm_setintro':
        photo = update.message.photo[-1]
        file_id = photo.file_id
        caption = (update.message.caption or '').strip()
        db.set_setting('platform_intro_media_type', 'photo')
        db.set_setting('platform_intro_media_id', file_id)
        db.set_setting('platform_intro', caption)
        context.user_data['action'] = None
        await update.message.reply_text('✅ 云际会议资讯已更新（图片' + ('＋文字说明' if caption else '') + '）', reply_markup=keyboard())
        return

    if action not in ('upload_wallet_qr_main', 'upload_wallet_qr_backup'):
        return
    photo = update.message.photo[-1]  # 取最大尺寸
    file_id = photo.file_id
    context.user_data['action'] = None
    if action == 'upload_wallet_qr_main':
        db.set_setting('wallet_main_qr', file_id)
        await update.message.reply_text('✅ 主钱包二维码已保存！\n付款页面将展示此二维码。', reply_markup=keyboard())
    else:
        db.set_setting('wallet_backup_qr', file_id)
        await update.message.reply_text('✅ 备用钱包二维码已保存！\n付款页面将展示此二维码。', reply_markup=keyboard())


async def on_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理管理员上传视频（云际会议资讯视频）"""
    action = context.user_data.get('action')
    user_id = update.effective_user.id
    if action != 'wait_adm_setintro' or not is_admin(user_id):
        return
    video = update.message.video
    file_id = video.file_id
    caption = (update.message.caption or '').strip()
    db.set_setting('platform_intro_media_type', 'video')
    db.set_setting('platform_intro_media_id', file_id)
    db.set_setting('platform_intro', caption)
    context.user_data['action'] = None
    await update.message.reply_text('✅ 云际会议资讯已更新（视频' + ('＋文字说明' if caption else '') + '）', reply_markup=keyboard())


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or '').strip()
    action = context.user_data.get('action')
    user_id = update.effective_user.id

    if text == BTN_JOIN_AGENT:
        context.user_data['action'] = None
        contact = db.get_setting('support_contact', SUPPORT_CONTACT)
        u = update.effective_user
        uname = f'@{u.username}' if u.username else '未设置'
        await update.message.reply_text(
            f'☁️ <b>云际会议官方客服</b>\n\n'
            f'📞 客服：{contact}\n\n'
            f'🪪 您的ID：<code>{u.id}</code>\n'
            f'👤 用户名：{uname}\n\n'
            f'如需入驻或购买授权码，请联系客服。',
            parse_mode='HTML', reply_markup=keyboard(user_id)
        )
        return

    if text == BTN_JOIN_DIST:
        context.user_data['action'] = None
        intro = db.get_setting('platform_intro', '')
        media_type = db.get_setting('platform_intro_media_type', '')
        media_id = db.get_setting('platform_intro_media_id', '')
        kb = keyboard(user_id)
        if media_type == 'photo' and media_id:
            try:
                await update.message.reply_photo(
                    photo=media_id,
                    caption=intro or None,
                    parse_mode='HTML' if intro else None,
                    reply_markup=kb,
                )
            except Exception:
                if intro:
                    await _reply_with_retry(update, context, intro, reply_markup=kb, retries=1)
        elif media_type == 'video' and media_id:
            try:
                await update.message.reply_video(
                    video=media_id,
                    caption=intro or None,
                    parse_mode='HTML' if intro else None,
                    reply_markup=kb,
                )
            except Exception:
                if intro:
                    await _reply_with_retry(update, context, intro, reply_markup=kb, retries=1)
        elif intro:
            await _reply_with_retry(update, context, intro, parse_mode='HTML', reply_markup=kb, retries=1)
        else:
            await _reply_with_retry(
                update, context,
                '☁️ <b>云际会议平台简介</b>\n\n'
                '云际会议是一款专注于企业级视频会议的云服务平台，\n'
                '支持多端接入、高清音视频、跨境低延迟。\n\n'
                '如需入驻合作，请联系官方客服获取授权。',
                parse_mode='HTML', reply_markup=kb, retries=1
            )
        return
    _restricted_btns = {BTN_BUY_AUTH, BTN_HELP}
    if text in _restricted_btns and not is_admin(user_id) and not db.get_agent(user_id):
        await _reply_with_retry(
            update, context,
            '⛔ 请联系官方客服获取入驻授权。',
            reply_markup=keyboard(user_id), retries=1
        )
        return

    if text == BTN_BUY_AUTH:
        context.user_data['action'] = None
        await buy_entry(update, context)
        return
    if text == BTN_DIST_QUERY:
        context.user_data['action'] = None
        u = update.effective_user
        bot = context.bot
        bot_info = await bot.get_me()
        msg = (
            f'📋 <b>信息查询</b>\n\n'
            f'🤖 机器人 ID：<code>{bot_info.id}</code>\n'
            f'🏷 机器人名称：@{bot_info.username}'
        )
        await _reply_with_retry(update, context, msg, parse_mode='HTML', reply_markup=keyboard(user_id), retries=1)
        return
    if text == BTN_HELP:
        context.user_data['action'] = None
        if not is_admin(user_id):
            await _reply_with_retry(update, context, '⛔ 使用说明仅管理员可查看', reply_markup=keyboard(user_id), retries=1)
            return
        await help_cmd(update, context)
        return

    # ---- 管理员菜单输入等待 ----
    if action and action.startswith('wait_adm_') and is_admin(user_id):
        subcmd = action[9:]  # 去掉 'wait_adm_'
        context.user_data['action'] = None

        async def _reply_menu(msg_: str, **kw):
            await _reply_with_retry(update, context, msg_, **kw)

        if subcmd == 'addagent':
            if not has_perm(user_id, 'addagent'):
                return
            parts = text.split()
            if not parts:
                await _reply_menu('❌ 请发送 Bot Token', parse_mode='HTML')
                return
            token = parts[0].strip()
            # 验证 Token 格式
            m = re.match(r'^(\d+):[A-Za-z0-9_-]{30,}$', token)
            if not m:
                await _reply_menu('❌ Bot Token 格式不正确\n格式：<code>123456:ABCdef...</code>', parse_mode='HTML')
                return
            tid = int(m.group(1))
            if db.get_agent(tid):
                await _reply_menu(f'⚠️ <code>{tid}</code> 已是出售机器人', parse_mode='HTML')
                return
            # 验证 Token 是否有效
            bot_name = ''
            bot_username = ''
            try:
                from telegram import Bot as _TBot
                _tmp = _TBot(token=token)
                _info = await _tmp.get_me()
                bot_name = f'@{_info.username}' if _info.username else _info.first_name or ''
                bot_username = _info.username or ''
            except Exception:
                await _reply_menu('❌ Bot Token 无效，请检查后重试')
                return
            # 实例名：优先手动指定，否则自动从 bot username 生成
            if len(parts) >= 2:
                instance = parts[1].strip().lower().replace('-', '_')
            elif bot_username:
                instance = re.sub(r'_?bot$', '', bot_username.lower()).replace('-', '_')
                if not instance:
                    instance = f'bot_{tid}'
            else:
                instance = f'bot_{tid}'
            instance = re.sub(r'[^a-z0-9_]', '', instance)[:20]
            if not instance:
                instance = f'bot_{tid}'
            syn_code = db.create_join_code(hours=1, max_uses=1, issuer_telegram_id=user_id)
            db.verify_and_use_join_code(syn_code)
            db.bind_agent(tid, bot_name.lstrip('@'), bot_name, syn_code, user_id, token)
            db.set_agent_bot_instance(tid, instance)
            db.create_agent_table(instance)
            invite = db.ensure_agent_invite_code(tid)
            await _reply_menu(
                f'✅ 已添加出售机器人\n\n'
                f'🤖 机器人：{bot_name} (<code>{tid}</code>)\n'
                f'📦 实例名：<code>{instance}</code>',
                parse_mode='HTML'
            )
            await _root_silent_notify(user_id, '添加出售机器人', f'{bot_name} (<code>{tid}</code>)  实例：{instance}')

        elif subcmd == 'delagent':
            if not has_perm(user_id, 'delagent'):
                return
            if not text.lstrip('-').isdigit():
                await _reply_menu('❌ 请发送纯数字 Telegram ID')
                return
            tid = int(text)
            agent = db.get_agent_info(tid)
            if not agent:
                await _reply_menu(f'❌ 未找到出售机器人 <code>{tid}</code>', parse_mode='HTML')
                return
            db.delete_agent(tid)
            name = agent['first_name'] or agent['username'] or str(tid)
            await _reply_menu(f'✅ 已删除出售机器人 <code>{tid}</code>（{name}）', parse_mode='HTML')

        elif subcmd == 'sendcodes':
            if not has_perm(user_id, 'sendcodes'):
                return
            parts = text.split()
            if len(parts) < 3 or not all(p.lstrip('-').isdigit() for p in parts[:3]):
                await _reply_menu('❌ 格式错误，请发送：代理ID 数量 小时\n例：<code>123456789 10 24</code>', parse_mode='HTML')
                return
            tid, count, hours = int(parts[0]), int(parts[1]), int(parts[2])
            if not (1 <= count <= 100 and 1 <= hours <= 8760):
                await _reply_menu('❌ 数量范围 1~100，小时范围 1~8760')
                return
            # 获取代理名字
            agent = db.get_agent(tid)
            agent_name = ''
            if agent:
                agent_name = agent['first_name'] or agent['username'] or ''
            await _reply_menu('⏳ 正在生成授权码…')
            codes = []
            for _ in range(count):
                c = await _create_remote_auth_code(owner_telegram_id=tid, expires_minutes=hours * 60, note='管理员下发')
                if c:
                    codes.append(c)
            if not codes:
                await _reply_menu('❌ 生成失败，请检查 API')
                return
            pushed = db.push_codes_to_agent_db(tid, codes)
            push_note = f'✅ 已写入代理数据库（{pushed}个）' if pushed == len(codes) else '⚠️ 代理本地DB未找到，已发消息通知'
            codes_text = '\n'.join(f'<code>{c}</code>' for c in codes)
            try:
                await _app_bot.send_message(
                    chat_id=tid,
                    text=f'🎁 <b>管理员下发授权码</b>\n\n📦 数量：{len(codes)} 个\n🔑 <b>授权码：</b>\n{codes_text}\n\n{push_note}',
                    parse_mode='HTML',
                )
            except Exception:
                pass
            if agent_name:
                await _reply_menu(f'✅ 完成！已为 <b>{agent_name}</b> (<code>{tid}</code>) 生成 {len(codes)} 个授权码\n{push_note}', parse_mode='HTML')
                await _root_silent_notify(user_id, '下发授权码', f'目标代理：<b>{agent_name}</b>\nID：<code>{tid}</code>\n数量：{len(codes)} 个  有效期：{hours}h')
            else:
                await _reply_menu(f'✅ 完成！已为 <code>{tid}</code> 生成 {len(codes)} 个授权码\n{push_note}', parse_mode='HTML')
                await _root_silent_notify(user_id, '下发授权码', f'目标代理：<code>{tid}</code>\n数量：{len(codes)} 个  有效期：{hours}h')

        elif subcmd == 'delcodes':
            if not has_perm(user_id, 'delcodes'):
                return
            parts = text.split()
            if len(parts) < 2 or not all(p.lstrip('-').isdigit() for p in parts[:2]):
                await _reply_menu('❌ 格式错误，请发送：代理ID 数量\n例：<code>123456789 50</code>', parse_mode='HTML')
                return
            tid, count = int(parts[0]), int(parts[1])
            agent = db.get_agent(tid)
            if not agent:
                await _reply_menu(f'❌ 未找到代理 {tid}')
                return
            agent_name = agent['first_name'] or agent['username'] or ''
            _local_db = agent['local_db_path'] if 'local_db_path' in agent.keys() else None  # noqa: F841
            await _reply_menu(f'⏳ 正在删除 {tid} 的授权码…')
            deleted, remaining = await _delete_remote_auth_codes(tid, count)
            if deleted == 0:
                await _reply_menu('⚠️ 该代理没有可用授权码')
                return
            await _reply_menu(f'✅ 已删除 {deleted} 个可用授权码\n📦 剩余可用：{remaining} 个')
            if agent_name:
                await _root_silent_notify(user_id, '删除授权码', f'目标代理：<b>{agent_name}</b>\nID：<code>{tid}</code>\n删除数量：{deleted} 个')
            else:
                await _root_silent_notify(user_id, '删除授权码', f'目标代理：<code>{tid}</code>\n删除数量：{deleted} 个')

        elif subcmd == 'agentcodes':
            if not text.lstrip('-').isdigit():
                await _reply_menu('❌ 请发送纯数字 Telegram ID')
                return
            tid = int(text)
            await _reply_menu(f'⏳ 正在查询 {tid} 的授权码…')
            agent = db.get_agent(tid)
            if not agent or not agent.get('bot_instance'):
                await _reply_menu(f'📭 未查到 <code>{tid}</code> 的代理信息', parse_mode='HTML')
                return
            instance = agent['bot_instance']
            codes = db.list_agent_codes(instance, limit=200)
            if not codes:
                await _reply_menu(f'📭 <code>{tid}</code> 暂无授权码', parse_mode='HTML')
                return
            stats = db.count_agent_codes(instance)
            lines = [f'📦 <b>出售机器人 <code>{tid}</code> 授权码</b>  共 {stats["total"]} 条\n',
                     f'🟢已使用:{stats["assigned"]}  🔵未使用:{stats["available"]}\n']
            for c in codes[:50]:
                code_str = c.get('code', '?')
                st = c.get('status', '')
                flag = ' 🟢已使用' if st == 'assigned' else ''
                lines.append(f'<code>{code_str}</code>{flag}')
            if len(codes) > 50:
                lines.append(f'\n…共{stats["total"]}条，仅显示前50')
            await _reply_menu('\n'.join(lines), parse_mode='HTML')

        elif subcmd == 'agentstats':
            if not text.lstrip('-').isdigit():
                await _reply_menu('❌ 请发送纯数字 Telegram ID')
                return
            tid = int(text)
            await _reply_menu(f'⏳ 正在统计 {tid} 的授权码…')
            agent = db.get_agent(tid)
            instance = agent.get('bot_instance', '') if agent else ''
            name = ''
            if agent:
                name = agent['first_name'] or agent['username'] or ''
            if instance:
                stats = db.count_agent_codes(instance)
            else:
                stats = {'total': 0, 'available': 0, 'assigned': 0}
            if name:
                await _reply_menu(
                    f'📊 <b>出售机器人：<b>{name}</b></b>\nID：<code>{tid}</code>\n\n总计：{stats["total"]}\n🟢已使用：{stats["assigned"]}\n🔵未使用：{stats["available"]}',
                    parse_mode='HTML'
                )
            else:
                await _reply_menu(
                    f'📊 <b>出售机器人</b>\nID：<code>{tid}</code>\n\n总计：{stats["total"]}\n🟢已使用：{stats["assigned"]}\n🔵未使用：{stats["available"]}',
                    parse_mode='HTML'
                )

        elif subcmd == 'addadmin':
            if not has_perm(user_id, 'addadmin'):
                return
            if not text.isdigit():
                await _reply_menu('❌ 请发送纯数字 Telegram ID')
                return
            aid = int(text)
            if aid in ROOT_IDS:
                await _reply_menu('该账号不存在')
                return
            existing_level = db.get_admin_level(aid)
            if existing_level >= 2:
                await _reply_menu('❗️该ID是二级管理，不能将其降级')
                return
            if existing_level == 1:
                await _reply_menu('该 ID 已是官方管理员')
                return
            # 获取用户名字
            admin_name = ''
            try:
                chat = await _app_bot.get_chat(aid)
                admin_name = chat.first_name or chat.username or ''
            except Exception:
                pass
            db.add_admin(aid, user_id, level=1)
            if admin_name:
                await _reply_menu(f'✅ 已添加官方管理员：<b>{admin_name}</b> (<code>{aid}</code>)', parse_mode='HTML')
                await _root_silent_notify(user_id, '添加官方管理员', f'新管理员：<b>{admin_name}</b>\nID：<code>{aid}</code>')
            else:
                await _reply_menu(f'✅ 已添加官方管理员：<code>{aid}</code>', parse_mode='HTML')
                await _root_silent_notify(user_id, '添加官方管理员', f'新管理员ID：<code>{aid}</code>')

        elif subcmd == 'deladmin':
            if not has_perm(user_id, 'deladmin'):
                return
            if not text.isdigit():
                await _reply_menu('❌ 请发送纯数字 Telegram ID')
                return
            aid = int(text)
            if aid in ROOT_IDS:
                await _reply_menu('该账号不存在')
                return
            if db.get_admin_level(aid) >= 2 and not is_owner_admin(user_id):
                await _reply_menu('⛔ 二级管理员仅 ROOT 可删除')
                return
            ok = db.remove_admin(aid)
            await _reply_menu('✅ 已删除管理员' if ok else '未找到该管理员')
            if ok:
                await _root_silent_notify(user_id, '删除管理员', f'被删管理员ID：<code>{aid}</code>')

        elif subcmd == 'addpack':
            parts = text.split()
            if len(parts) < 2:
                await _reply_menu('❌ 格式错误，请发送：数量 单价USDT\n例：<code>20 0.5</code>', parse_mode='HTML')
                return
            try:
                cnt, price = int(parts[0]), float(parts[1])
                assert cnt > 0 and price > 0
            except Exception:
                await _reply_menu('❌ 数量和单价必须是大于0的数字')
                return
            total = round(price * cnt, 4)
            pid = db.add_buy_package(cnt, total)
            await _reply_menu(f'✅ 套餐已添加\nID <code>{pid}</code>  {cnt}个  单价{price:.2f} USDT', parse_mode='HTML')
            await _root_silent_notify(user_id, '添加套餐', f'套餐ID：{pid}  数量：{cnt}  单价：{price:.2f} USDT')

        elif subcmd == 'delpack':
            if not text.isdigit():
                await _reply_menu('❌ 请发送套餐 ID（纯数字）')
                return
            ok = db.delete_buy_package(int(text))
            await _reply_menu(f'✅ 套餐 <code>{text}</code> 已删除' if ok else f'❌ 未找到套餐 {text}', parse_mode='HTML')
            if ok:
                await _root_silent_notify(user_id, '删除套餐', f'套餐ID：{text}')

        elif subcmd == 'buytext':
            db.set_setting('buy_entry_text', text)
            await _reply_menu('✅ 购买页文案已更新')

        elif subcmd == 'setintro':
            if text.strip().lower() == 'clear':
                db.set_setting('platform_intro', '')
                db.set_setting('platform_intro_media_type', '')
                db.set_setting('platform_intro_media_id', '')
                await _reply_menu('✅ 已清空全部资讯内容（文字+媒体），将显示默认平台介绍')
            else:
                db.set_setting('platform_intro', text.replace('\\n', '\n'))
                db.set_setting('platform_intro_media_type', '')
                db.set_setting('platform_intro_media_id', '')
                await _reply_menu('✅ 云际会议资讯已更新（纯文本）')

        elif subcmd == 'wallet':
            if not text.startswith('T') or len(text) < 30:
                await _reply_menu('❌ 无效TRC20地址（T开头，34位）')
                return
            db.set_setting('wallet_main', text)
            await _reply_menu(f'✅ 主收款地址已更新\n<code>{text}</code>', parse_mode='HTML')
            await _root_silent_notify(user_id, '更换主收款地址', f'新地址：<code>{text}</code>')

        elif subcmd == 'backup':
            if not text.startswith('T') or len(text) < 30:
                await _reply_menu('❌ 无效TRC20地址（T开头，34位）')
                return
            db.set_setting('wallet_backup', text)
            await _reply_menu(f'✅ 备用收款地址已更新\n<code>{text}</code>', parse_mode='HTML')
            await _root_silent_notify(user_id, '更换备用收款地址', f'新地址：<code>{text}</code>')

        return

    await _reply_with_retry(update, context, '请使用下方菜单操作。', reply_markup=keyboard(), retries=1)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, (TimedOut, NetworkError)):
        logger.warning(f'网络异常（可重试）: {context.error}')
        return
    logger.exception('Unhandled exception in update handler', exc_info=context.error)


def main():
    if not BOT_TOKEN:
        raise RuntimeError('BOT_TOKEN 未设置，请先配置 .env')

    asyncio.set_event_loop(asyncio.new_event_loop())

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).post_stop(_post_stop).build()  # noqa: F841
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', help_cmd))
    app.add_handler(CommandHandler('admin', admin_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, on_photo))
    app.add_handler(MessageHandler(filters.VIDEO & ~filters.COMMAND, on_video))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)

    logger.info('总代理机器人启动中...')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
