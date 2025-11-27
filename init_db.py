import sqlite3
import os

# 数据库文件路径 - 支持持久化存储
# 如果设置了 DATA_DIR 环境变量，使用该目录；否则使用当前目录
DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, 'loan_bot.db')


def init_database():
    """初始化数据库，创建所有必要的表"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # 创建订单表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE NOT NULL,
        group_id TEXT NOT NULL,
        chat_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        weekday_group TEXT NOT NULL,
        customer TEXT NOT NULL,
        amount REAL NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 创建财务数据表（全局统计）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        valid_orders INTEGER DEFAULT 0,
        valid_amount REAL DEFAULT 0,
        liquid_funds REAL DEFAULT 0,
        new_clients INTEGER DEFAULT 0,
        new_clients_amount REAL DEFAULT 0,
        old_clients INTEGER DEFAULT 0,
        old_clients_amount REAL DEFAULT 0,
        interest REAL DEFAULT 0,
        completed_orders INTEGER DEFAULT 0,
        completed_amount REAL DEFAULT 0,
        breach_orders INTEGER DEFAULT 0,
        breach_amount REAL DEFAULT 0,
        breach_end_orders INTEGER DEFAULT 0,
        breach_end_amount REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 创建分组数据表（按归属ID分组）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS grouped_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT UNIQUE NOT NULL,
        valid_orders INTEGER DEFAULT 0,
        valid_amount REAL DEFAULT 0,
        liquid_funds REAL DEFAULT 0,
        new_clients INTEGER DEFAULT 0,
        new_clients_amount REAL DEFAULT 0,
        old_clients INTEGER DEFAULT 0,
        old_clients_amount REAL DEFAULT 0,
        interest REAL DEFAULT 0,
        completed_orders INTEGER DEFAULT 0,
        completed_amount REAL DEFAULT 0,
        breach_orders INTEGER DEFAULT 0,
        breach_amount REAL DEFAULT 0,
        breach_end_orders INTEGER DEFAULT 0,
        breach_end_amount REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 创建日结数据表（按日期和归属ID存储）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        group_id TEXT,
        new_clients INTEGER DEFAULT 0,
        new_clients_amount REAL DEFAULT 0,
        old_clients INTEGER DEFAULT 0,
        old_clients_amount REAL DEFAULT 0,
        interest REAL DEFAULT 0,
        completed_orders INTEGER DEFAULT 0,
        completed_amount REAL DEFAULT 0,
        breach_orders INTEGER DEFAULT 0,
        breach_amount REAL DEFAULT 0,
        breach_end_orders INTEGER DEFAULT 0,
        breach_end_amount REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, group_id)
    )
    ''')

    # 初始化财务数据（如果不存在）
    cursor.execute('SELECT COUNT(*) FROM financial_data')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
        INSERT INTO financial_data (
            valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (0, 0, 100000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ''')

    # 创建授权用户表（员工）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS authorized_users (
        user_id INTEGER PRIMARY KEY,
        added_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 创建支付账号表（GCASH和PayMaya）
    # 检查表是否存在，如果存在需要迁移（移除UNIQUE约束）
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='payment_accounts'")
    table_exists = cursor.fetchone()
    
    if table_exists:
        # 检查是否有UNIQUE约束（通过检查索引）
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='payment_accounts'")
        table_sql = cursor.fetchone()
        if table_sql and 'UNIQUE' in table_sql[0]:
            # 需要迁移：保存数据，重建表
            cursor.execute('SELECT * FROM payment_accounts')
            old_data = cursor.fetchall()
            cursor.execute('DROP TABLE payment_accounts')
            # 创建新表（无UNIQUE约束）
            cursor.execute('''
            CREATE TABLE payment_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_type TEXT NOT NULL,
                account_number TEXT NOT NULL,
                account_name TEXT,
                balance REAL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            # 恢复数据
            if old_data:
                cursor.executemany('''
                INSERT INTO payment_accounts (id, account_type, account_number, account_name, balance, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', old_data)
    
    # 创建表（如果不存在）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payment_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_type TEXT NOT NULL,
        account_number TEXT NOT NULL,
        account_name TEXT,
        balance REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 初始化支付账号（如果不存在）
    cursor.execute('SELECT COUNT(*) FROM payment_accounts')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
        INSERT INTO payment_accounts (account_type, account_number, account_name, balance)
        VALUES ('gcash', '', '', 0)
        ''')
        cursor.execute('''
        INSERT INTO payment_accounts (account_type, account_number, account_name, balance)
        VALUES ('paymaya', '', '', 0)
        ''')

    # 创建定时播报表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slot INTEGER NOT NULL CHECK(slot >= 1 AND slot <= 3),
        time TEXT NOT NULL,
        chat_id INTEGER,
        chat_title TEXT,
        message TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(slot)
    )
    ''')

    conn.commit()
    conn.close()
    print(f"数据库 {DB_NAME} 初始化完成！")


if __name__ == "__main__":
    init_database()
