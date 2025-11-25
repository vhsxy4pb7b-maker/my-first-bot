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

    # 创建订单计数器表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS order_counter (
        id INTEGER PRIMARY KEY,
        counter INTEGER DEFAULT 0
    )
    ''')

    # 初始化计数器（如果不存在）
    cursor.execute('SELECT COUNT(*) FROM order_counter')
    if cursor.fetchone()[0] == 0:
        cursor.execute('INSERT INTO order_counter (id, counter) VALUES (1, 0)')

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

    conn.commit()
    conn.close()
    print(f"数据库 {DB_NAME} 初始化完成！")


if __name__ == "__main__":
    init_database()
