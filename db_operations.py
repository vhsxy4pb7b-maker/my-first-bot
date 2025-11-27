import sqlite3
import os
import asyncio
from typing import Optional, Dict, List, Tuple, Any
from functools import wraps

# 数据库文件路径 - 支持持久化存储
DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, 'loan_bot.db')


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_transaction(func):
    """数据库事务装饰器: 异步执行，自动处理连接开启、提交、回滚和关闭"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_running_loop()

        def sync_work():
            conn = get_connection()
            cursor = conn.cursor()
            try:
                # 执行被装饰的同步函数
                result = func(conn, cursor, *args, **kwargs)
                return result
            except Exception as e:
                conn.rollback()
                print(f"Database error in {func.__name__}: {e}")
                return False
            finally:
                conn.close()

        return await loop.run_in_executor(None, sync_work)
    return wrapper


def db_query(func):
    """数据库查询装饰器: 异步执行，自动处理连接开启和关闭"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_running_loop()

        def sync_work():
            conn = get_connection()
            cursor = conn.cursor()
            try:
                return func(conn, cursor, *args, **kwargs)
            except Exception as e:
                print(f"Database query error in {func.__name__}: {e}")
                raise e
            finally:
                conn.close()

        return await loop.run_in_executor(None, sync_work)
    return wrapper

# ========== 订单操作 ==========


@db_transaction
def create_order(conn, cursor, order_data: Dict) -> bool:
    """创建新订单"""
    try:
        cursor.execute('''
        INSERT INTO orders (
            order_id, group_id, chat_id, date, weekday_group,
            customer, amount, state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            order_data['order_id'],
            order_data['group_id'],
            order_data['chat_id'],
            order_data['date'],
            order_data['group'],
            order_data['customer'],
            order_data['amount'],
            order_data['state']
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError as e:
        print(f"订单创建失败（重复）: {e}")
        return False


@db_query
def get_order_by_chat_id(conn, cursor, chat_id: int) -> Optional[Dict]:
    """根据chat_id获取订单"""
    cursor.execute('SELECT * FROM orders WHERE chat_id = ? AND state NOT IN (?, ?)',
                   (chat_id, 'end', 'breach_end'))
    row = cursor.fetchone()
    return dict(row) if row else None


@db_query
def get_order_by_order_id(conn, cursor, order_id: str) -> Optional[Dict]:
    """根据order_id获取订单"""
    cursor.execute('SELECT * FROM orders WHERE order_id = ?', (order_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


@db_transaction
def update_order_amount(conn, cursor, chat_id: int, new_amount: float) -> bool:
    """更新订单金额"""
    cursor.execute('''
    UPDATE orders 
    SET amount = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ? AND state NOT IN (?, ?)
    ''', (new_amount, chat_id, 'end', 'breach_end'))
    conn.commit()
    return cursor.rowcount > 0


@db_transaction
def update_order_state(conn, cursor, chat_id: int, new_state: str) -> bool:
    """更新订单状态"""
    cursor.execute('''
    UPDATE orders 
    SET state = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ? AND state NOT IN (?, ?)
    ''', (new_state, chat_id, 'end', 'breach_end'))
    conn.commit()
    return cursor.rowcount > 0


@db_transaction
def update_order_group_id(conn, cursor, chat_id: int, new_group_id: str) -> bool:
    """更新订单归属ID"""
    cursor.execute('''
    UPDATE orders 
    SET group_id = ?, updated_at = CURRENT_TIMESTAMP
    WHERE chat_id = ?
    ''', (new_group_id, chat_id))
    conn.commit()
    return cursor.rowcount > 0


def delete_order_by_chat_id(chat_id: int) -> bool:
    """删除订单（标记为完成或违约完成时使用）"""
    return True

# ========== 查找功能 ==========


@db_query
def search_orders_by_group_id(conn, cursor, group_id: str, state: Optional[str] = None) -> List[Dict]:
    """根据归属ID查找订单"""
    if state:
        cursor.execute('SELECT * FROM orders WHERE group_id = ? AND state = ? ORDER BY date DESC',
                       (group_id, state))
    else:
        # 默认排除完成和违约完成的订单
        cursor.execute(
            "SELECT * FROM orders WHERE group_id = ? AND state NOT IN ('end', 'breach_end') ORDER BY date DESC", (group_id,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_by_date_range(conn, cursor, start_date: str, end_date: str) -> List[Dict]:
    """根据日期范围查找订单"""
    cursor.execute('''
    SELECT * FROM orders 
    WHERE date >= ? AND date <= ?
    ORDER BY date DESC
    ''', (start_date, end_date))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_by_customer(conn, cursor, customer: str) -> List[Dict]:
    """根据客户类型查找订单"""
    cursor.execute(
        'SELECT * FROM orders WHERE customer = ? ORDER BY date DESC', (customer.upper(),))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_by_state(conn, cursor, state: str) -> List[Dict]:
    """根据状态查找订单"""
    cursor.execute(
        'SELECT * FROM orders WHERE state = ? ORDER BY date DESC', (state,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_all(conn, cursor) -> List[Dict]:
    """查找所有订单"""
    cursor.execute('SELECT * FROM orders ORDER BY date DESC')
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_advanced(conn, cursor, criteria: Dict) -> List[Dict]:
    """
    高级查找订单（支持混合条件）
    """
    query = "SELECT * FROM orders WHERE 1=1"
    params = []

    if 'group_id' in criteria and criteria['group_id']:
        query += " AND group_id = ?"
        params.append(criteria['group_id'])

    if 'state' in criteria and criteria['state']:
        query += " AND state = ?"
        params.append(criteria['state'])
    else:
        # 默认排除完成和违约完成的订单
        query += " AND state NOT IN ('end', 'breach_end')"

    if 'customer' in criteria and criteria['customer']:
        query += " AND customer = ?"
        params.append(criteria['customer'])

    if 'order_id' in criteria and criteria['order_id']:
        query += " AND order_id = ?"
        params.append(criteria['order_id'])

    if 'date_range' in criteria and criteria['date_range']:
        start_date, end_date = criteria['date_range']
        query += " AND date >= ? AND date <= ?"
        params.extend([start_date, end_date])

    if 'weekday_group' in criteria and criteria['weekday_group']:
        query += " AND weekday_group = ?"
        params.append(criteria['weekday_group'])

    query += " ORDER BY date DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def search_orders_advanced_all_states(conn, cursor, criteria: Dict) -> List[Dict]:
    """
    高级查找订单（支持混合条件，包含所有状态的订单）
    用于报表查找功能
    """
    query = "SELECT * FROM orders WHERE 1=1"
    params = []

    if 'group_id' in criteria and criteria['group_id']:
        query += " AND group_id = ?"
        params.append(criteria['group_id'])

    if 'state' in criteria and criteria['state']:
        query += " AND state = ?"
        params.append(criteria['state'])
    # 注意：这里不排除任何状态，包含所有状态的订单

    if 'customer' in criteria and criteria['customer']:
        query += " AND customer = ?"
        params.append(criteria['customer'])

    if 'order_id' in criteria and criteria['order_id']:
        query += " AND order_id = ?"
        params.append(criteria['order_id'])

    if 'date_range' in criteria and criteria['date_range']:
        start_date, end_date = criteria['date_range']
        query += " AND date >= ? AND date <= ?"
        params.extend([start_date, end_date])

    if 'weekday_group' in criteria and criteria['weekday_group']:
        query += " AND weekday_group = ?"
        params.append(criteria['weekday_group'])

    query += " ORDER BY date DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]

# ========== 财务数据操作 ==========


@db_query
def get_financial_data(conn, cursor) -> Dict:
    """获取全局财务数据"""
    cursor.execute('SELECT * FROM financial_data ORDER BY id DESC LIMIT 1')
    row = cursor.fetchone()
    if row:
        return dict(row)
    # 如果不存在，返回默认值
    return {
        'valid_orders': 0,
        'valid_amount': 0,
        'liquid_funds': 0,
        'new_clients': 0,
        'new_clients_amount': 0,
        'old_clients': 0,
        'old_clients_amount': 0,
        'interest': 0,
        'completed_orders': 0,
        'completed_amount': 0,
        'breach_orders': 0,
        'breach_amount': 0,
        'breach_end_orders': 0,
        'breach_end_amount': 0
    }


@db_transaction
def update_financial_data(conn, cursor, field: str, amount: float) -> bool:
    """更新财务数据字段"""
    # 先获取当前值
    cursor.execute('SELECT * FROM financial_data ORDER BY id DESC LIMIT 1')
    row = cursor.fetchone()
    if not row:
        # 如果不存在，创建新记录
        cursor.execute('''
        INSERT INTO financial_data (
            valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ''')
        conn.commit()
        current_value = 0
    else:
        row_dict = dict(row)
        current_value = row_dict.get(field, 0)

    # 更新值
    new_value = current_value + amount
    # 使用参数化查询防止SQL注入
    cursor.execute(f'''
    UPDATE financial_data 
    SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM financial_data ORDER BY id DESC LIMIT 1)
    ''', (new_value,))
    conn.commit()
    return True

# ========== 分组数据操作 ==========


@db_query
def get_grouped_data(conn, cursor, group_id: Optional[str] = None) -> Dict:
    """获取分组数据"""
    if group_id:
        cursor.execute(
            'SELECT * FROM grouped_data WHERE group_id = ?', (group_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        # 如果不存在，返回默认值
        return {
            'group_id': group_id,
            'valid_orders': 0,
            'valid_amount': 0,
            'liquid_funds': 0,
            'new_clients': 0,
            'new_clients_amount': 0,
            'old_clients': 0,
            'old_clients_amount': 0,
            'interest': 0,
            'completed_orders': 0,
            'completed_amount': 0,
            'breach_orders': 0,
            'breach_amount': 0,
            'breach_end_orders': 0,
            'breach_end_amount': 0
        }
    else:
        # 获取所有分组数据
        cursor.execute('SELECT * FROM grouped_data')
        rows = cursor.fetchall()
        result = {}
        for row in rows:
            result[row['group_id']] = dict(row)
        return result


@db_transaction
def update_grouped_data(conn, cursor, group_id: str, field: str, amount: float) -> bool:
    """更新分组数据字段"""
    # 检查分组是否存在
    cursor.execute(
        'SELECT * FROM grouped_data WHERE group_id = ?', (group_id,))
    row = cursor.fetchone()

    if not row:
        # 如果不存在，创建新记录
        cursor.execute('''
        INSERT INTO grouped_data (
            group_id, valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ''', (group_id,))
        conn.commit()
        current_value = 0
    else:
        row_dict = dict(row)
        current_value = row_dict.get(field, 0)

    # 更新值
    new_value = current_value + amount
    # 使用参数化查询防止SQL注入
    cursor.execute(f'''
    UPDATE grouped_data 
    SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE group_id = ?
    ''', (new_value, group_id))
    conn.commit()
    return True


@db_query
def get_all_group_ids(conn, cursor) -> List[str]:
    """获取所有归属ID列表"""
    cursor.execute(
        'SELECT DISTINCT group_id FROM grouped_data ORDER BY group_id')
    rows = cursor.fetchall()
    return [row[0] for row in rows]

# ========== 日结数据操作 ==========


@db_query
def get_daily_data(conn, cursor, date: str, group_id: Optional[str] = None) -> Dict:
    """获取日结数据"""
    if group_id:
        cursor.execute(
            'SELECT * FROM daily_data WHERE date = ? AND group_id = ?', (date, group_id))
    else:
        # 全局日结数据（group_id为NULL）
        cursor.execute(
            'SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL', (date,))

    row = cursor.fetchone()
    if row:
        return dict(row)

    # 如果不存在，返回默认值
    return {
        'new_clients': 0,
        'new_clients_amount': 0,
        'old_clients': 0,
        'old_clients_amount': 0,
        'interest': 0,
        'completed_orders': 0,
        'completed_amount': 0,
        'breach_orders': 0,
        'breach_amount': 0,
        'breach_end_orders': 0,
        'breach_end_amount': 0,
        'liquid_flow': 0,
        'company_expenses': 0,
        'other_expenses': 0
    }


@db_transaction
def update_daily_data(conn, cursor, date: str, field: str, amount: float, group_id: Optional[str] = None) -> bool:
    """更新日结数据字段"""
    # 检查记录是否存在
    if group_id:
        cursor.execute(
            'SELECT * FROM daily_data WHERE date = ? AND group_id = ?', (date, group_id))
    else:
        cursor.execute(
            'SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL', (date,))

    row = cursor.fetchone()

    if not row:
        # 如果不存在，创建新记录
        cursor.execute('''
        INSERT INTO daily_data (
            date, group_id, new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount,
            liquid_flow, company_expenses, other_expenses
        ) VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ''', (date, group_id))
        conn.commit()
        current_value = 0
    else:
        row_dict = dict(row)
        current_value = row_dict.get(field, 0)

    # 更新值
    new_value = current_value + amount
    if group_id:
        cursor.execute(f'''
        UPDATE daily_data 
        SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id = ?
        ''', (new_value, date, group_id))
    else:
        cursor.execute(f'''
        UPDATE daily_data 
        SET "{field}" = ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id IS NULL
        ''', (new_value, date))

    conn.commit()
    return True


@db_query
def get_stats_by_date_range(conn, cursor, start_date: str, end_date: str, group_id: Optional[str] = None) -> Dict:
    """根据日期范围聚合统计数据"""
    # 构建查询条件
    where_clause = "date >= ? AND date <= ?"
    params = [start_date, end_date]

    if group_id:
        where_clause += " AND group_id = ?"
        params.append(group_id)
    else:
        where_clause += " AND group_id IS NULL"

    cursor.execute(f'''
    SELECT 
        SUM(new_clients) as new_clients,
        SUM(new_clients_amount) as new_clients_amount,
        SUM(old_clients) as old_clients,
        SUM(old_clients_amount) as old_clients_amount,
        SUM(interest) as interest,
        SUM(completed_orders) as completed_orders,
        SUM(completed_amount) as completed_amount,
        SUM(breach_orders) as breach_orders,
        SUM(breach_amount) as breach_amount,
        SUM(breach_end_orders) as breach_end_orders,
        SUM(breach_end_amount) as breach_end_amount,
        SUM(liquid_flow) as liquid_flow,
        SUM(company_expenses) as company_expenses,
        SUM(other_expenses) as other_expenses
    FROM daily_data 
    WHERE {where_clause}
    ''', params)

    row = cursor.fetchone()

    # 将结果转换为字典，None转为0
    result = {}
    keys = [
        'new_clients', 'new_clients_amount',
        'old_clients', 'old_clients_amount',
        'interest',
        'completed_orders', 'completed_amount',
        'breach_orders', 'breach_amount',
        'breach_end_orders', 'breach_end_amount',
        'liquid_flow', 'company_expenses', 'other_expenses'
    ]

    for i, key in enumerate(keys):
        result[key] = row[i] if row[i] is not None else 0

    return result

# ========== 授权用户操作 ==========


@db_transaction
def add_authorized_user(conn, cursor, user_id: int) -> bool:
    """添加授权用户"""
    cursor.execute(
        'INSERT OR IGNORE INTO authorized_users (user_id) VALUES (?)', (user_id,))
    conn.commit()
    return True


@db_transaction
def remove_authorized_user(conn, cursor, user_id: int) -> bool:
    """移除授权用户"""
    cursor.execute(
        'DELETE FROM authorized_users WHERE user_id = ?', (user_id,))
    conn.commit()
    return True


@db_query
def get_authorized_users(conn, cursor) -> List[int]:
    """获取所有授权用户ID"""
    cursor.execute('SELECT user_id FROM authorized_users')
    rows = cursor.fetchall()
    return [row[0] for row in rows]


@db_query
def is_user_authorized(conn, cursor, user_id: int) -> bool:
    """检查用户是否授权"""
    cursor.execute(
        'SELECT 1 FROM authorized_users WHERE user_id = ?', (user_id,))
    return cursor.fetchone() is not None

# ========== 支付账号操作 ==========


@db_query
def get_payment_account(conn, cursor, account_type: str) -> Optional[Dict]:
    """获取支付账号信息"""
    cursor.execute(
        'SELECT * FROM payment_accounts WHERE account_type = ?', (account_type,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


@db_query
def get_all_payment_accounts(conn, cursor) -> List[Dict]:
    """获取所有支付账号信息"""
    cursor.execute('SELECT * FROM payment_accounts ORDER BY account_type, account_name')
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_payment_accounts_by_type(conn, cursor, account_type: str) -> List[Dict]:
    """获取指定类型的所有支付账号信息"""
    cursor.execute(
        'SELECT * FROM payment_accounts WHERE account_type = ? ORDER BY account_name', 
        (account_type,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_payment_account_by_id(conn, cursor, account_id: int) -> Optional[Dict]:
    """根据ID获取支付账号信息"""
    cursor.execute(
        'SELECT * FROM payment_accounts WHERE id = ?', (account_id,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


@db_transaction
def create_payment_account(conn, cursor, account_type: str, account_number: str, 
                          account_name: str = '', balance: float = 0) -> int:
    """创建新的支付账号，返回账户ID"""
    cursor.execute('''
    INSERT INTO payment_accounts (account_type, account_number, account_name, balance)
    VALUES (?, ?, ?, ?)
    ''', (account_type, account_number, account_name or '', balance or 0))
    conn.commit()
    return cursor.lastrowid


@db_transaction
def update_payment_account_by_id(conn, cursor, account_id: int, account_number: str = None, 
                                 account_name: str = None, balance: float = None) -> bool:
    """根据ID更新支付账号信息"""
    updates = []
    params = []
    
    if account_number is not None:
        updates.append('account_number = ?')
        params.append(account_number)
    
    if account_name is not None:
        updates.append('account_name = ?')
        params.append(account_name)
    
    if balance is not None:
        updates.append('balance = ?')
        params.append(balance)
    
    if not updates:
        return False
    
    updates.append('updated_at = CURRENT_TIMESTAMP')
    params.append(account_id)
    
    set_clause = ", ".join(updates)
    query = f'UPDATE payment_accounts SET {set_clause} WHERE id = ?'
    cursor.execute(query, params)
    conn.commit()
    return True


@db_transaction
def delete_payment_account(conn, cursor, account_id: int) -> bool:
    """删除支付账号"""
    cursor.execute('DELETE FROM payment_accounts WHERE id = ?', (account_id,))
    conn.commit()
    return cursor.rowcount > 0


@db_transaction
def update_payment_account(conn, cursor, account_type: str, account_number: str = None, 
                          account_name: str = None, balance: float = None) -> bool:
    """更新支付账号信息（兼容旧代码，更新该类型的第一个账户）"""
    # 获取该类型的第一个账户
    cursor.execute(
        'SELECT * FROM payment_accounts WHERE account_type = ? LIMIT 1', (account_type,))
    row = cursor.fetchone()
    
    if row:
        # 更新现有记录
        account_id = row['id']
        return update_payment_account_by_id(conn, cursor, account_id, 
                                           account_number, account_name, balance)
    else:
        # 创建新记录
        if account_number:
            create_payment_account(conn, cursor, account_type, account_number, 
                                  account_name or '', balance or 0)
            return True
        return False


@db_transaction
def record_expense(conn, cursor, date: str, type: str, amount: float, note: str) -> bool:
    """记录开销"""
    # 1. 插入详细记录
    cursor.execute('''
    INSERT INTO expense_records (date, type, amount, note)
    VALUES (?, ?, ?, ?)
    ''', (date, type, amount, note))

    # 2. 更新日结数据
    field = 'company_expenses' if type == 'company' else 'other_expenses'

    # 复用 update_daily_data 逻辑的简化版
    cursor.execute(
        'SELECT * FROM daily_data WHERE date = ? AND group_id IS NULL', (date,))
    row = cursor.fetchone()

    if not row:
        cursor.execute('''
        INSERT INTO daily_data (
            date, group_id, new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount,
            liquid_flow, company_expenses, other_expenses
        ) VALUES (?, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?)
        ''', (date, amount if field == 'company_expenses' else 0, amount if field == 'other_expenses' else 0))
    else:
        cursor.execute(f'''
        UPDATE daily_data 
        SET "{field}" = "{field}" + ?, updated_at = CURRENT_TIMESTAMP
        WHERE date = ? AND group_id IS NULL
        ''', (amount, date))

    # 3. 更新全局流动资金 (扣除开销)

    # 先获取当前值
    cursor.execute('SELECT * FROM financial_data ORDER BY id DESC LIMIT 1')
    row = cursor.fetchone()
    if not row:
        # 如果不存在，创建新记录
        cursor.execute('''
        INSERT INTO financial_data (
            valid_orders, valid_amount, liquid_funds,
            new_clients, new_clients_amount,
            old_clients, old_clients_amount,
            interest, completed_orders, completed_amount,
            breach_orders, breach_amount,
            breach_end_orders, breach_end_amount
        ) VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ''')
        current_value = 0
    else:
        row_dict = dict(row)
        current_value = row_dict.get('liquid_funds', 0)

    # 更新值 (减少)
    new_value = current_value - amount

    # 使用参数化查询防止SQL注入
    cursor.execute('''
    UPDATE financial_data 
    SET "liquid_funds" = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM financial_data ORDER BY id DESC LIMIT 1)
    ''', (new_value,))

    conn.commit()
    return True


@db_query
def get_expense_records(conn, cursor, start_date: str, end_date: str = None, type: Optional[str] = None) -> List[Dict]:
    """获取开销记录（支持日期范围）"""
    query = "SELECT * FROM expense_records WHERE date >= ?"
    params = [start_date]

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    else:
        # 如果没有结束日期，就只查开始日期那一天
        query += " AND date <= ?"
        params.append(start_date)

    if type:
        query += " AND type = ?"
        params.append(type)

    query += " ORDER BY date DESC, created_at ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    return [dict(row) for row in rows]

# ========== 定时播报操作 ==========


@db_query
def get_scheduled_broadcast(conn, cursor, slot: int) -> Optional[Dict]:
    """获取指定槽位的定时播报"""
    cursor.execute('SELECT * FROM scheduled_broadcasts WHERE slot = ?', (slot,))
    row = cursor.fetchone()
    return dict(row) if row else None


@db_query
def get_all_scheduled_broadcasts(conn, cursor) -> List[Dict]:
    """获取所有定时播报"""
    cursor.execute('SELECT * FROM scheduled_broadcasts ORDER BY slot')
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_query
def get_active_scheduled_broadcasts(conn, cursor) -> List[Dict]:
    """获取所有激活的定时播报"""
    cursor.execute('SELECT * FROM scheduled_broadcasts WHERE is_active = 1 ORDER BY slot')
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


@db_transaction
def create_or_update_scheduled_broadcast(conn, cursor, slot: int, time: str, 
                                       chat_id: Optional[int], chat_title: Optional[str], 
                                       message: str, is_active: int = 1) -> bool:
    """创建或更新定时播报"""
    # 检查是否已存在
    cursor.execute('SELECT * FROM scheduled_broadcasts WHERE slot = ?', (slot,))
    row = cursor.fetchone()
    
    if row:
        # 更新现有记录
        cursor.execute('''
        UPDATE scheduled_broadcasts 
        SET time = ?, chat_id = ?, chat_title = ?, message = ?, 
            is_active = ?, updated_at = CURRENT_TIMESTAMP
        WHERE slot = ?
        ''', (time, chat_id, chat_title, message, is_active, slot))
    else:
        # 创建新记录
        cursor.execute('''
        INSERT INTO scheduled_broadcasts (slot, time, chat_id, chat_title, message, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (slot, time, chat_id, chat_title, message, is_active))
    
    conn.commit()
    return True


@db_transaction
def delete_scheduled_broadcast(conn, cursor, slot: int) -> bool:
    """删除定时播报"""
    cursor.execute('DELETE FROM scheduled_broadcasts WHERE slot = ?', (slot,))
    conn.commit()
    return cursor.rowcount > 0


@db_transaction
def toggle_scheduled_broadcast(conn, cursor, slot: int, is_active: int) -> bool:
    """切换定时播报的激活状态"""
    cursor.execute('''
    UPDATE scheduled_broadcasts 
    SET is_active = ?, updated_at = CURRENT_TIMESTAMP
    WHERE slot = ?
    ''', (is_active, slot))
    conn.commit()
    return cursor.rowcount > 0
