import sqlite3
import os
from datetime import datetime
from typing import Optional, Dict, List, Tuple

# 数据库文件路径 - 支持持久化存储
# 如果设置了 DATA_DIR 环境变量，使用该目录；否则使用当前目录
DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
DB_NAME = os.path.join(DATA_DIR, 'loan_bot.db')


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # 使结果可以像字典一样访问
    return conn


# ========== 订单操作 ==========

def create_order(order_data: Dict) -> bool:
    """创建新订单"""
    conn = get_connection()
    cursor = conn.cursor()
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
    except Exception as e:
        print(f"订单创建失败: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def get_order_by_chat_id(chat_id: int) -> Optional[Dict]:
    """根据chat_id获取订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM orders WHERE chat_id = ? AND state NOT IN (?, ?)',
                       (chat_id, 'end', 'breach_end'))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    finally:
        conn.close()


def get_order_by_order_id(order_id: str) -> Optional[Dict]:
    """根据order_id获取订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM orders WHERE order_id = ?', (order_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    finally:
        conn.close()


def update_order_amount(chat_id: int, new_amount: float) -> bool:
    """更新订单金额"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
        UPDATE orders 
        SET amount = ?, updated_at = CURRENT_TIMESTAMP
        WHERE chat_id = ? AND state NOT IN (?, ?)
        ''', (new_amount, chat_id, 'end', 'breach_end'))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def update_order_state(chat_id: int, new_state: str) -> bool:
    """更新订单状态"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
        UPDATE orders 
        SET state = ?, updated_at = CURRENT_TIMESTAMP
        WHERE chat_id = ? AND state NOT IN (?, ?)
        ''', (new_state, chat_id, 'end', 'breach_end'))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def delete_order_by_chat_id(chat_id: int) -> bool:
    """删除订单（标记为完成或违约完成时使用）"""
    # 实际上不删除，只是状态已变为end或breach_end
    return True


# ========== 查找功能 ==========

def search_orders_by_group_id(group_id: str, state: Optional[str] = None) -> List[Dict]:
    """根据归属ID查找订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if state:
            cursor.execute('SELECT * FROM orders WHERE group_id = ? AND state = ? ORDER BY date DESC',
                           (group_id, state))
        else:
            cursor.execute(
                'SELECT * FROM orders WHERE group_id = ? ORDER BY date DESC', (group_id,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_orders_by_date_range(start_date: str, end_date: str) -> List[Dict]:
    """根据日期范围查找订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
        SELECT * FROM orders 
        WHERE date >= ? AND date <= ?
        ORDER BY date DESC
        ''', (start_date, end_date))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_orders_by_customer(customer: str) -> List[Dict]:
    """根据客户类型查找订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT * FROM orders WHERE customer = ? ORDER BY date DESC', (customer.upper(),))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_orders_by_state(state: str) -> List[Dict]:
    """根据状态查找订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT * FROM orders WHERE state = ? ORDER BY date DESC', (state,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_orders_all() -> List[Dict]:
    """查找所有订单"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM orders ORDER BY date DESC')
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


# ========== 订单ID生成 ==========

def get_next_order_id() -> str:
    """获取下一个订单ID"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'UPDATE order_counter SET counter = counter + 1 WHERE id = 1')
        cursor.execute('SELECT counter FROM order_counter WHERE id = 1')
        counter = cursor.fetchone()[0]
        conn.commit()
        return f"{counter:04d}"
    finally:
        conn.close()


# ========== 财务数据操作 ==========

def get_financial_data() -> Dict:
    """获取全局财务数据"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
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
    finally:
        conn.close()


def update_financial_data(field: str, amount: float) -> bool:
    """更新财务数据字段"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
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
    except Exception as e:
        print(f"更新财务数据错误: {e}")
        return False
    finally:
        conn.close()


# ========== 分组数据操作 ==========

def get_grouped_data(group_id: Optional[str] = None) -> Dict:
    """获取分组数据"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
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
    finally:
        conn.close()


def update_grouped_data(group_id: str, field: str, amount: float) -> bool:
    """更新分组数据字段"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
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
    except Exception as e:
        print(f"更新分组数据错误: {e}")
        return False
    finally:
        conn.close()


def get_all_group_ids() -> List[str]:
    """获取所有归属ID列表"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT DISTINCT group_id FROM grouped_data ORDER BY group_id')
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


# ========== 日结数据操作 ==========

def get_daily_data(date: str, group_id: Optional[str] = None) -> Dict:
    """获取日结数据（11:00-23:00为一个周期）"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
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
            'breach_end_amount': 0
        }
    finally:
        conn.close()


def update_daily_data(date: str, field: str, amount: float, group_id: Optional[str] = None) -> bool:
    """更新日结数据字段"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
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
                breach_end_orders, breach_end_amount
            ) VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
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
    except Exception as e:
        print(f"更新日结数据错误: {e}")
        return False
    finally:
        conn.close()
