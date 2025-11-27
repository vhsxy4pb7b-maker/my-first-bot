"""常量定义"""

# 星期分组映射
WEEKDAY_GROUP = {
    0: '一',  # Monday
    1: '二',  # Tuesday
    2: '三',  # Wednesday
    3: '四',  # Thursday
    4: '五',  # Friday
    5: '六',  # Saturday
    6: '日'   # Sunday
}

# 订单状态
ORDER_STATES = {
    'normal': '正常',
    'overdue': '逾期',
    'breach': '违约',
    'end': '完成',
    'breach_end': '违约完成'
}

# 历史订单阈值日期（2025-11-25之前的订单不扣款）
HISTORICAL_THRESHOLD_DATE = (2025, 11, 25)

# 日结时间阈值（23:00）
DAILY_CUTOFF_HOUR = 23

# 允许的日结字段前缀
DAILY_ALLOWED_PREFIXES = [
    'new_clients', 'old_clients',
    'interest', 'completed', 'breach', 'breach_end'
]

# 用户状态
USER_STATES = {
    'WAITING_BREACH_END_AMOUNT': '等待违约完成金额',
    'SEARCHING': '搜索中',
    'REPORT_QUERY': '报表查询',
    'QUERY_EXPENSE_COMPANY': '查询公司开销',
    'QUERY_EXPENSE_OTHER': '查询其他开销',
    'WAITING_EXPENSE_COMPANY': '等待公司开销输入',
    'WAITING_EXPENSE_OTHER': '等待其他开销输入',
    'BROADCASTING': '群发中',
    'BROADCAST_PAYMENT': '播报付款提醒',
    'REPORT_SEARCHING': '报表查找中',
    'UPDATING_BALANCE_GCASH': '更新GCASH余额',
    'UPDATING_BALANCE_PAYMAYA': '更新PayMaya余额',
    'EDITING_ACCOUNT_GCASH': '编辑GCASH账号',
    'EDITING_ACCOUNT_PAYMAYA': '编辑PayMaya账号',
    'ADDING_ACCOUNT_GCASH': '添加GCASH账户',
    'ADDING_ACCOUNT_PAYMAYA': '添加PayMaya账户',
    'EDITING_ACCOUNT_BY_ID_GCASH': '编辑GCASH账户（按ID）',
    'EDITING_ACCOUNT_BY_ID_PAYMAYA': '编辑PayMaya账户（按ID）'
}

