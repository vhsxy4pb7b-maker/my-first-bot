"""日期相关工具函数"""
from datetime import datetime, timedelta
import pytz
from constants import DAILY_CUTOFF_HOUR


def get_daily_period_date() -> str:
    """获取当前日结周期对应的日期（每天23:00日切）"""
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    current_hour = now.hour

    # 如果当前时间 >= 23:00，算作明天
    if current_hour >= DAILY_CUTOFF_HOUR:
        period_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        period_date = now.strftime("%Y-%m-%d")

    return period_date


