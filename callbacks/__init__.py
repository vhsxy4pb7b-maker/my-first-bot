"""回调处理器模块"""
from .report_callbacks import handle_report_callback
from .search_callbacks import handle_search_callback
from .order_callbacks import handle_order_action_callback
from .payment_callbacks import handle_payment_callback
from .schedule_callbacks import handle_schedule_callback
from .main_callback import button_callback
import os
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
# 这样子模块在导入时能找到 handlers, decorators, utils 等模块
# ⚠️ 必须在所有导入语句之前执行！否则会导致 ModuleNotFoundError
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


__all__ = [
    'handle_report_callback',
    'handle_search_callback',
    'handle_order_action_callback',
    'handle_payment_callback',
    'handle_schedule_callback',
    'button_callback'
]
