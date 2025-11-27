"""回调处理器模块"""
from .report_callbacks import handle_report_callback
from .search_callbacks import handle_search_callback
from .order_callbacks import handle_order_action_callback
from .payment_callbacks import handle_payment_callback
from .schedule_callbacks import handle_schedule_callback
from .main_callback import button_callback

__all__ = [
    'handle_report_callback',
    'handle_search_callback',
    'handle_order_action_callback',
    'handle_payment_callback',
    'handle_schedule_callback',
    'button_callback'
]

