"""命令处理器模块"""
import os
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
# 这样子模块在导入时能找到 decorators, utils 等模块
# ⚠️ 必须在所有导入语句之前执行！否则会导致 ModuleNotFoundError
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from .schedule_handlers import show_schedule_menu, handle_schedule_input
from .payment_handlers import show_gcash, show_paymaya, show_all_accounts
from .broadcast_handlers import broadcast_payment
from .message_handlers import (
    handle_new_chat_members,
    handle_new_chat_title,
    handle_text_input
)
from .search_handlers import search_orders
from .report_handlers import show_report
from .amount_handlers import handle_amount_operation
from .order_handlers import (
    set_normal,
    set_overdue,
    set_end,
    set_breach,
    set_breach_end
)
from .command_handlers import (
    start,
    create_order,
    show_current_order,
    adjust_funds,
    create_attribution,
    list_attributions,
    add_employee,
    remove_employee,
    list_employees
)


__all__ = [
    'start',
    'create_order',
    'show_current_order',
    'adjust_funds',
    'create_attribution',
    'list_attributions',
    'add_employee',
    'remove_employee',
    'list_employees',
    'set_normal',
    'set_overdue',
    'set_end',
    'set_breach',
    'set_breach_end',
    'handle_amount_operation',
    'show_report',
    'search_orders',
    'handle_new_chat_members',
    'handle_new_chat_title',
    'handle_text_input',
    'broadcast_payment',
    'show_gcash',
    'show_paymaya',
    'show_all_accounts',
    'show_schedule_menu',
    'handle_schedule_input'
]
