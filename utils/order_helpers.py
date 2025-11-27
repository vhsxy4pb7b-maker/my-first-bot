"""订单相关工具函数"""
import re
import logging
from datetime import datetime, date, timedelta
from telegram import Update
from telegram.ext import ContextTypes
import db_operations
from constants import HISTORICAL_THRESHOLD_DATE, WEEKDAY_GROUP
from utils.stats_helpers import update_all_stats, update_liquid_capital
from utils.chat_helpers import is_group_chat, get_current_group, reply_in_group

logger = logging.getLogger(__name__)


def get_state_from_title(title: str) -> str:
    """从群名识别订单状态"""
    if '❌' in title:
        return 'breach'
    elif '❗️' in title:
        return 'overdue'
    else:
        return 'normal'


def parse_order_from_title(title: str):
    """从群名解析订单信息"""
    # 规则:
    # 1. 10位数字开头 -> 老客户 (B)
    # 2. A + 10位数字开头 -> 新客户 (A)

    customer = 'B'  # Default
    raw_digits = None
    order_id = None

    # Check for New Customer (A...)
    match_new = re.search(r'^A(\d{10})', title)
    if match_new:
        customer = 'A'
        raw_digits = match_new.group(1)
        order_id = match_new.group(0)  # A + digits as ID
    else:
        # Check for Old Customer (10 digits...)
        match_old = re.search(r'^(\d{10})', title)
        if match_old:
            customer = 'B'
            raw_digits = match_old.group(1)
            order_id = match_old.group(0)

    if not raw_digits:
        return None

    # Parse Date and Amount from the 10 digits
    # Digits: YYMMDDNNKK
    # YYMMDD: Date
    # NN: Seq
    # KK: Amount (k)

    date_part = raw_digits[:6]
    amount_part = raw_digits[8:10]

    try:
        # 假设 20YY
        full_date_str = f"20{date_part}"
        order_date_obj = datetime.strptime(full_date_str, "%Y%m%d").date()
    except ValueError:
        return None

    amount = int(amount_part) * 1000

    return {
        'date': order_date_obj,
        'amount': amount,
        'order_id': order_id,
        'customer': customer,
        'full_date_str': full_date_str
    }


async def update_order_state_from_title(update: Update, context: ContextTypes.DEFAULT_TYPE, order: dict, title: str):
    """根据群名变更自动更新订单状态"""
    current_state = order['state']

    # 1. 完成状态不再更改
    if current_state in ['end', 'breach_end']:
        return

    target_state = get_state_from_title(title)

    # 2. 状态一致无需更改
    if current_state == target_state:
        return

    chat_id = order['chat_id']
    group_id = order['group_id']
    amount = order['amount']
    order_id = order['order_id']

    try:
        # 3. 执行状态变更逻辑
        # 逻辑矩阵:
        # Normal/Overdue -> Breach: 移动统计 (Valid -> Breach)
        # Breach -> Normal/Overdue: 移动统计 (Breach -> Valid)
        # Normal <-> Overdue: 仅更新状态 (都在 Valid 统计下)

        is_current_valid = current_state in ['normal', 'overdue']
        is_target_valid = target_state in ['normal', 'overdue']

        is_current_breach = current_state == 'breach'
        is_target_breach = target_state == 'breach'

        # 更新数据库状态
        if await db_operations.update_order_state(chat_id, target_state):

            # 处理统计数据迁移
            if is_current_valid and is_target_breach:
                # Valid -> Breach
                await update_all_stats('valid', -amount, -1, group_id)
                await update_all_stats('breach', amount, 1, group_id)
                await reply_in_group(update, f"🔄 State Changed: {target_state} (Auto)\nStats moved to Breach.")

            elif is_current_breach and is_target_valid:
                # Breach -> Valid
                await update_all_stats('breach', -amount, -1, group_id)
                await update_all_stats('valid', amount, 1, group_id)
                await reply_in_group(update, f"🔄 State Changed: {target_state} (Auto)\nStats moved to Valid.")

            else:
                # Normal <-> Overdue (都在 Valid 池中，仅状态变更)
                await reply_in_group(update, f"🔄 State Changed: {target_state} (Auto)")

    except Exception as e:
        logger.error(f"Auto update state failed: {e}", exc_info=True)


async def try_create_order_from_title(update: Update, context: ContextTypes.DEFAULT_TYPE, chat, title: str, manual_trigger: bool = False):
    """尝试从群标题创建订单（通用逻辑）"""
    chat_id = chat.id

    # 1. 解析群名 (ID, Customer, Date, Amount)
    parsed_info = parse_order_from_title(title)
    if not parsed_info:
        if manual_trigger:
            await update.message.reply_text(
                "❌ Invalid Group Title Format.\n"
                "Expected:\n"
                "1. Old Customer: 10 digits (e.g., 2401150105)\n"
                "2. New Customer: A + 10 digits (e.g., A2401150105)"
            )
        else:
            logger.info(f"Group title {title} does not match order pattern.")
        return

    # 2. 检查是否已存在订单
    existing_order = await db_operations.get_order_by_chat_id(chat_id)
    if existing_order:
        # 如果是手动触发，提示已存在
        if manual_trigger:
            await update.message.reply_text("⚠️ Order already exists in this group.")
        # 如果是自动触发（改名），则尝试更新状态
        elif not manual_trigger:
            await update_order_state_from_title(update, context, existing_order, title)
        return

    # 3. 提取信息
    order_date = parsed_info['date']
    amount = parsed_info['amount']
    order_id = parsed_info['order_id']
    customer = parsed_info['customer']  # 'A' or 'B'

    # 4. 初始状态识别 (根据群名标志)
    initial_state = get_state_from_title(title)

    # 5. 检查日期阈值 (2025-11-25)
    # 规则: 2025-11-25之前的订单录入规则不变 (作为历史数据导入，不扣款)
    threshold_date = date(*HISTORICAL_THRESHOLD_DATE)
    is_historical = order_date < threshold_date

    # 检查余额 (仅当非历史订单时检查)
    if not is_historical:
        financial_data = await db_operations.get_financial_data()
        if financial_data['liquid_funds'] < amount:
            msg = (
                f"❌ Insufficient Liquid Funds\n"
                f"Current Balance: {financial_data['liquid_funds']:.2f}\n"
                f"Required: {amount:.2f}\n"
                f"Missing: {amount - financial_data['liquid_funds']:.2f}"
            )
            if manual_trigger or is_group_chat(update):
                await update.message.reply_text(msg)
            return

    group_id = 'S01'  # 默认归属
    weekday_group = get_current_group()

    # 构造创建时间
    created_at = f"{order_date.strftime('%Y-%m-%d')} 12:00:00"

    new_order = {
        'order_id': order_id,
        'group_id': group_id,
        'chat_id': chat_id,
        'date': created_at,
        'group': weekday_group,
        'customer': customer,
        'amount': amount,
        'state': initial_state
    }

    # 6. 创建订单
    if not await db_operations.create_order(new_order):
        if manual_trigger:
            await update.message.reply_text("❌ Failed to create order. Order ID might duplicate.")
        return

    # 7. 更新统计
    # 根据初始状态决定计入 Valid 还是 Breach
    is_initial_breach = (initial_state == 'breach')

    if not is_historical:
        # 正常扣款流程

        # 统计金额/数量
        if is_initial_breach:
            await update_all_stats('breach', amount, 1, group_id)
        else:
            await update_all_stats('valid', amount, 1, group_id)

        # 扣除流动资金
        await update_liquid_capital(-amount)

        # 客户统计
        client_field = 'new_clients' if customer == 'A' else 'old_clients'
        await update_all_stats(client_field, amount, 1, group_id)

        msg = (
            f"✅ Order Created Successfully\n\n"
            f"📋 Order ID: {order_id}\n"
            f"🏷️ Group ID: {group_id}\n"
            f"📅 Date: {created_at}\n"
            f"👥 Week Group: {weekday_group}\n"
            f"👤 Customer: {'New' if customer == 'A' else 'Returning'}\n"
            f"💰 Amount: {amount:.2f}\n"
            f"📈 Status: {initial_state}"
        )
        await update.message.reply_text(msg)

        # 自动播报下一期还款
        await send_auto_broadcast(update, context, chat_id, amount)

    else:
        # 历史订单流程 (不扣款)
        if is_initial_breach:
            await update_all_stats('breach', amount, 1, group_id)
        else:
            await update_all_stats('valid', amount, 1, group_id)

        msg = (
            f"✅ Historical Order Imported\n\n"
            f"📋 Order ID: {order_id}\n"
            f"🏷️ Group ID: {group_id}\n"
            f"📅 Date: {created_at}\n"
            f"👤 Customer: {'New' if customer == 'A' else 'Returning'} (Historical)\n"
            f"💰 Amount: {amount:.2f}\n"
            f"📈 Status: {initial_state}\n"
            f"⚠️ Funds Update: Skipped (Historical Data Only)"
        )
        await update.message.reply_text(msg)

        # 历史订单也自动播报
        await send_auto_broadcast(update, context, chat_id, amount)


async def send_auto_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, amount: float):
    """订单创建后自动播报下一期还款"""
    try:
        # 计算本金和本金12%
        principal = amount
        principal_12 = principal * 0.12

        # 计算下一个付款日期（下周五）
        today = datetime.now()
        days_until_friday = (4 - today.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday = 7
        next_friday = today + timedelta(days=days_until_friday)

        # 格式化日期（格式：November 26,2025）
        date_str = next_friday.strftime("%B %d,%Y")
        weekday_str = next_friday.strftime("%A")

        # 格式化金额（添加千位分隔符）
        principal_formatted = f"{principal:,.0f}"
        principal_12_formatted = f"{principal_12:,.0f}"

        # 构建并发送播报消息
        message = (
            f"Your next payment is due on {date_str} ({weekday_str}) "
            f"for {principal_formatted} or {principal_12_formatted} to defer the principal payment for one week.\n\n"
            f"Your outstanding interest is 0"
        )

        await context.bot.send_message(chat_id=chat_id, text=message)
        logger.info(f"自动播报已发送到群组 {chat_id}")
    except Exception as e:
        logger.error(f"自动播报失败: {e}", exc_info=True)
        # 不显示错误给用户，静默失败
