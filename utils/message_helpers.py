"""消息处理相关工具函数"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.chat_helpers import is_group_chat

logger = logging.getLogger(__name__)


async def display_search_results_helper(update: Update, context: ContextTypes.DEFAULT_TYPE, orders: list):
    """辅助函数：显示搜索结果"""
    if not orders:
        if update.callback_query:
            await update.callback_query.message.reply_text("❌ 未找到匹配的订单")
        else:
            await update.message.reply_text("❌ 未找到匹配的订单")
        return

    # 锁定群组
    locked_groups = list(set(order['chat_id'] for order in orders))
    context.user_data['locked_groups'] = locked_groups

    # 保存查找结果到context，用于后续修改归属
    context.user_data['search_orders'] = orders

    # 计算订单数量和金额
    order_count = len(orders)
    total_amount = sum(order.get('amount', 0) for order in orders)

    result_msg = (
        f"📊 查找结果\n\n"
        f"订单数量: {order_count}\n"
        f"订单金额: {total_amount:,.2f}\n"
        f"群组数量: {len(locked_groups)}\n\n"
        f"✅ 已锁定 {len(locked_groups)} 个群组，可用于群发消息"
    )

    # 添加操作按钮
    keyboard = [
        [
            InlineKeyboardButton("📢 群发消息", callback_data="broadcast_start"),
            InlineKeyboardButton(
                "🔄 更改归属", callback_data="search_change_attribution")
        ]
    ]

    # 确定发送消息的方法
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                result_msg, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            # 如果无法编辑消息，则发送新消息
            await update.callback_query.message.reply_text(
                result_msg, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(
            result_msg, reply_markup=InlineKeyboardMarkup(keyboard))
