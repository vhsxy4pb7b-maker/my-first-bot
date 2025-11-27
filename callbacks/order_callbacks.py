"""订单操作回调处理器"""
from telegram import Update
from telegram.ext import ContextTypes
from handlers.order_handlers import (
    set_normal, set_overdue, set_end, set_breach, set_breach_end
)


async def handle_order_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理订单操作的回调"""
    query = update.callback_query

    # 获取原始数据
    action = query.data.replace("order_action_", "")

    if action == "normal":
        await set_normal(update, context)
    elif action == "overdue":
        await set_overdue(update, context)
    elif action == "end":
        await set_end(update, context)
    elif action == "breach":
        await set_breach(update, context)
    elif action == "breach_end":
        await set_breach_end(update, context)
    elif action == "create":
        # create 命令需要参数，这里只能提示用法
        await query.message.reply_text("To create an order, please use command: /create <Group ID> <Customer A/B> <Amount>")

    # 尝试 answer callback，消除加载状态
    try:
        await query.answer()
    except:
        pass


