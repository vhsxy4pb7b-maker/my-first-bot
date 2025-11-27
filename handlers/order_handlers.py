"""订单状态处理相关命令"""
import logging
from telegram import Update
from telegram.ext import ContextTypes
import db_operations
from utils.chat_helpers import is_group_chat
from utils.stats_helpers import update_all_stats, update_liquid_capital
from decorators import authorized_required, group_chat_only

logger = logging.getLogger(__name__)


@authorized_required
@group_chat_only
async def set_normal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """转为正常状态"""
    try:
        # 兼容 CallbackQuery
        if update.message:
            chat_id = update.message.chat_id
            reply_func = update.message.reply_text
        elif update.callback_query:
            chat_id = update.callback_query.message.chat_id
            reply_func = update.callback_query.message.reply_text
        else:
            return

        order = await db_operations.get_order_by_chat_id(chat_id)
        if not order:
            message = "❌ Failed: No active order."
            await reply_func(message)
            return

        if order['state'] != 'overdue':
            message = "❌ Failed: Order must be overdue."
            await reply_func(message)
            return

        if not await db_operations.update_order_state(chat_id, 'normal'):
            message = "❌ Failed: DB Error"
            await reply_func(message)
            return

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await reply_func(f"✅ Status Updated: normal\nOrder ID: {order['order_id']}")
        else:
            await reply_func(
                f"✅ Status Updated: normal\n"
                f"Order ID: {order['order_id']}\n"
                f"State: normal"
            )
    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}", exc_info=True)
        message = "❌ Error processing request."
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query:
            await update.callback_query.message.reply_text(message)


@authorized_required
@group_chat_only
async def set_overdue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """转为逾期状态"""
    try:
        # 兼容 CallbackQuery
        if update.message:
            chat_id = update.message.chat_id
            reply_func = update.message.reply_text
        elif update.callback_query:
            chat_id = update.callback_query.message.chat_id
            reply_func = update.callback_query.message.reply_text
        else:
            return

        order = await db_operations.get_order_by_chat_id(chat_id)
        if not order:
            message = "❌ Failed: No active order."
            await reply_func(message)
            return

        if order['state'] != 'normal':
            message = "❌ Failed: Order must be normal."
            await reply_func(message)
            return

        if not await db_operations.update_order_state(chat_id, 'overdue'):
            message = "❌ Failed: DB Error"
            await reply_func(message)
            return

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await reply_func(f"✅ Status Updated: overdue\nOrder ID: {order['order_id']}")
        else:
            await reply_func(
                f"✅ Status Updated: overdue\n"
                f"Order ID: {order['order_id']}\n"
                f"State: overdue"
            )
    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}", exc_info=True)
        message = "❌ Error processing request."
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query:
            await update.callback_query.message.reply_text(message)


@authorized_required
@group_chat_only
async def set_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """标记订单为完成"""
    # 兼容 CallbackQuery
    if update.message:
        chat_id = update.message.chat_id
        reply_func = update.message.reply_text
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        reply_func = update.callback_query.message.reply_text
    else:
        return

    order = await db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed: No active order."
        await reply_func(message)
        return

    if order['state'] not in ('normal', 'overdue'):
        message = "❌ Failed: State must be normal or overdue."
        await reply_func(message)
        return

    # 更新订单状态
    await db_operations.update_order_state(chat_id, 'end')
    group_id = order['group_id']
    amount = order['amount']

    # 1. 有效订单减少
    await update_all_stats('valid', -amount, -1, group_id)

    # 2. 完成订单增加
    await update_all_stats('completed', amount, 1, group_id)

    # 3. 流动资金增加
    await update_liquid_capital(amount)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await reply_func(f"✅ Order Completed\nAmount: {amount:.2f}")
    else:
        await reply_func(
            f"✅ Order Completed!\n"
            f"Order ID: {order['order_id']}\n"
            f"Amount: {amount:.2f}"
        )


@authorized_required
@group_chat_only
async def set_breach(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """标记为违约"""
    # 兼容 CallbackQuery
    if update.message:
        chat_id = update.message.chat_id
        reply_func = update.message.reply_text
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        reply_func = update.callback_query.message.reply_text
    else:
        return

    order = await db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed: No active order."
        await reply_func(message)
        return

    if order['state'] != 'overdue':
        message = "❌ Failed: Order must be overdue."
        await reply_func(message)
        return

    # 更新订单状态
    await db_operations.update_order_state(chat_id, 'breach')
    group_id = order['group_id']
    amount = order['amount']

    # 1. 有效订单减少
    await update_all_stats('valid', -amount, -1, group_id)

    # 2. 违约订单增加
    await update_all_stats('breach', amount, 1, group_id)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await reply_func(f"✅ Marked as Breach\nAmount: {amount:.2f}")
    else:
        await reply_func(
            f"✅ Order Marked as Breach!\n"
            f"Order ID: {order['order_id']}\n"
            f"Amount: {amount:.2f}"
        )


@authorized_required
@group_chat_only
async def set_breach_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """违约订单完成 - 请求金额"""
    # 兼容 CallbackQuery
    if update.message:
        chat_id = update.message.chat_id
        reply_func = update.message.reply_text
        # 参数仅在 CommandHandler 时存在
        args = context.args
    elif update.callback_query:
        chat_id = update.callback_query.message.chat_id
        reply_func = update.callback_query.message.reply_text
        args = None
    else:
        return

    order = await db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed: No active order."
        await reply_func(message)
        return

    if order['state'] != 'breach':
        message = "❌ Failed: Order must be in breach."
        await reply_func(message)
        return

    # 检查是否直接提供了金额参数 (仅限命令方式)
    if args and len(args) > 0:
        try:
            amount = float(args[0])
            if amount <= 0:
                await reply_func("❌ Amount must be positive.")
                return

            # 直接执行完成逻辑
            await db_operations.update_order_state(chat_id, 'breach_end')
            group_id = order['group_id']

            # 违约完成订单增加，金额增加
            await update_all_stats('breach_end', amount, 1, group_id)

            # 更新流动资金 (Liquid Flow & Cash Balance)
            await update_liquid_capital(amount)

            msg_en = f"✅ Breach Order Ended\nAmount: {amount:.2f}"

            if is_group_chat(update):
                await reply_func(msg_en)
            else:
                await reply_func(msg_en + f"\nOrder ID: {order['order_id']}")
            return

        except ValueError:
            await reply_func("❌ Invalid amount format.")
            return

    # 询问金额 (如果没有提供参数)
    if is_group_chat(update):
        await reply_func(
            "Please enter the final amount for this breach order (e.g., 5000).\n"
            "This amount will be recorded as liquid capital inflow."
        )
    else:
        await reply_func("Please enter the final amount for breach order:")

    # 设置状态，等待输入
    context.user_data['state'] = 'WAITING_BREACH_END_AMOUNT'
    context.user_data['breach_end_chat_id'] = chat_id


