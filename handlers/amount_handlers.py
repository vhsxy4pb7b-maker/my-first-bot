"""金额操作处理器"""
import os
import sys
from pathlib import Path

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import logging
from telegram import Update
from telegram.ext import ContextTypes
import db_operations
from utils.chat_helpers import is_group_chat
from utils.stats_helpers import update_all_stats, update_liquid_capital
from config import ADMIN_IDS

logger = logging.getLogger(__name__)


async def handle_amount_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理金额操作（需要管理员权限）"""
    # 检查是否在群组中
    if not is_group_chat(update):
        return

    # 检查是否有消息对象
    if not update.message or not update.message.text:
        return

    # 权限检查
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return

    # 检查是否是管理员或授权用户
    is_admin = user_id in ADMIN_IDS
    is_authorized = await db_operations.is_user_authorized(user_id)

    if not is_admin and not is_authorized:
        logger.debug(f"用户 {user_id} 无权限执行快捷操作")
        return  # 无权限不处理

    chat_id = update.message.chat_id
    text = update.message.text.strip()

    logger.info(f"收到快捷操作消息: {text} (用户: {user_id}, 群组: {chat_id})")

    # 只处理以 + 开头的消息（快捷操作）
    if not text.startswith('+'):
        return  # 不是快捷操作格式，不处理

    # 检查是否有订单（利息收入不需要订单）
    order = await db_operations.get_order_by_chat_id(chat_id)

    # 解析金额和操作类型
    try:
        # 去掉加号后的文本
        amount_text = text[1:].strip()

        if not amount_text:
            message = "❌ Failed: Please enter amount (e.g., +1000 or +1000b)"
            await update.message.reply_text(message)
            return

        if amount_text.endswith('b'):
            # 本金减少 - 需要订单
            if not order:
                message = "❌ Failed: No active order in this group."
                await update.message.reply_text(message)
                return
            amount = float(amount_text[:-1])
            await process_principal_reduction(update, order, amount)
        else:
            # 利息收入 - 不需要订单，但如果有订单会关联到订单的归属ID
            try:
                amount = float(amount_text)
                if order:
                    # 如果有订单，关联到订单的归属ID
                    await process_interest(update, order, amount)
                else:
                    # 如果没有订单，更新全局和日结数据
                    await update_all_stats('interest', amount, 0, None)
                    await update_liquid_capital(amount)
                    # 群组只回复成功，私聊显示详情
                    if is_group_chat(update):
                        await update.message.reply_text("✅ Success")
                    else:
                        financial_data = await db_operations.get_financial_data()
                        await update.message.reply_text(
                            f"✅ Interest Recorded!\n"
                            f"Amount: {amount:.2f}\n"
                            f"Total Interest: {financial_data['interest']:.2f}"
                        )
            except ValueError:
                message = "❌ Failed: Invalid amount format."
                await update.message.reply_text(message)
    except ValueError:
        message = "❌ Failed: Invalid format. Example: +1000 or +1000b"
        await update.message.reply_text(message)
    except Exception as e:
        logger.error(f"处理金额操作时出错: {e}", exc_info=True)
        message = "❌ Failed: An error occurred."
        await update.message.reply_text(message)


async def process_principal_reduction(update: Update, order: dict, amount: float):
    """处理本金减少"""
    try:
        if order['state'] not in ('normal', 'overdue'):
            message = "❌ Failed: Order state not allowed."
            await update.message.reply_text(message)
            return

        if amount <= 0:
            message = "❌ Failed: Amount must be positive."
            await update.message.reply_text(message)
            return

        if amount > order['amount']:
            message = f"❌ Failed: Exceeds order amount ({order['amount']:.2f})"
            await update.message.reply_text(message)
            return

        # 更新订单金额
        new_amount = order['amount'] - amount
        if not await db_operations.update_order_amount(order['chat_id'], new_amount):
            message = "❌ Failed: DB Error"
            await update.message.reply_text(message)
            return

        group_id = order['group_id']

        # 1. 有效金额减少
        await update_all_stats('valid', -amount, 0, group_id)

        # 2. 完成金额增加
        await update_all_stats('completed', amount, 0, group_id)

        # 3. 流动资金增加
        await update_liquid_capital(amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text(f"✅ Principal Reduced: {amount:.2f}\nRemaining: {new_amount:.2f}")
        else:
            await update.message.reply_text(
                f"✅ Principal Reduced Successfully!\n"
                f"Order ID: {order['order_id']}\n"
                f"Reduced Amount: {amount:.2f}\n"
                f"Remaining Amount: {new_amount:.2f}"
            )
    except Exception as e:
        logger.error(f"处理本金减少时出错: {e}", exc_info=True)
        message = "❌ Error processing request."
        await update.message.reply_text(message)


async def process_interest(update: Update, order: dict, amount: float):
    """处理利息收入"""
    try:
        if amount <= 0:
            message = "❌ Failed: Amount must be positive."
            await update.message.reply_text(message)
            return

        group_id = order['group_id']

        # 1. 利息收入
        await update_all_stats('interest', amount, 0, group_id)

        # 2. 流动资金增加
        await update_liquid_capital(amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text("✅ Interest Received")
        else:
            financial_data = await db_operations.get_financial_data()
            await update.message.reply_text(
                f"✅ Interest Recorded!\n"
                f"Amount: {amount:.2f}\n"
                f"Total Interest: {financial_data['interest']:.2f}"
            )
    except Exception as e:
        logger.error(f"处理利息收入时出错: {e}", exc_info=True)
        message = "❌ Error processing request."
        await update.message.reply_text(message)


