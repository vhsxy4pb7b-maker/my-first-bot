"""播报功能处理器"""
import logging
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ContextTypes
import db_operations
from utils.chat_helpers import is_group_chat
from decorators import authorized_required, group_chat_only

logger = logging.getLogger(__name__)


@authorized_required
@group_chat_only
async def broadcast_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """播报付款提醒命令（群聊）- 直接发送模板消息"""
    # 检查是否有订单
    chat_id = update.message.chat_id
    order = await db_operations.get_order_by_chat_id(chat_id)
    
    if not order:
        await update.message.reply_text("❌ 当前群组没有活跃订单")
        return
    
    # 从订单获取本金
    principal = order.get('amount', 0)
    principal_12 = principal * 0.12
    
    # 计算下一个付款日期（下周五）
    today = datetime.now()
    days_until_friday = (4 - today.weekday()) % 7
    if days_until_friday == 0:
        days_until_friday = 7
    next_friday = today + timedelta(days=days_until_friday)
    
    # 格式化日期（格式：October 31,2025）
    date_str = next_friday.strftime("%B %d,%Y")
    weekday_str = next_friday.strftime("%A")
    
    # 格式化金额（添加千位分隔符）
    principal_formatted = f"{principal:,.0f}"
    principal_12_formatted = f"{principal_12:,.0f}"
    
    # 构建并发送模板消息
    message = (
        f"Your next payment is due on {date_str} ({weekday_str}) "
        f"for {principal_formatted} or {principal_12_formatted} to defer the principal payment for one week."
    )
    
    try:
        await context.bot.send_message(chat_id=chat_id, text=message)
        # 不发送任何回复，静默完成
    except Exception as e:
        logger.error(f"发送播报消息失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 发送失败: {e}")


async def handle_broadcast_payment_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理播报输入"""
    # 检查取消
    if text.lower() == 'cancel':
        context.user_data['state'] = None
        context.user_data['broadcast_step'] = None
        context.user_data['broadcast_data'] = {}
        await update.message.reply_text("✅ 操作已取消")
        return
    
    step = context.user_data.get('broadcast_step', 1)
    data = context.user_data.get('broadcast_data', {})
    
    if step == 1:
        # 输入本金
        try:
            principal = float(text)
            if principal <= 0:
                await update.message.reply_text("❌ 本金必须大于0")
                return
            data['principal'] = principal
            context.user_data['broadcast_data'] = data
            context.user_data['broadcast_step'] = 2
            await update.message.reply_text(
                f"✅ 本金已设置: {principal:.2f}\n\n"
                "请输入本金12%金额（或输入 'auto' 自动计算）:"
            )
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字")
    
    elif step == 2:
        # 输入本金12%
        try:
            if text.lower() == 'auto':
                principal = data.get('principal', 0)
                principal_12 = principal * 0.12
            else:
                principal_12 = float(text)
                if principal_12 <= 0:
                    await update.message.reply_text("❌ 金额必须大于0")
                    return
            data['principal_12'] = principal_12
            context.user_data['broadcast_data'] = data
            context.user_data['broadcast_step'] = 3
            await update.message.reply_text(
                f"✅ 本金12%已设置: {principal_12:.2f}\n\n"
                "请输入未付利息（员工输入）:"
            )
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字或 'auto'")
    
    elif step == 3:
        # 输入未付利息
        try:
            outstanding_interest = float(text)
            if outstanding_interest < 0:
                await update.message.reply_text("❌ 利息不能为负数")
                return
            data['outstanding_interest'] = outstanding_interest
            
            # 生成并发送播报消息
            await send_broadcast_message(update, context, data)
            
            # 清除状态
            context.user_data['state'] = None
            context.user_data['broadcast_step'] = None
            context.user_data['broadcast_data'] = {}
        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字")


async def send_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict):
    """发送播报消息"""
    principal = data.get('principal', 0)
    principal_12 = data.get('principal_12', 0)
    outstanding_interest = data.get('outstanding_interest', 0)
    
    # 计算下一个付款日期（假设是下周五）
    today = datetime.now()
    days_until_friday = (4 - today.weekday()) % 7
    if days_until_friday == 0:
        days_until_friday = 7
    next_friday = today + timedelta(days=days_until_friday)
    
    # 格式化日期
    date_str = next_friday.strftime("%B %d, %Y")
    weekday_str = next_friday.strftime("%A")
    
    # 构建消息 - 发送本金版本
    message = (
        f"Your next payment is due on {date_str} ({weekday_str}) "
        f"for {principal:.2f} to defer the principal payment for one week.\n\n"
        f"Your outstanding interest is {outstanding_interest:.2f}."
    )
    
    # 发送消息到当前群组
    try:
        await context.bot.send_message(chat_id=update.message.chat_id, text=message)
        
        # 保存数据到context，用于后续发送
        context.user_data['broadcast_principal_12'] = principal_12
        context.user_data['broadcast_outstanding_interest'] = outstanding_interest
        context.user_data['broadcast_date_str'] = date_str
        context.user_data['broadcast_weekday_str'] = weekday_str
        
        # 询问是否发送本金12%版本
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = [
            [
                InlineKeyboardButton(
                    f"发送本金12%版本 ({principal_12:.2f})", 
                    callback_data="broadcast_send_12")
            ],
            [
                InlineKeyboardButton("完成", callback_data="broadcast_done")
            ]
        ]
        await update.message.reply_text(
            f"✅ 本金版本已发送\n\n"
            f"是否发送本金12%版本 ({principal_12:.2f})？",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"发送播报消息失败: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 发送失败: {e}")

