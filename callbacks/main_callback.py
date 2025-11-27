"""主回调处理器"""
import logging
from telegram import Update
from telegram.ext import ContextTypes
from callbacks.report_callbacks import handle_report_callback
from callbacks.search_callbacks import handle_search_callback
from callbacks.payment_callbacks import handle_payment_callback
from decorators import authorized_required

logger = logging.getLogger(__name__)


@authorized_required
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """主按钮回调入口"""
    query = update.callback_query

    # 必须先 answer，防止客户端转圈
    try:
        await query.answer()
    except Exception:
        pass  # 忽略 answer 错误（例如 query 已过期）

    data = query.data

    # 记录日志以便排查
    logger.info(
        f"Processing callback: {data} from user {update.effective_user.id}")

    if data.startswith("search_"):
        await handle_search_callback(update, context)
    elif data.startswith("report_"):
        await handle_report_callback(update, context)
    elif data.startswith("payment_"):
        await handle_payment_callback(update, context)
    elif data == "broadcast_start":
        locked_groups = context.user_data.get('locked_groups', [])
        if not locked_groups:
            await query.message.reply_text("⚠️ 没有锁定的群组。请先使用查找功能锁定群组。")
            return

        await query.message.reply_text(
            f"📢 准备向 {len(locked_groups)} 个群组发送消息。\n"
            "请输入消息内容：\n"
            "（输入 'cancel' 取消）"
        )
        context.user_data['state'] = 'BROADCASTING'
    elif data == "broadcast_send_12":
        # 处理发送本金12%版本
        principal_12 = context.user_data.get('broadcast_principal_12', 0)
        outstanding_interest = context.user_data.get(
            'broadcast_outstanding_interest', 0)
        date_str = context.user_data.get('broadcast_date_str', '')
        weekday_str = context.user_data.get('broadcast_weekday_str', 'Friday')

        if principal_12 == 0:
            await query.answer("❌ 数据错误")
            return

        message = (
            f"Your next payment is due on {date_str} ({weekday_str}) "
            f"for {principal_12:.2f} to defer the principal payment for one week.\n\n"
            f"Your outstanding interest is {outstanding_interest:.2f}."
        )

        try:
            await context.bot.send_message(chat_id=query.message.chat_id, text=message)
            await query.answer("✅ 本金12%版本已发送")
            await query.edit_message_text("✅ 播报完成")
            # 清除临时数据
            context.user_data.pop('broadcast_principal_12', None)
            context.user_data.pop('broadcast_outstanding_interest', None)
            context.user_data.pop('broadcast_date_str', None)
            context.user_data.pop('broadcast_weekday_str', None)
        except Exception as e:
            logger.error(f"发送播报消息失败: {e}", exc_info=True)
            await query.answer(f"❌ 发送失败: {e}")
    elif data == "broadcast_done":
        await query.answer("✅ 播报完成")
        await query.edit_message_text("✅ 播报完成")
        # 清除临时数据
        context.user_data.pop('broadcast_principal_12', None)
        context.user_data.pop('broadcast_outstanding_interest', None)
        context.user_data.pop('broadcast_date_str', None)
        context.user_data.pop('broadcast_weekday_str', None)
    else:
        logger.warning(f"Unhandled callback data: {data}")
        await query.message.reply_text(f"⚠️ 未知的操作: {data}")
