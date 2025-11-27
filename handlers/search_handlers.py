"""搜索相关处理器"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.message_helpers import display_search_results_helper
from decorators import error_handler, authorized_required, private_chat_only

logger = logging.getLogger(__name__)


@error_handler
@private_chat_only
@authorized_required
async def search_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查找订单（支持交互式菜单和旧命令方式）"""
    # 如果没有参数，显示交互式菜单
    if not context.args:
        keyboard = [
            [
                InlineKeyboardButton(
                    "按状态", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "按归属ID", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "按星期分组", callback_data="search_menu_group")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🔍 查找方式:", reply_markup=reply_markup)
        return

    # 如果参数不足2个，提示用法
    if len(context.args) < 2:
        keyboard = [
            [
                InlineKeyboardButton(
                    "按状态", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "按归属ID", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "按星期分组", callback_data="search_menu_group")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🔍 查找方式:", reply_markup=reply_markup)
        return

    search_type = context.args[0].lower()
    orders = []

    # 构建 criteria 字典
    criteria = {}

    try:
        if search_type == 'order_id':
            if len(context.args) < 2:
                await update.message.reply_text("Please provide Order ID")
                return
            criteria['order_id'] = context.args[1]
        elif search_type == 'group_id':
            if len(context.args) < 2:
                await update.message.reply_text("Please provide Group ID")
                return
            criteria['group_id'] = context.args[1]
        elif search_type == 'customer':
            if len(context.args) < 2:
                await update.message.reply_text("Please provide Customer Type (A or B)")
                return
            criteria['customer'] = context.args[1].upper()
        elif search_type == 'state':
            if len(context.args) < 2:
                await update.message.reply_text("Please provide State")
                return
            criteria['state'] = context.args[1]
        elif search_type == 'date':
            if len(context.args) < 3:
                await update.message.reply_text("Please provide Start Date and End Date (Format: YYYY-MM-DD)")
                return
            criteria['date_range'] = (context.args[1], context.args[2])
        elif search_type == 'group':  # 支持按群组(星期)查找
            if len(context.args) < 2:
                await update.message.reply_text("Please provide Group (e.g., Mon, Tue)")
                return
            val = context.args[1]
            if val.startswith('周') and len(val) == 2:
                val = val[1]
            criteria['weekday_group'] = val
        else:
            await update.message.reply_text(f"Unknown search type: {search_type}")
            return

        orders = await db_operations.search_orders_advanced(criteria)
        await display_search_results_helper(update, context, orders)

    except Exception as e:
        logger.error(f"搜索订单时出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ Search Error: {str(e)}")
