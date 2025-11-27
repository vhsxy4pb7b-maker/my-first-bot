"""命令处理器"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.chat_helpers import is_group_chat
from utils.order_helpers import try_create_order_from_title
from utils.stats_helpers import update_liquid_capital, update_all_stats
from utils.date_helpers import get_daily_period_date
from utils.message_helpers import display_search_results_helper
from decorators import error_handler, admin_required, authorized_required, private_chat_only, group_chat_only

logger = logging.getLogger(__name__)


@error_handler
@private_chat_only
@authorized_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """发送欢迎消息"""
    financial_data = await db_operations.get_financial_data()

    await update.message.reply_text(
        "📋 订单管理系统\n\n"
        "💰 当前流动资金: {:.2f}\n\n"
        "📝 订单操作:\n"
        "/create - 读取群名创建新订单\n"
        "/order - 管理当前订单\n\n"
        "⚡ 快捷操作 (在订单群):\n"
        "+<金额>b - 减少本金\n"
        "+<金额> - 利息收入\n\n"
        "🔄 状态变更:\n"
        "/normal - 设为正常\n"
        "/overdue - 设为逾期\n"
        "/end - 标记为完成\n"
        "/breach - 标记为违约\n"
        "/breach_end - 违约完成\n\n"
        "📊 查询:\n"
        "/report [归属ID] - 查看报表\n"
        "/search <类型> <值> - 搜索订单\n"
        "  类型: order_id/group_id/customer/state/date\n\n"
        "📢 播报:\n"
        "/broadcast - 播报付款提醒（群聊）\n"
        "/schedule - 管理定时播报（最多3个）\n\n"
        "💳 支付账号:\n"
        "/accounts - 查看所有账户数据表格\n"
        "/gcash - 查看GCASH账号\n"
        "/paymaya - 查看PayMaya账号\n\n"
        "⚙️ 管理:\n"
        "/adjust <金额> [备注] - 调整资金\n"
        "/create_attribution <ID> - 创建归属ID\n"
        "/list_attributions - 列出归属ID\n"
        "/add_employee <ID> - 添加员工\n"
        "/remove_employee <ID> - 移除员工\n"
        "/list_employees - 列出员工\n\n"
        "⚠️ 部分操作需要管理员权限".format(
            financial_data['liquid_funds'])
    )


@authorized_required
@group_chat_only
async def create_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """创建新订单 (读取群名)"""
    chat = update.effective_chat
    title = chat.title
    if not title:
        await update.message.reply_text("❌ Cannot get group title.")
        return

    await try_create_order_from_title(update, context, chat, title, manual_trigger=True)


@authorized_required
@group_chat_only
async def show_current_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示当前订单状态和操作菜单"""
    # 支持 CommandHandler 和 CallbackQueryHandler
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
        await reply_func("❌ No active order in this group.\nUse /create to start a new order.")
        return

    # 构建订单信息
    msg = (
        f"📋 Current Order Status:\n"
        f"──────────────────\n"
        f"📝 Order ID: `{order['order_id']}`\n"
        f"🏷️ Group ID: `{order['group_id']}`\n"
        f"📅 Date: {order['date']}\n"
        f"👥 Week Group: {order['weekday_group']}\n"
        f"👤 Customer: {order['customer']}\n"
        f"💰 Amount: {order['amount']:.2f}\n"
        f"📊 State: {order['state']}\n"
        f"──────────────────"
    )

    # 构建操作按钮
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ 正常", callback_data="order_action_normal"),
            InlineKeyboardButton(
                "⚠️ 逾期", callback_data="order_action_overdue")
        ],
        [
            InlineKeyboardButton("🏁 完成", callback_data="order_action_end"),
            InlineKeyboardButton(
                "🚫 违约", callback_data="order_action_breach")
        ],
        [
            InlineKeyboardButton(
                "💸 违约完成", callback_data="order_action_breach_end")
        ],
        [
            InlineKeyboardButton(
                "💳 发送账户", callback_data="payment_select_account")
        ]
    ]

    await reply_func(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


@error_handler
@admin_required
@private_chat_only
async def adjust_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """调整流动资金余额命令"""
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "❌ 用法: /adjust <金额> [备注]\n"
            "示例: /adjust +5000 收入备注\n"
            "      /adjust -3000 支出备注"
        )
        return

    amount_str = context.args[0]
    note = " ".join(context.args[1:]) if len(context.args) > 1 else "无备注"

    # 验证金额格式
    if not (amount_str.startswith('+') or amount_str.startswith('-')):
        await update.message.reply_text("❌ 金额格式错误，请使用+100或-200格式")
        return

    amount = float(amount_str)
    if amount == 0:
        await update.message.reply_text("❌ 调整金额不能为0")
        return

    # 更新财务数据
    await update_liquid_capital(amount)

    financial_data = await db_operations.get_financial_data()
    await update.message.reply_text(
        f"✅ 资金调整成功\n"
        f"调整类型: {'增加' if amount > 0 else '减少'}\n"
        f"调整金额: {abs(amount):.2f}\n"
        f"调整后余额: {financial_data['liquid_funds']:.2f}\n"
        f"备注: {note}"
    )


@admin_required
@private_chat_only
async def create_attribution(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """创建新的归属ID"""
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ 用法: /create_attribution <归属ID>\n示例: /create_attribution S03")
        return

    group_id = context.args[0].upper()

    # 验证格式
    if len(group_id) != 3 or not group_id[0].isalpha() or not group_id[1:].isdigit():
        await update.message.reply_text("❌ 格式错误，正确格式：字母+两位数字（如S01）")
        return

    # 检查是否已存在
    existing_groups = await db_operations.get_all_group_ids()
    if group_id in existing_groups:
        await update.message.reply_text(f"⚠️ 归属ID {group_id} 已存在")
        return

    # 创建分组数据记录
    await db_operations.update_grouped_data(group_id, 'valid_orders', 0)
    await update.message.reply_text(f"✅ 成功创建归属ID {group_id}")


@admin_required
@private_chat_only
async def list_attributions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有归属ID"""
    group_ids = await db_operations.get_all_group_ids()

    if not group_ids:
        await update.message.reply_text("暂无归属ID，使用 /create_attribution <ID> 创建")
        return

    message = "📋 所有归属ID:\n\n"
    for i, group_id in enumerate(sorted(group_ids), 1):
        data = await db_operations.get_grouped_data(group_id)
        message += (
            f"{i}. {group_id}\n"
            f"   有效订单: {data['valid_orders']} | "
            f"金额: {data['valid_amount']:.2f}\n"
        )

    await update.message.reply_text(message)


@admin_required
@private_chat_only
async def add_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加员工（授权用户）"""
    if not context.args:
        await update.message.reply_text("❌ 用法: /add_employee <用户ID>")
        return

    try:
        user_id = int(context.args[0])
        if await db_operations.add_authorized_user(user_id):
            await update.message.reply_text(f"✅ 已添加员工: {user_id}")
        else:
            await update.message.reply_text("⚠️ 添加失败或用户已存在")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


@admin_required
@private_chat_only
async def remove_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """移除员工（授权用户）"""
    if not context.args:
        await update.message.reply_text("❌ 用法: /remove_employee <用户ID>")
        return

    try:
        user_id = int(context.args[0])
        if await db_operations.remove_authorized_user(user_id):
            await update.message.reply_text(f"✅ 已移除员工: {user_id}")
        else:
            await update.message.reply_text("⚠️ 移除失败或用户不存在")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


@admin_required
@private_chat_only
async def list_employees(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有员工"""
    users = await db_operations.get_authorized_users()
    if not users:
        await update.message.reply_text("📋 暂无授权员工")
        return

    message = "📋 授权员工列表:\n\n"
    for uid in users:
        message += f"👤 `{uid}`\n"

    await update.message.reply_text(message, parse_mode='Markdown')
