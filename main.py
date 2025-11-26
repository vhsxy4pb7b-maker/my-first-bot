import logging
from datetime import datetime, date
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import error as telegram_error
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler
)
from functools import wraps
import os
import db_operations
import init_db

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# 获取配置（优先从环境变量，其次从配置文件）
def load_config():
    """加载配置，优先从环境变量，其次从config.py文件"""
    # 先尝试从环境变量读取
    token = os.getenv("BOT_TOKEN")
    admin_ids_str = os.getenv("ADMIN_USER_IDS", "")

    # 如果环境变量没有，尝试从config.py读取
    if not token or not admin_ids_str:
        try:
            import config
            token = token or getattr(config, 'BOT_TOKEN', None)
            admin_ids_str = admin_ids_str or getattr(
                config, 'ADMIN_USER_IDS', '')
        except ImportError:
            pass

    # 验证token
    if not token:
        raise ValueError(
            "BOT_TOKEN 未设置！\n"
            "请选择以下方式之一设置：\n"
            "1. 设置环境变量 BOT_TOKEN\n"
            "2. 创建 config.py 文件，添加：BOT_TOKEN = '你的token'\n"
            "3. 或直接修改 main.py 中的配置（不推荐）"
        )

    # 解析管理员ID
    admin_ids = [int(id.strip())
                 for id in admin_ids_str.split(",") if id.strip()]

    if not admin_ids:
        raise ValueError(
            "ADMIN_USER_IDS 未设置！\n"
            "请选择以下方式之一设置：\n"
            "1. 设置环境变量 ADMIN_USER_IDS（多个ID用逗号分隔）\n"
            "2. 创建 config.py 文件，添加：ADMIN_USER_IDS = '你的用户ID1,你的用户ID2'\n"
            "3. 或直接修改 main.py 中的配置（不推荐）"
        )

    return token, admin_ids


token, ADMIN_IDS = load_config()


def error_handler(func):
    """
    统一错误处理装饰器
    自动捕获异常并向用户发送错误消息
    """
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            error_msg = f"⚠️ Operation Failed: {str(e)}"

            # 尝试回复用户
            try:
                if update.callback_query:
                    await update.callback_query.message.reply_text(error_msg)
                elif update.message:
                    await update.message.reply_text(error_msg)
            except Exception as send_error:
                logger.error(f"Failed to send error message: {send_error}")
    return wrapped


# 星期分组映射
WEEKDAY_GROUP = {
    0: '一',  # Monday
    1: '二',  # Tuesday
    2: '三',  # Wednesday
    3: '四',  # Thursday
    4: '五',  # Friday
    5: '六',  # Saturday
    6: '日'   # Sunday
}


def admin_required(func):
    """检查用户是否是管理员的装饰器"""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        # 检查是否有消息对象
        if not update.message and not update.callback_query:
            return

        # 获取用户ID
        user_id = update.effective_user.id if update.effective_user else None

        if not user_id or user_id not in ADMIN_IDS:
            error_msg = "⚠️ Admin permission required."
            if update.message:
                await update.message.reply_text(error_msg)
            elif update.callback_query:
                await update.callback_query.answer(error_msg, show_alert=True)
            return

        return await func(update, context, *args, **kwargs)
    return wrapped


def authorized_required(func):
    """检查用户是否有操作权限（管理员或员工）"""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        # 检查是否有消息对象
        if not update.message and not update.callback_query:
            return

        # 获取用户ID
        user_id = update.effective_user.id if update.effective_user else None

        if not user_id:
            return

        # 检查是否是管理员
        if user_id in ADMIN_IDS:
            return await func(update, context, *args, **kwargs)

        # 检查是否是授权员工
        if await db_operations.is_user_authorized(user_id):
            return await func(update, context, *args, **kwargs)

        error_msg = "⚠️ Permission denied."
        if update.message:
            await update.message.reply_text(error_msg)
        elif update.callback_query:
            await update.callback_query.answer(error_msg, show_alert=True)
        return

    return wrapped


def private_chat_only(func):
    """检查是否在私聊中使用命令的装饰器"""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_chat.type != "private":
            await update.message.reply_text("⚠️ This command can only be used in private chat.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


def group_chat_only(func):
    """检查是否在群组中使用命令的装饰器"""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not is_group_chat(update):
            await update.message.reply_text("⚠️ This command can only be used in group chat.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


def get_current_group():
    """获取当前星期对应的分组"""
    today = date.today().weekday()
    return WEEKDAY_GROUP[today]


def is_group_chat(update: Update) -> bool:
    """判断是否是群组聊天"""
    return update.effective_chat.type in ['group', 'supergroup']


def reply_in_group(update: Update, message: str):
    """在群组中回复消息（英语）"""
    if is_group_chat(update):
        return update.message.reply_text(message)
    else:
        # 私聊保持中文
        return update.message.reply_text(message)


def get_daily_period_date() -> str:
    """获取当前日结周期对应的日期（每天23:00日切）"""
    from datetime import datetime, timedelta
    import pytz

    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    current_hour = now.hour

    # 如果当前时间 >= 23:00，算作明天
    if current_hour >= 23:
        period_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        period_date = now.strftime("%Y-%m-%d")

    return period_date


async def update_liquid_capital(amount: float):
    """更新流动资金（全局余额 + 日结流量）"""
    # 1. 全局余额 (Cash Balance)
    await db_operations.update_financial_data('liquid_funds', amount)

    # 2. 日结流量 (Liquid Flow)
    date = get_daily_period_date()
    await db_operations.update_daily_data(date, 'liquid_flow', amount, None)


async def update_all_stats(field: str, amount: float, count: int = 0, group_id: str = None):
    """
    统一更新所有统计数据（全局、日结、分组）
    :param field: 字段名（不含_amount/orders后缀的基础名，或者完整字段名）
                  例如 'new_clients' 或 'valid'
    :param amount: 金额变动
    :param count: 数量变动
    :param group_id: 归属ID
    """
    # 1. 更新全局财务数据
    if amount != 0:
        # 处理特殊字段名映射
        global_amount_field = field if field.endswith('_amount') or field in [
            'liquid_funds', 'interest'] else f"{field}_amount"
        await db_operations.update_financial_data(global_amount_field, amount)

    if count != 0:
        global_count_field = field if field.endswith('_orders') or field in [
            'new_clients', 'old_clients'] else f"{field}_orders"
        await db_operations.update_financial_data(global_count_field, count)

    # 2. 更新日结数据
    # 日结表只包含流量数据，不包含存量（如valid_orders/amount）
    # 允许的日结前缀
    daily_allowed_prefixes = ['new_clients', 'old_clients',
                              'interest', 'completed', 'breach', 'breach_end']

    # 检查field是否以允许的前缀开头
    is_daily_field = any(field.startswith(prefix)
                         for prefix in daily_allowed_prefixes)

    if is_daily_field:
        date = get_daily_period_date()
        # 全局日结
        if amount != 0:
            daily_amount_field = field if field.endswith(
                '_amount') or field == 'interest' else f"{field}_amount"
            await db_operations.update_daily_data(
                date, daily_amount_field, amount, None)
        if count != 0:
            daily_count_field = field if field.endswith('_orders') or field in [
                'new_clients', 'old_clients'] else f"{field}_orders"
            await db_operations.update_daily_data(
                date, daily_count_field, count, None)

        # 分组日结
        if group_id:
            if amount != 0:
                await db_operations.update_daily_data(
                    date, daily_amount_field, amount, group_id)
            if count != 0:
                await db_operations.update_daily_data(
                    date, daily_count_field, count, group_id)

    # 3. 更新分组累计数据
    if group_id:
        if amount != 0:
            # 分组表字段通常与全局表一致
            group_amount_field = global_amount_field
            await db_operations.update_grouped_data(
                group_id, group_amount_field, amount)
        if count != 0:
            group_count_field = global_count_field
            await db_operations.update_grouped_data(
                group_id, group_count_field, count)


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


def get_state_from_title(title: str) -> str:
    """从群名识别订单状态"""
    if '❌' in title:
        return 'breach'
    elif '❗️' in title:
        return 'overdue'
    else:
        return 'normal'


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
    threshold_date = date(2025, 11, 25)
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


async def create_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """创建新订单 (读取群名)"""
    chat = update.effective_chat
    if not is_group_chat(update):
        await update.message.reply_text("⚠️ This command can only be used in group chat.")
        return

    title = chat.title
    if not title:
        await update.message.reply_text("❌ Cannot get group title.")
        return

    await try_create_order_from_title(update, context, chat, title, manual_trigger=True)


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
        order_id = match_new.group(0)  # A + digits as ID? Or just digits?
        # User says: "A2401150105" is the name.
        # Usually Order ID is the unique identifier.
        # Previous logic: `order_id = match.group(0)` (the full match)
        # If the group name is A..., likely the ID in DB should be A... to match?
        # Or is the ID still just the numbers?
        # "10个纯数字前加A为新客户"
        # Let's use the full string as ID to be unique and preserve type info if needed,
        # OR just the digits if ID must be numeric.
        # But `order_id` in DB is string. Let's use the full match (A...) to avoid collision with same numbers but B type?
        # Actually, if it's the same order, it shouldn't exist twice.
        # I will use the full match (e.g. "A2401150105") as Order ID.
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
    # seq_part = raw_digits[6:8]
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


async def handle_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理新成员入群（机器人入群）"""
    # 检查是否是机器人自己被添加
    if not update.message.new_chat_members:
        return

    bot_id = context.bot.id
    is_bot_added = False
    for member in update.message.new_chat_members:
        if member.id == bot_id:
            is_bot_added = True
            break

    if not is_bot_added:
        return

    chat = update.effective_chat
    if not chat.title:
        return

    logger.info(f"Bot added to group: {chat.title} ({chat.id})")

    # 尝试创建订单
    await try_create_order_from_title(update, context, chat, chat.title, manual_trigger=False)


async def handle_new_chat_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群名变更"""
    chat = update.effective_chat
    new_title = update.message.new_chat_title

    if not new_title:
        return

    logger.info(f"Group title changed to: {new_title} ({chat.id})")

    existing_order = await db_operations.get_order_by_chat_id(chat.id)
    if existing_order:
        await update_order_state_from_title(update, context, existing_order, new_title)
    else:
        await try_create_order_from_title(update, context, chat, new_title, manual_trigger=False)


async def handle_amount_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理金额操作（需要管理员权限）"""
    # 检查是否在群组中 (利息操作可能可以在私聊? 不，为了关联ID，最好也在群里，或者私聊不支持)
    # 根据需求"私聊界面不可以有任何订单"，这里也限制
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
        # elif amount_text.endswith('c'):
        #     # 违约协商还款 - 需要订单
        #     if not order:
        #         message = "❌ Failed: No active order in this group."
        #         await update.message.reply_text(message)
        #         return
        #     amount = float(amount_text[:-1])
        #     await process_breach_payment(update, order, amount)
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


async def process_breach_payment(update: Update, order: dict, amount: float):
    """处理违约协商还款"""
    try:
        if order['state'] != 'breach':
            message = "❌ Failed: Order must be in breach state."
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

        # 1. 违约回款统计
        await update_all_stats('breach_end', amount, 1, group_id)

        # 2. 流动资金增加
        await update_liquid_capital(amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text(f"✅ Breach Payment: {amount:.2f}\nRemaining: {new_amount:.2f}")
        else:
            await update.message.reply_text(
                f"✅ Breach Payment Successful!\n"
                f"Order ID: {order['order_id']}\n"
                f"Payment Amount: {amount:.2f}\n"
                f"Remaining Amount: {new_amount:.2f}"
            )
    except Exception as e:
        logger.error(f"处理违约还款时出错: {e}", exc_info=True)
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
        # 这里的 reply_func 可能未定义如果出错发生在开头，但一般不会
        message = "❌ Error processing request."
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query:
            await update.callback_query.message.reply_text(message)


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


async def generate_report_text(period_type: str, start_date: str, end_date: str, group_id: str = None) -> str:
    """生成报表文本"""
    import pytz

    # 获取当前状态数据（资金和有效订单）
    if group_id:
        current_data = await db_operations.get_grouped_data(group_id)
        report_title = f"归属ID {group_id} 的报表"
    else:
        current_data = await db_operations.get_financial_data()
        report_title = "全局报表"

    # 获取周期统计数据
    stats = await db_operations.get_stats_by_date_range(
        start_date, end_date, group_id)

    # 格式化时间
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M")

    period_display = ""
    if period_type == "today":
        period_display = f"今日数据 ({start_date})"
    elif period_type == "month":
        period_display = f"本月数据 ({start_date[:-3]})"
    else:
        period_display = f"区间数据 ({start_date} 至 {end_date})"

    report = (
        f"=== {report_title} ===\n"
        f"📅 {now}\n"
        f"{'─' * 25}\n"
        f"💰 【当前状态】\n"
        f"有效订单数: {current_data['valid_orders']}\n"
        f"有效订单金额: {current_data['valid_amount']:.2f}\n"
        f"{'─' * 25}\n"
        f"📈 【{period_display}】\n"
        f"流动资金: {stats['liquid_flow']:.2f}\n"
        f"新客户数: {stats['new_clients']}\n"
        f"新客户金额: {stats['new_clients_amount']:.2f}\n"
        f"老客户数: {stats['old_clients']}\n"
        f"老客户金额: {stats['old_clients_amount']:.2f}\n"
        f"利息收入: {stats['interest']:.2f}\n"
        f"完成订单数: {stats['completed_orders']}\n"
        f"完成订单金额: {stats['completed_amount']:.2f}\n"
        f"违约订单数: {stats['breach_orders']}\n"
        f"违约订单金额: {stats['breach_amount']:.2f}\n"
        f"违约完成订单数: {stats['breach_end_orders']}\n"
        f"违约完成金额: {stats['breach_end_amount']:.2f}\n"
        f"{'─' * 25}\n"
        f"💸 【开销与余额】\n"
        f"公司开销: {stats['company_expenses']:.2f}\n"
        f"其他开销: {stats['other_expenses']:.2f}\n"
        f"现金余额: {current_data['liquid_funds']:.2f}\n"
    )
    return report


async def show_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示报表"""
    # 默认为今日报表
    period_type = "today"
    group_id = None

    # 处理参数
    if context.args:
        group_id = context.args[0]

    # 获取今日日期
    daily_date = get_daily_period_date()

    # 生成报表
    report_text = await generate_report_text(period_type, daily_date, daily_date, group_id)

    # 构建按钮
    keyboard = [
        [
            InlineKeyboardButton(
                "📅 Month Report", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}"),
            InlineKeyboardButton(
                "📆 Date Query", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
        ],
        [
            InlineKeyboardButton(
                "🏢 Company Expense", callback_data="report_record_company"),
            InlineKeyboardButton(
                "📝 Other Expense", callback_data="report_record_other")
        ],
        [
            InlineKeyboardButton(
                "🔍 Search & Lock", callback_data="search_lock_start"),
            InlineKeyboardButton(
                "📢 Broadcast", callback_data="broadcast_start")
        ]
    ]

    # 如果是全局报表，显示归属查询按钮
    if not group_id:
        keyboard.append([
            InlineKeyboardButton(
                "🔍 Search by Group", callback_data="report_menu_attribution"),
            InlineKeyboardButton(
                "🔍 Search & Lock", callback_data="search_lock_start"),
            InlineKeyboardButton(
                "📢 Broadcast", callback_data="broadcast_start")
        ])
    else:
        keyboard.append([InlineKeyboardButton(
            "🔙 Back", callback_data="report_view_today_ALL")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(report_text, reply_markup=reply_markup)


async def display_search_results_helper(update: Update, context: ContextTypes.DEFAULT_TYPE, orders: list):
    """辅助函数：显示搜索结果"""
    if not orders:
        if update.callback_query:
            await update.callback_query.message.reply_text("❌ No matching orders found")
        else:
            await update.message.reply_text("❌ No matching orders found")
        return

    # 锁定群组
    locked_groups = list(set(order['chat_id'] for order in orders))
    context.user_data['locked_groups'] = locked_groups

    # 确定发送消息的方法
    if update.callback_query:
        send_msg = update.callback_query.message.reply_text
    else:
        send_msg = update.message.reply_text

    await send_msg(f"ℹ️ Locked {len(locked_groups)} groups for broadcasting.")

    # 格式化输出
    if len(orders) == 1:
        order = orders[0]
        chat_id = order['chat_id']

        # 尝试获取群组信息
        chat_title = None
        chat_username = None
        try:
            chat = await context.bot.get_chat(chat_id)
            chat_title = chat.title or "Unnamed Group"
            if hasattr(chat, 'username') and chat.username:
                chat_username = chat.username
        except Exception as e:
            logger.debug(f"无法获取群组 {chat_id} 的信息: {e}")

        # 构建结果消息
        result = "📍 Order Found:\n\n"

        if chat_title:
            result += f"📋 Group Name: {chat_title}\n"

        result += (
            f"🆔 Group ID: `{chat_id}`\n"
            f"📝 Order ID: {order['order_id']}\n"
            f"💰 Amount: {order['amount']:.2f}\n"
            f"📊 State: {order['state']}\n"
        )

        # 添加跳转方式
        if chat_username:
            result += f"\n🔗 Link: @{chat_username}"
        else:
            result += f"\n💡 Search Group ID in Telegram: {chat_id}"
            result += f"\n   Or use: tg://openmessage?chat_id={chat_id}"
    else:
        result = f"📍 Found {len(orders)} orders:\n\n"
        for i, order in enumerate(orders[:20], 1):  # 最多显示20个
            chat_id = order['chat_id']
            chat_title = None
            try:
                chat = await context.bot.get_chat(chat_id)
                chat_title = chat.title or "Unnamed Group"
            except:
                pass

            if chat_title:
                result += f"{i}. 📋 {chat_title}\n"
            else:
                result += f"{i}. 🆔 Group ID: {chat_id}\n"

            result += (
                f"   📝 Order: {order['order_id']} | "
                f"💰 {order['amount']:.2f} | "
                f"📊 {order['state']}\n"
                f"   🔗 tg://openmessage?chat_id={chat_id}\n\n"
            )
        if len(orders) > 20:
            result += f"⚠️ And {len(orders) - 20} more..."

    await send_msg(result, parse_mode='Markdown')


async def handle_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理搜索相关的回调"""
    query = update.callback_query
    data = query.data

    if data == "search_menu_state":
        keyboard = [
            [InlineKeyboardButton(
                "Normal", callback_data="search_do_state_normal")],
            [InlineKeyboardButton(
                "Overdue", callback_data="search_do_state_overdue")],
            [InlineKeyboardButton(
                "Breach", callback_data="search_do_state_breach")],
            [InlineKeyboardButton(
                "End", callback_data="search_do_state_end")],
            [InlineKeyboardButton("Breach End",
                                  callback_data="search_do_state_breach_end")],
            [InlineKeyboardButton("🔙 Back", callback_data="search_start")]
        ]
        await query.edit_message_text("Select State:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_menu_attribution":
        group_ids = await db_operations.get_all_group_ids()
        if not group_ids:
            await query.edit_message_text("⚠️ No Attribution Data",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="search_start")]]))
            return

        keyboard = []
        row = []
        for gid in sorted(group_ids)[:40]:
            row.append(InlineKeyboardButton(
                gid, callback_data=f"search_do_attribution_{gid}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton(
            "🔙 Back", callback_data="search_start")])
        await query.edit_message_text("Select Group ID:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_menu_group":
        keyboard = [
            [InlineKeyboardButton("Mon", callback_data="search_do_group_一"), InlineKeyboardButton(
                "Tue", callback_data="search_do_group_二"), InlineKeyboardButton("Wed", callback_data="search_do_group_三")],
            [InlineKeyboardButton("Thu", callback_data="search_do_group_四"), InlineKeyboardButton(
                "Fri", callback_data="search_do_group_五"), InlineKeyboardButton("Sat", callback_data="search_do_group_六")],
            [InlineKeyboardButton("Sun", callback_data="search_do_group_日")],
            [InlineKeyboardButton("🔙 Back", callback_data="search_start")]
        ]
        await query.edit_message_text("Select Week Group:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_start":
        keyboard = [
            [
                InlineKeyboardButton(
                    "By State", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "By Group ID", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "By Week Group", callback_data="search_menu_group")
            ]
        ]
        await query.edit_message_text("🔍 Search By:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_lock_start":
        await query.message.reply_text(
            "🔍 Enter search criteria (mixed allowed):\n"
            "Format: key1=value1 key2=value2\n"
            "Example: `group_id=S01 state=normal`\n"
            "Please Enter:",
            parse_mode='Markdown'
        )
        context.user_data['state'] = 'SEARCHING'
        return

    # 执行查找
    if data.startswith("search_do_"):
        criteria = {}
        if data.startswith("search_do_state_"):
            criteria['state'] = data[16:]
        elif data.startswith("search_do_attribution_"):
            criteria['group_id'] = data[22:]
        elif data.startswith("search_do_group_"):
            criteria['weekday_group'] = data[16:]

        orders = await db_operations.search_orders_advanced(criteria)
        await display_search_results_helper(update, context, orders)
        return


async def handle_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理报表相关的回调"""
    query = update.callback_query
    data = query.data

    if data == "report_record_company":
        date = get_daily_period_date()
        records = await db_operations.get_expense_records(date, date, 'company')

        msg = f"🏢 Company Expense Today ({date}):\n\n"
        if not records:
            msg += "No records\n"
        else:
            total = 0
            for i, r in enumerate(records, 1):
                msg += f"{i}. {r['amount']:.2f} - {r['note'] or 'No Note'}\n"
                total += r['amount']
            msg += f"\nTotal: {total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "➕ Add Expense", callback_data="report_add_expense_company")],
            [
                InlineKeyboardButton(
                    "📅 Month", callback_data="report_expense_month_company"),
                InlineKeyboardButton(
                    "📆 Query", callback_data="report_expense_query_company")
            ],
            [InlineKeyboardButton(
                "🔙 Back", callback_data="report_view_today_ALL")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_month_company":
        import pytz
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        records = await db_operations.get_expense_records(
            start_date, end_date, 'company')

        msg = f"🏢 Company Expense This Month ({start_date} to {end_date}):\n\n"
        if not records:
            msg += "No records\n"
        else:
            total = 0
            # 限制显示数量，防止消息过长
            display_records = records[-20:] if len(records) > 20 else records

            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or 'No Note'}\n"
                total += r['amount']

            # 计算总额（所有记录）
            real_total = sum(r['amount'] for r in records)
            if len(records) > 20:
                msg += f"\n... (Total {len(records)} records, showing last 20)\n"
            msg += f"\nTotal: {real_total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "🔙 Back", callback_data="report_record_company")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_query_company":
        await query.message.reply_text(
            "🏢 Enter date range:\n"
            "Format 1 (Day): 2024-01-01\n"
            "Format 2 (Range): 2024-01-01 2024-01-31\n"
            "Enter 'cancel' to cancel"
        )
        context.user_data['state'] = 'QUERY_EXPENSE_COMPANY'
        return

    if data == "report_add_expense_company":
        await query.message.reply_text(
            "🏢 Enter amount and note:\n"
            "Format: Amount Note\n"
            "Example: 100 Server Cost"
        )
        context.user_data['state'] = 'WAITING_EXPENSE_COMPANY'
        return

    if data == "report_record_other":
        date = get_daily_period_date()
        records = await db_operations.get_expense_records(date, date, 'other')

        msg = f"📝 Other Expense Today ({date}):\n\n"
        if not records:
            msg += "No records\n"
        else:
            total = 0
            for i, r in enumerate(records, 1):
                msg += f"{i}. {r['amount']:.2f} - {r['note'] or 'No Note'}\n"
                total += r['amount']
            msg += f"\nTotal: {total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "➕ Add Expense", callback_data="report_add_expense_other")],
            [
                InlineKeyboardButton(
                    "📅 Month", callback_data="report_expense_month_other"),
                InlineKeyboardButton(
                    "📆 Query", callback_data="report_expense_query_other")
            ],
            [InlineKeyboardButton(
                "🔙 Back", callback_data="report_view_today_ALL")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_month_other":
        import pytz
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        records = await db_operations.get_expense_records(
            start_date, end_date, 'other')

        msg = f"📝 Other Expense This Month ({start_date} to {end_date}):\n\n"
        if not records:
            msg += "No records\n"
        else:
            display_records = records[-20:] if len(records) > 20 else records
            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or 'No Note'}\n"

            real_total = sum(r['amount'] for r in records)
            if len(records) > 20:
                msg += f"\n... (Total {len(records)} records, showing last 20)\n"
            msg += f"\nTotal: {real_total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "🔙 Back", callback_data="report_record_other")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_query_other":
        await query.message.reply_text(
            "📝 Enter date range:\n"
            "Format 1 (Day): 2024-01-01\n"
            "Format 2 (Range): 2024-01-01 2024-01-31\n"
            "Enter 'cancel' to cancel"
        )
        context.user_data['state'] = 'QUERY_EXPENSE_OTHER'
        return

    if data == "report_add_expense_other":
        await query.message.reply_text(
            "📝 Enter amount and note:\n"
            "Format: Amount Note\n"
            "Example: 50 Office Supplies"
        )
        context.user_data['state'] = 'WAITING_EXPENSE_OTHER'
        return

    if data == "report_menu_attribution":
        group_ids = await db_operations.get_all_group_ids()
        if not group_ids:
            await query.edit_message_text(
                "⚠️ No Attribution Data",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Back", callback_data="report_view_today_ALL")]])
            )
            return

        keyboard = []
        row = []
        for gid in sorted(group_ids):
            row.append(InlineKeyboardButton(
                gid, callback_data=f"report_view_today_{gid}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton(
            "🔙 Back", callback_data="report_view_today_ALL")])
        await query.edit_message_text("Please select Group ID:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # 提取视图类型和参数
    # 格式: report_view_{type}_{group_id}
    # 或者旧格式: report_{group_id}

    if data.startswith("report_") and not data.startswith("report_view_"):
        # 兼容旧格式，转为 today 视图
        group_id = data[7:]
        view_type = 'today'
    else:
        parts = data.split('_')
        # report, view, type, group_id...
        if len(parts) < 4:
            return
        view_type = parts[2]
        group_id = parts[3]

    group_id = None if group_id == 'ALL' else group_id

    if view_type == 'today':
        date = get_daily_period_date()
        report_text = await generate_report_text("today", date, date, group_id)

        keyboard = [
            [
                InlineKeyboardButton(
                    "📅 Month Report", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}"),
                InlineKeyboardButton(
                    "📆 Date Query", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
            ],
            [
                InlineKeyboardButton(
                    "🏢 Company Expense", callback_data="report_record_company"),
                InlineKeyboardButton(
                    "📝 Other Expense", callback_data="report_record_other")
            ]
        ]
        # 全局视图添加通用按钮
        if not group_id:
            keyboard.append([
                InlineKeyboardButton(
                    "🔍 Search by Group", callback_data="report_menu_attribution"),
                InlineKeyboardButton(
                    "🔍 Search & Lock", callback_data="search_lock_start"),
                InlineKeyboardButton(
                    "📢 Broadcast", callback_data="broadcast_start")
            ])
        else:
            keyboard.append([InlineKeyboardButton(
                "🔙 Back", callback_data="report_view_today_ALL")])

        await query.edit_message_text(report_text, reply_markup=InlineKeyboardMarkup(keyboard))

    elif view_type == 'month':
        import pytz
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        report_text = await generate_report_text("month", start_date, end_date, group_id)

        keyboard = [
            [
                InlineKeyboardButton(
                    "📄 Today Report", callback_data=f"report_view_today_{group_id if group_id else 'ALL'}"),
                InlineKeyboardButton(
                    "📆 Date Query", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
            ]
        ]
        await query.edit_message_text(report_text, reply_markup=InlineKeyboardMarkup(keyboard))

    elif view_type == 'query':
        await query.message.reply_text(
            "📆 请输入查询日期范围：\n"
            "格式1 (单日): 2024-01-01\n"
            "格式2 (范围): 2024-01-01 2024-01-31\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'REPORT_QUERY'
        context.user_data['report_group_id'] = group_id


@authorized_required
@error_handler
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
    elif data == "broadcast_start":
        locked_groups = context.user_data.get('locked_groups', [])
        if not locked_groups:
            await query.message.reply_text("⚠️ No locked groups. Use Search to lock groups first.")
            return

        await query.message.reply_text(
            f"📢 Ready to broadcast to {len(locked_groups)} groups.\n"
            "Please enter the message:\n"
            "(Enter 'cancel' to cancel)"
        )
        context.user_data['state'] = 'BROADCASTING'
    else:
        logger.warning(f"Unhandled callback data: {data}")
        await query.message.reply_text(f"⚠️ 未知的操作: {data}")


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
                "✅ Normal", callback_data="order_action_normal"),
            InlineKeyboardButton(
                "⚠️ Overdue", callback_data="order_action_overdue")
        ],
        [
            InlineKeyboardButton("🏁 End", callback_data="order_action_end"),
            InlineKeyboardButton(
                "🚫 Breach", callback_data="order_action_breach")
        ],
        [
            InlineKeyboardButton(
                "💸 Breach End", callback_data="order_action_breach_end")
        ]
    ]

    await reply_func(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


@error_handler
@admin_required
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


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理文本输入（用于搜索和群发）"""
    user_state = context.user_data.get('state')

    # 1. 检查是否是快捷操作（+开头），如果是，交给 handle_amount_operation 处理
    if update.message.text.startswith('+'):
        return

    # 2. 检查状态是否需要处理群组消息
    # 目前只有 WAITING_BREACH_END_AMOUNT 可能在群组中触发（如果用户在群组点击了按钮并收到提示）
    allow_group = False
    if user_state == 'WAITING_BREACH_END_AMOUNT':
        allow_group = True

    # 3. 检查聊天类型
    # 如果不是私聊，且不允许群组，则忽略
    if update.effective_chat.type != 'private' and not allow_group:
        return

    # 如果没有状态，忽略
    if not user_state:
        return

    text = update.message.text.strip()

    # 通用取消逻辑 (群组中也支持取消)
    if text.lower() == 'cancel':
        context.user_data['state'] = None
        msg = "✅ Operation Cancelled"
        await update.message.reply_text(msg)
        return

    if user_state == 'WAITING_BREACH_END_AMOUNT':
        try:
            amount = float(text)
            if amount <= 0:
                msg = "❌ Amount must be positive"
                await update.message.reply_text(msg)
                return

            chat_id = context.user_data.get('breach_end_chat_id')
            if not chat_id:
                msg = "❌ State Error. Please retry."
                await update.message.reply_text(msg)
                context.user_data['state'] = None
                return

            # 验证是否是在对应的群组中操作（如果是群组消息）
            if is_group_chat(update) and update.effective_chat.id != chat_id:
                pass

            order = await db_operations.get_order_by_chat_id(chat_id)
            if not order or order['state'] != 'breach':
                msg = "❌ Order state changed or not found"
                await update.message.reply_text(msg)
                context.user_data['state'] = None
                return

            # 执行完成逻辑
            await db_operations.update_order_state(chat_id, 'breach_end')
            group_id = order['group_id']

            # 违约完成订单增加，金额增加
            await update_all_stats('breach_end', amount, 1, group_id)

            # 更新流动资金 (Liquid Flow & Cash Balance)
            await update_liquid_capital(amount)

            msg_en = f"✅ Breach Order Ended\nAmount: {amount:.2f}"

            # 如果当前聊天不是订单所在的聊天（例如私聊操作群订单），通知群组
            if update.effective_chat.id != chat_id:
                await context.bot.send_message(chat_id=chat_id, text=msg_en)
                await update.message.reply_text(msg_en + f"\nOrder ID: {order['order_id']}")
            else:
                await update.message.reply_text(msg_en)

            context.user_data['state'] = None

        except ValueError:
            msg = "❌ Invalid amount. Please enter a number."
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"处理违约完成时出错: {e}", exc_info=True)
            msg = f"⚠️ Error: {e}"
            await update.message.reply_text(msg)
        return

    # 以下状态仅限私聊 (search, report, broadcast)
    if update.effective_chat.type != 'private':
        return

    if user_state in ['QUERY_EXPENSE_COMPANY', 'QUERY_EXPENSE_OTHER']:
        try:
            dates = text.split()
            if len(dates) == 1:
                start_date = end_date = dates[0]
            elif len(dates) == 2:
                start_date = dates[0]
                end_date = dates[1]
            else:
                await update.message.reply_text("❌ Format Error. Use 'YYYY-MM-DD' or 'YYYY-MM-DD YYYY-MM-DD'")
                return

            # 验证日期格式
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")

            expense_type = 'company' if user_state == 'QUERY_EXPENSE_COMPANY' else 'other'
            records = await db_operations.get_expense_records(
                start_date, end_date, expense_type)

            title = "Company Expense" if expense_type == 'company' else "Other Expense"
            msg = f"🔍 {title} Query ({start_date} to {end_date}):\n\n"

            if not records:
                msg += "No records found.\n"
            else:
                total = 0
                # 限制显示数量，防止消息过长
                display_records = records[-20:] if len(
                    records) > 20 else records

                for r in display_records:
                    msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or 'No Note'}\n"
                    total += r['amount']

                # 计算总额（所有记录）
                real_total = sum(r['amount'] for r in records)
                if len(records) > 20:
                    msg += f"\n... (Total {len(records)} records, showing last 20)\n"
                msg += f"\nTotal: {real_total:.2f}\n"

            back_callback = "report_record_company" if expense_type == 'company' else "report_record_other"
            keyboard = [[InlineKeyboardButton(
                "🔙 Back", callback_data=back_callback)]]
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ Invalid Date Format. Use YYYY-MM-DD")
        except Exception as e:
            logger.error(f"查询开销出错: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ Error: {e}")
        return

    if user_state in ['WAITING_EXPENSE_COMPANY', 'WAITING_EXPENSE_OTHER']:
        try:
            # 格式: 金额 备注
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2:
                amount_str = parts[0]
                note = "No Note"
            else:
                amount_str, note = parts

            amount = float(amount_str)
            if amount <= 0:
                await update.message.reply_text("❌ Amount must be positive")
                return

            expense_type = 'company' if user_state == 'WAITING_EXPENSE_COMPANY' else 'other'
            date_str = get_daily_period_date()

            # 记录开销
            await db_operations.record_expense(date_str, expense_type, amount, note)

            financial_data = await db_operations.get_financial_data()
            await update.message.reply_text(
                f"✅ Expense Recorded\n"
                f"Type: {'Company' if expense_type == 'company' else 'Other'}\n"
                f"Amount: {amount:.2f}\n"
                f"Note: {note}\n"
                f"Current Balance: {financial_data['liquid_funds']:.2f}"
            )
            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ Invalid Format. Example: 100 Server Cost")
        except Exception as e:
            logger.error(f"记录开销时出错: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ Error: {e}")
        return

    if user_state == 'SEARCHING':
        # 解析搜索条件
        criteria = {}
        try:
            # 支持 key=value 格式
            if '=' in text:
                parts = text.split()
                for part in parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        key = key.strip().lower()
                        value = value.strip()

                        # 映射别名
                        if key == 'group':
                            key = 'weekday_group'
                            # 处理周一到周日的映射
                            if value.startswith('周') and len(value) == 2:
                                value = value[1]

                        if key in ['group_id', 'state', 'customer', 'order_id', 'weekday_group']:
                            criteria[key] = value
            else:
                # 智能识别
                val = text.strip()
                # 1. 星期分组 (一, 二... 或 周一, 周二...)
                if val in ['一', '二', '三', '四', '五', '六', '日']:
                    criteria['weekday_group'] = val
                elif val.startswith('周') and len(val) == 2 and val[1] in ['一', '二', '三', '四', '五', '六', '日']:
                    criteria['weekday_group'] = val[1]
                # 2. 客户类型
                elif val.upper() in ['A', 'B']:
                    criteria['customer'] = val.upper()
                # 3. 状态
                elif val in ['normal', 'overdue', 'breach', 'end', 'breach_end', '正常', '逾期', '违约', '完成', '违约完成']:
                    state_map = {
                        '正常': 'normal', '逾期': 'overdue', '违约': 'breach',
                        '完成': 'end', '违约完成': 'breach_end'
                    }
                    criteria['state'] = state_map.get(val, val)
                # 4. 归属ID (S01)
                elif len(val) == 3 and val[0].isalpha() and val[1:].isdigit():
                    criteria['group_id'] = val.upper()
                # 5. 默认按订单ID
                else:
                    criteria['order_id'] = val

            if not criteria:
                await update.message.reply_text("❌ Cannot recognize search criteria", parse_mode='Markdown')
                return

            orders = await db_operations.search_orders_advanced(criteria)

            if not orders:
                await update.message.reply_text("❌ No matching orders found")
                context.user_data['state'] = None
                return

            # 锁定群组
            locked_groups = list(set(order['chat_id'] for order in orders))
            context.user_data['locked_groups'] = locked_groups

            await update.message.reply_text(
                f"✅ Found {len(orders)} orders in {len(locked_groups)} groups.\n"
                f"Groups locked. You can now use 【Broadcast】 feature.\n"
                f"Enter 'cancel' to exit search mode (locks retained)."
            )
            # 退出输入状态，但保留 locked_groups
            context.user_data['state'] = None

        except Exception as e:
            logger.error(f"搜索出错: {e}")
            await update.message.reply_text(f"⚠️ Search Error: {e}")
            context.user_data['state'] = None

    elif user_state == 'REPORT_QUERY':
        group_id = context.user_data.get('report_group_id')

        # 解析日期
        try:
            dates = text.split()
            if len(dates) == 1:
                start_date = end_date = dates[0]
            elif len(dates) == 2:
                start_date = dates[0]
                end_date = dates[1]
            else:
                await update.message.reply_text("❌ Format Error. Use 'YYYY-MM-DD' or 'YYYY-MM-DD YYYY-MM-DD'")
                return

            # 验证日期格式
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")

            # 生成报表
            report_text = await generate_report_text("query", start_date, end_date, group_id)

            # 键盘
            keyboard = [
                [
                    InlineKeyboardButton(
                        "📄 Today Report", callback_data=f"report_view_today_{group_id if group_id else 'ALL'}"),
                    InlineKeyboardButton(
                        "📅 Month Report", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}")
                ]
            ]

            await update.message.reply_text(report_text, reply_markup=InlineKeyboardMarkup(keyboard))
            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ Invalid Date Format. Use YYYY-MM-DD")
        except Exception as e:
            logger.error(f"查询报表出错: {e}")
            await update.message.reply_text(f"⚠️ Query Error: {e}")
            context.user_data['state'] = None

    elif user_state == 'BROADCASTING':
        locked_groups = context.user_data.get('locked_groups', [])
        if not locked_groups:
            await update.message.reply_text("⚠️ No locked groups")
            context.user_data['state'] = None
            return

        success_count = 0
        fail_count = 0

        await update.message.reply_text(f"⏳ Sending message to {len(locked_groups)} groups...")

        for chat_id in locked_groups:
            try:
                await context.bot.send_message(chat_id=chat_id, text=text)
                success_count += 1
            except Exception as e:
                logger.error(f"群发失败 {chat_id}: {e}")
                fail_count += 1

        await update.message.reply_text(
            f"✅ Broadcast Completed\n"
            f"Success: {success_count}\n"
            f"Failed: {fail_count}"
        )
        context.user_data['state'] = None


async def search_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查找订单（支持交互式菜单和旧命令方式）"""
    # 如果没有参数，显示交互式菜单
    if not context.args:
        keyboard = [
            [
                InlineKeyboardButton(
                    "By State", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "By Group ID", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "By Week Group", callback_data="search_menu_group")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🔍 Search By:", reply_markup=reply_markup)
        return

    # 如果参数不足2个，提示用法（兼容旧习惯，或者直接忽略参数显示菜单？）
    if len(context.args) < 2:
        keyboard = [
            [
                InlineKeyboardButton(
                    "By State", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "By Group ID", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "By Week Group", callback_data="search_menu_group")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🔍 Search By:", reply_markup=reply_markup)
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


def main() -> None:
    """启动机器人"""
    # 验证配置
    if not token:
        logger.error("BOT_TOKEN 未设置，无法启动机器人")
        print("\n❌ 错误: BOT_TOKEN 未设置")
        print("请检查 config.py 文件或环境变量")
        return

    if not ADMIN_IDS:
        logger.error("ADMIN_USER_IDS 未设置，无法启动机器人")
        print("\n❌ 错误: ADMIN_USER_IDS 未设置")
        print("请检查 config.py 文件或环境变量")
        return

    logger.info(f"机器人启动中... 管理员数量: {len(ADMIN_IDS)}")
    print(f"\n🤖 机器人启动中...")
    print(f"📋 管理员数量: {len(ADMIN_IDS)}")

    # 初始化数据库（如果不存在）
    print("📦 检查数据库...")
    try:
        init_db.init_database()
        print("✅ 数据库已就绪")
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        print(f"❌ 数据库初始化失败: {e}")
        return

    try:
        # 创建Application并传入bot的token
        application = Application.builder().token(token).build()
    except Exception as e:
        logger.error(f"创建应用时出错: {e}")
        print(f"\n❌ 创建应用时出错: {e}")
        return

    # 添加命令处理器（按新需求修改）
    application.add_handler(CommandHandler(
        "start", private_chat_only(authorized_required(start))))
    application.add_handler(CommandHandler(
        "report", private_chat_only(authorized_required(show_report))))
    application.add_handler(CommandHandler(
        "search", private_chat_only(authorized_required(search_orders))))

    # 订单操作命令（员工可用）
    application.add_handler(CommandHandler(
        "create", authorized_required(group_chat_only(create_order))))
    application.add_handler(CommandHandler(
        "normal", authorized_required(group_chat_only(set_normal))))
    application.add_handler(CommandHandler(
        "overdue", authorized_required(group_chat_only(set_overdue))))
    application.add_handler(CommandHandler(
        "end", authorized_required(group_chat_only(set_end))))
    application.add_handler(CommandHandler(
        "breach", authorized_required(group_chat_only(set_breach))))
    application.add_handler(CommandHandler(
        "breach_end", authorized_required(group_chat_only(set_breach_end))))
    application.add_handler(CommandHandler(
        "order", authorized_required(group_chat_only(show_current_order))))

    # 资金和归属ID管理（仅管理员）
    application.add_handler(CommandHandler(
        "adjust", private_chat_only(admin_required(adjust_funds))))
    application.add_handler(CommandHandler(
        "create_attribution", private_chat_only(admin_required(create_attribution))))
    application.add_handler(CommandHandler(
        "list_attributions", private_chat_only(admin_required(list_attributions))))

    # 员工管理（仅管理员）
    application.add_handler(CommandHandler(
        "add_employee", private_chat_only(admin_required(add_employee))))
    application.add_handler(CommandHandler(
        "remove_employee", private_chat_only(admin_required(remove_employee))))
    application.add_handler(CommandHandler(
        "list_employees", private_chat_only(admin_required(list_employees))))

    # 自动订单创建（新成员入群监听 & 群名变更监听）
    application.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_chat_members))
    application.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_TITLE, handle_new_chat_title))

    # 添加消息处理器（金额操作）- 需要管理员或员工权限
    # 只处理以 + 开头的消息（快捷操作）
    # 修改：为了兼容私聊不处理金额操作，handle_amount_operation 已经添加了检查
    # 这里保持不变，因为我们希望通过 filters 就过滤掉大部分非目标消息
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(
            r'^\+') & filters.ChatType.GROUPS,
        handle_amount_operation),
        group=1)  # 设置优先级组

    # 添加通用文本处理器（用于处理搜索和群发输入）
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\+'),
        handle_text_input),
        group=2)

    # 添加回调查询处理器
    application.add_handler(CallbackQueryHandler(
        authorized_required(handle_order_action_callback), pattern="^order_action_"))
    application.add_handler(CallbackQueryHandler(
        authorized_required(button_callback)))

    # 启动机器人
    try:
        # 设置命令菜单
        commands = [
            ("create", "Create new order"),
            ("order", "Manage current order"),
            ("report", "View reports"),
            ("start", "Start/Help")
        ]

        async def post_init(application: Application):
            await application.bot.set_my_commands(commands)
            print("✅ 命令菜单已更新")

        print("✅ 机器人已启动，等待消息...")
        application.post_init = post_init
        application.run_polling()
    except telegram_error.InvalidToken:
        print("\n" + "="*60)
        print("❌ Token 无效或被拒绝！")
        print("="*60)
        print("\n可能的原因：")
        print("  1. Token 已过期或被撤销")
        print("  2. Token 格式不正确")
        print("  3. Token 不属于你的机器人")
        print("\n解决方法：")
        print("  1. 在 Telegram 中搜索 @BotFather")
        print("  2. 发送 /mybots 查看你的机器人列表")
        print("  3. 选择你的机器人，点击 'API Token'")
        print("  4. 复制新的 Token")
        print("  5. 更新 config.py 文件中的 BOT_TOKEN")
        print("\n当前使用的 Token（已隐藏部分）:")
        if token:
            masked_token = token[:10] + "..." + \
                token[-10:] if len(token) > 20 else "***"
            print(f"  {masked_token}")
        print("="*60)
        logger.error("Token 验证失败")
    except KeyboardInterrupt:
        print("\n\n👋 机器人已停止")
        logger.info("机器人被用户停止")
    except Exception as e:
        print(f"\n❌ 运行时发生错误: {e}")
        logger.error(f"运行时错误: {e}", exc_info=True)


if __name__ == "__main__":
    main()
