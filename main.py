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
            error_msg = f"⚠️ 操作失败: {str(e)}"

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
            error_msg = "⚠️ 此操作需要管理员权限"
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
        if db_operations.is_user_authorized(user_id):
            return await func(update, context, *args, **kwargs)

        error_msg = "⚠️ 您没有权限执行此操作"
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
            await update.message.reply_text("⚠️ 此命令只能在私聊中使用")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


def group_chat_only(func):
    """检查是否在群组中使用命令的装饰器"""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not is_group_chat(update):
            await update.message.reply_text("⚠️ 此命令只能在群组中使用")
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


def update_liquid_capital(amount: float):
    """更新流动资金（全局余额 + 日结流量）"""
    # 1. 全局余额 (Cash Balance)
    db_operations.update_financial_data('liquid_funds', amount)

    # 2. 日结流量 (Liquid Flow)
    date = get_daily_period_date()
    db_operations.update_daily_data(date, 'liquid_flow', amount, None)


def update_all_stats(field: str, amount: float, count: int = 0, group_id: str = None):
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
        db_operations.update_financial_data(global_amount_field, amount)

    if count != 0:
        global_count_field = field if field.endswith('_orders') or field in [
            'new_clients', 'old_clients'] else f"{field}_orders"
        db_operations.update_financial_data(global_count_field, count)

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
            db_operations.update_daily_data(
                date, daily_amount_field, amount, None)
        if count != 0:
            daily_count_field = field if field.endswith('_orders') or field in [
                'new_clients', 'old_clients'] else f"{field}_orders"
            db_operations.update_daily_data(
                date, daily_count_field, count, None)

        # 分组日结
        if group_id:
            if amount != 0:
                db_operations.update_daily_data(
                    date, daily_amount_field, amount, group_id)
            if count != 0:
                db_operations.update_daily_data(
                    date, daily_count_field, count, group_id)

    # 3. 更新分组累计数据
    if group_id:
        if amount != 0:
            # 分组表字段通常与全局表一致
            group_amount_field = global_amount_field
            db_operations.update_grouped_data(
                group_id, group_amount_field, amount)
        if count != 0:
            group_count_field = global_count_field
            db_operations.update_grouped_data(
                group_id, group_count_field, count)


async def add_employee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加员工（授权用户）"""
    if not context.args:
        await update.message.reply_text("❌ 用法: /add_employee <用户ID>")
        return

    try:
        user_id = int(context.args[0])
        if db_operations.add_authorized_user(user_id):
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
        if db_operations.remove_authorized_user(user_id):
            await update.message.reply_text(f"✅ 已移除员工: {user_id}")
        else:
            await update.message.reply_text("⚠️ 移除失败或用户不存在")
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")


async def list_employees(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有员工"""
    users = db_operations.get_authorized_users()
    if not users:
        await update.message.reply_text("📋 暂无授权员工")
        return

    message = "📋 授权员工列表:\n\n"
    for uid in users:
        message += f"👤 `{uid}`\n"

    await update.message.reply_text(message, parse_mode='Markdown')


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """发送欢迎消息"""
    financial_data = db_operations.get_financial_data()

    await update.message.reply_text(
        "📋 订单管理系统\n\n"
        "💰 当前流动资金: {:.2f}\n\n"
        "📝 订单操作：\n"
        "/create <归属ID> <客户A/B> <金额> - 创建新订单\n"
        "/order - 查看当前订单状态\n\n"
        "⚡ 快捷操作（在订单群中）：\n"
        "+<金额>b - 本金减少\n"
        "+<金额>c - 违约协商还款\n"
        "+<金额> - 利息收入\n\n"
        "🔄 状态变更：\n"
        "/normal - 转为正常状态\n"
        "/overdue - 转为逾期状态\n"
        "/end - 标记订单为完成\n"
        "/breach - 标记为违约\n"
        "/breach_end - 违约订单完成\n\n"
        "📊 查询功能：\n"
        "/report [归属ID] - 查看报表\n"
        "/search <类型> <值> - 查找订单\n"
        "  类型: order_id/group_id/customer/state/date\n\n"
        "⚙️ 管理功能：\n"
        "/adjust <金额> [备注] - 调整流动资金\n"
        "/create_attribution <ID> - 创建归属ID\n"
        "/list_attributions - 列出所有归属ID\n"
        "/add_employee <ID> - 添加员工\n"
        "/remove_employee <ID> - 移除员工\n"
        "/list_employees - 查看员工列表\n\n"
        "⚠️ 部分操作需要管理员权限".format(financial_data['liquid_funds'])
    )


async def create_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """创建新订单"""
    chat_id = update.message.chat_id

    # 检查群组是否已有订单
    existing_order = db_operations.get_order_by_chat_id(chat_id)
    if existing_order:
        message = "⚠️ This group already has an order. Please complete or breach the current order first." if is_group_chat(
            update) else "⚠️ 本群已有一个订单，请先完成或违约当前订单后再创建新订单。"
        await update.message.reply_text(message)
        return

    # 验证参数
    if len(context.args) != 3:
        await update.message.reply_text("❌ 用法: /create <归属ID> <客户A/B> <金额>\n示例: /create S01 A 5000")
        return

    group_id, customer, amount = context.args

    # 验证归属ID格式
    if len(group_id) != 3 or not group_id[0].isalpha() or not group_id[1:].isdigit():
        await update.message.reply_text("❌ 归属ID格式错误，应为1个字母加2个数字（如S01）")
        return

    # 验证客户类型
    customer = customer.upper()
    if customer not in ('A', 'B'):
        await update.message.reply_text("❌ 客户类型错误，应为A(新客户)或B(老客户)")
        return

    # 验证金额
    try:
        amount = float(amount)
        if amount <= 0:
            raise ValueError("金额必须大于0")
    except ValueError as e:
        await update.message.reply_text(f"❌ 金额错误: {str(e)}")
        return

    # 检查流动资金是否充足
    financial_data = db_operations.get_financial_data()
    if financial_data['liquid_funds'] < amount:
        await update.message.reply_text(
            f"❌ 流动资金不足\n"
            f"当前余额: {financial_data['liquid_funds']:.2f}\n"
            f"所需金额: {amount:.2f}\n"
            f"缺少: {amount - financial_data['liquid_funds']:.2f}"
        )
        return

    # 从群名提取订单ID (10位数字)
    chat_title = update.effective_chat.title
    if not chat_title:
        # 如果是私聊，且没有群名，则无法创建
        if update.effective_chat.type == 'private':
            await update.message.reply_text("❌ 请在群组中使用此命令，因为需要从群名中获取订单ID。")
            return
        else:
            await update.message.reply_text("❌ 无法获取群组名称。")
            return

    match = re.search(r'(\d{10})', chat_title)
    if not match:
        await update.message.reply_text(f"❌ 群名中未找到10位数字订单ID。\n当前群名: {chat_title}")
        return

    order_id = match.group(1)

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    group = get_current_group()

    new_order = {
        'order_id': order_id,
        'group_id': group_id,
        'chat_id': chat_id,
        'date': current_date,
        'group': group,
        'customer': customer,
        'amount': amount,
        'state': 'normal'
    }

    # 保存订单到数据库
    if not db_operations.create_order(new_order):
        await update.message.reply_text("订单创建失败，订单ID可能已存在")
        return

    # 1. 有效订单统计（全局+日结+分组）
    update_all_stats('valid', amount, 1, group_id)

    # 2. 流动资金减少（全局+分组）
    update_liquid_capital(-amount)
    # update_grouped_data(group_id, 'liquid_funds', -amount) # 分组表也有liquid_funds

    # 3. 客户统计（全局+日结+分组）
    client_field = 'new_clients' if customer == 'A' else 'old_clients'
    update_all_stats(client_field, amount, 1, group_id)

    # 新订单创建需要完整播报（群组用英语，私聊用中文）
    if is_group_chat(update):
        message = (
            f"✅ Order Created Successfully\n\n"
            f"📋 Order ID: {order_id}\n"
            f"🏷️  Group ID: {group_id}\n"
            f"📅 Date: {current_date}\n"
            f"📊 Week Group: {group}\n"
            f"👤 Customer: {'New' if customer == 'A' else 'Returning'}\n"
            f"💰 Amount: {amount:.2f}\n"
            f"📈 Status: normal"
        )
    else:
        message = (
            f"订单创建成功！\n"
            f"订单ID: {order_id}\n"
            f"归属ID: {group_id}\n"
            f"日期: {current_date}\n"
            f"分组: {group}\n"
            f"客户: {customer}\n"
            f"金额: {amount:.2f}\n"
            f"状态: normal"
        )
    await update.message.reply_text(message)


def parse_order_from_title(title: str):
    """从群名解析订单信息"""
    # 格式: 2403110105xxxx
    # 240311 -> 2024-03-11
    # 01 -> 序号
    # 05 -> 金额 (k)
    # 只要开头是10位数字即可
    match = re.search(r'^(\d{6})(\d{2})(\d{2})', title)
    if not match:
        return None

    date_part = match.group(1)  # YYMMDD
    # seq_part = match.group(2)  # NN (unused)
    amount_part = match.group(3)  # NN (k)

    try:
        # 假设 20YY
        full_date_str = f"20{date_part}"
        # 验证日期有效性
        order_date_obj = datetime.strptime(full_date_str, "%Y%m%d").date()
    except ValueError:
        return None

    amount = int(amount_part) * 1000

    # 提取整个匹配到的10位数字作为订单ID
    order_id = match.group(0)

    return {
        'date': order_date_obj,
        'amount': amount,
        'order_id': order_id,
        'full_date_str': full_date_str  # YYYYMMDD
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
    chat_id = chat.id
    chat_title = chat.title

    if not chat_title:
        return

    logger.info(f"Bot added to group: {chat_title} ({chat_id})")

    # 1. 解析群名
    parsed_info = parse_order_from_title(chat_title)
    if not parsed_info:
        logger.info(
            f"Group title {chat_title} does not match auto-order pattern.")
        await update.message.reply_text(
            "👋 Hello! I'm LoanBot.\n"
            "⚠️ Auto-creation failed: Group name must start with 10 digits (YYMMDDNNNN).\n"
            "Please use /create manually if needed."
        )
        return

    # 2. 判断是否已存在订单
    existing_order = db_operations.get_order_by_chat_id(chat_id)
    if existing_order:
        await update.message.reply_text(
            "👋 Hello! Group recognized, but an order already exists here."
        )
        return

    # 3. 判断新老客户 & 历史订单
    # 规则: 2025年11月25之前的默认为老客户(B)，且资金不做变化
    # 2025年11月25及之后的，由人工创建，机器人不自动创建
    threshold_date = date(2025, 11, 25)
    order_date = parsed_info['date']

    if order_date >= threshold_date:
        await update.message.reply_text(
            "👋 Hello! I'm LoanBot.\n"
            "ℹ️ New order detected (Date >= 2025-11-25).\n"
            "Please create the order manually using /create command."
        )
        return

    # 既然是历史订单，肯定是老客户
    customer = 'B'  # 老客户
    skip_financials = True

    amount = parsed_info['amount']
    order_id = parsed_info['order_id']
    group_id = 'S01'  # 默认归属
    # 入群当天的分组，还是订单日期的分组？通常是入群管理时的分组。保持 get_current_group()
    weekday_group = get_current_group()

    # 构造完整日期字符串 (YYYY-MM-DD HH:MM:SS)
    # 简单起见，使用 order_date + " 12:00:00"
    created_at = f"{order_date.strftime('%Y-%m-%d')} 12:00:00"

    new_order = {
        'order_id': order_id,
        'group_id': group_id,
        'chat_id': chat_id,
        'date': created_at,
        'group': weekday_group,
        'customer': customer,
        'amount': amount,
        'state': 'normal'
    }

    # 4. 创建订单
    if not db_operations.create_order(new_order):
        await update.message.reply_text("❌ Auto-create failed: Order ID duplicate or DB error.")
        return

    # 5. 更新统计 (根据是否跳过)
    if not skip_financials:
        # 检查余额是否充足 (仅当非历史订单时检查?)
        # 自动创建如果余额不足怎么办？
        # 既然已经创建了订单，就必须扣款，否则数据不一致。
        # 如果余额不足，这里会变成负数。

        # 1. 有效订单统计
        update_all_stats('valid', amount, 1, group_id)
        # 2. 流动资金减少
        update_liquid_capital(-amount)
        # 3. 客户统计
        client_field = 'new_clients' if customer == 'A' else 'old_clients'
        update_all_stats(client_field, amount, 1, group_id)
    else:
        # 历史订单：
        # 流动资金和现金余额不变 (不调用 update_liquid_capital)
        # 有效订单数量和金额要增加
        update_all_stats('valid', amount, 1, group_id)

    # 6. 发送通知
    msg = (
        f"✅ Historical Order Imported\n\n"
        f"📋 Order ID: {order_id}\n"
        f"🏷️  Group ID: {group_id} (Default)\n"
        f"📅 Date: {created_at}\n"
        f"👤 Customer: Returning (Historical)\n"
        f"💰 Amount: {amount:.2f}\n"
        f"⚠️ Funds Update: Skipped (Historical Data Only)"
    )
    await update.message.reply_text(msg)


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
    is_authorized = db_operations.is_user_authorized(user_id)

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
    order = db_operations.get_order_by_chat_id(chat_id)

    # 解析金额和操作类型
    try:
        # 去掉加号后的文本
        amount_text = text[1:].strip()

        if not amount_text:
            message = "❌ Failed" if is_group_chat(
                update) else "❌ 请输入金额，例如：+1000 或 +1000b"
            await update.message.reply_text(message)
            return

        if amount_text.endswith('b'):
            # 本金减少 - 需要订单
            if not order:
                message = "❌ Failed: No active order in this group." if is_group_chat(
                    update) else "❌ 本群没有订单，无法进行本金减少操作"
                await update.message.reply_text(message)
                return
            amount = float(amount_text[:-1])
            await process_principal_reduction(update, order, amount)
        # elif amount_text.endswith('c'):
        #     # 违约协商还款 - 需要订单
        #     if not order:
        #         message = "❌ Failed: No active order in this group." if is_group_chat(
        #             update) else "❌ 本群没有订单，无法进行违约协商还款操作"
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
                    update_all_stats('interest', amount, 0, None)
                    update_liquid_capital(amount)
                    # 群组只回复成功，私聊显示详情
                    if is_group_chat(update):
                        await update.message.reply_text("✅ Success")
                    else:
                        financial_data = db_operations.get_financial_data()
                        await update.message.reply_text(
                            f"✅ 利息收入记录成功！\n"
                            f"本次金额: {amount:.2f}\n"
                            f"当前总利息: {financial_data['interest']:.2f}"
                        )
            except ValueError:
                message = "❌ Failed: Invalid amount format." if is_group_chat(
                    update) else "❌ 金额格式错误，请输入有效的数字"
                await update.message.reply_text(message)
    except ValueError:
        message = "❌ Failed: Invalid format. Example: +1000, +1000b, +1000c" if is_group_chat(
            update) else "❌ 金额格式错误，请输入有效的数字\n示例：+1000 或 +1000b 或 +1000c"
        await update.message.reply_text(message)
    except Exception as e:
        logger.error(f"处理金额操作时出错: {e}", exc_info=True)
        message = "❌ Failed: An error occurred." if is_group_chat(
            update) else f"⚠️ 处理时发生错误: {str(e)}"
        await update.message.reply_text(message)


async def process_principal_reduction(update: Update, order: dict, amount: float):
    """处理本金减少"""
    try:
        if order['state'] not in ('normal', 'overdue'):
            message = "❌ Failed: Order state not allowed." if is_group_chat(
                update) else "❌ 当前订单状态不支持本金减少操作"
            await update.message.reply_text(message)
            return

        if amount <= 0:
            message = "❌ Failed: Amount must be positive." if is_group_chat(
                update) else "❌ 金额必须大于0"
            await update.message.reply_text(message)
            return

        if amount > order['amount']:
            message = (f"❌ Failed: Exceeds order amount ({order['amount']:.2f})" if is_group_chat(update)
                       else f"❌ 金额超过订单金额\n订单金额: {order['amount']:.2f}\n输入金额: {amount:.2f}")
            await update.message.reply_text(message)
            return

        # 更新订单金额
        new_amount = order['amount'] - amount
        if not db_operations.update_order_amount(order['chat_id'], new_amount):
            message = "❌ Failed: DB Error" if is_group_chat(
                update) else "⚠️ 更新订单金额失败"
            await update.message.reply_text(message)
            return

        group_id = order['group_id']

        # 1. 有效金额减少
        update_all_stats('valid', -amount, 0, group_id)

        # 2. 完成金额增加
        update_all_stats('completed', amount, 0, group_id)

        # 3. 流动资金增加
        update_liquid_capital(amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text(f"✅ Principal Reduced: {amount:.2f}\nRemaining: {new_amount:.2f}")
        else:
            await update.message.reply_text(
                f"✅ 本金减少成功！\n"
                f"订单ID: {order['order_id']}\n"
                f"减少金额: {amount:.2f}\n"
                f"剩余金额: {new_amount:.2f}"
            )
    except Exception as e:
        logger.error(f"处理本金减少时出错: {e}", exc_info=True)
        message = "❌ Error" if is_group_chat(update) else "⚠️ 处理时发生错误，请稍后重试"
        await update.message.reply_text(message)


async def process_breach_payment(update: Update, order: dict, amount: float):
    """处理违约协商还款"""
    try:
        if order['state'] != 'breach':
            message = "❌ Failed: Order must be in breach state." if is_group_chat(
                update) else "❌ 只有违约状态的订单才能进行协商还款"
            await update.message.reply_text(message)
            return

        if amount <= 0:
            message = "❌ Failed: Amount must be positive." if is_group_chat(
                update) else "❌ 金额必须大于0"
            await update.message.reply_text(message)
            return

        if amount > order['amount']:
            message = (f"❌ Failed: Exceeds order amount ({order['amount']:.2f})" if is_group_chat(update)
                       else f"❌ 金额超过订单金额\n订单金额: {order['amount']:.2f}\n输入金额: {amount:.2f}")
            await update.message.reply_text(message)
            return

        # 更新订单金额
        new_amount = order['amount'] - amount
        if not db_operations.update_order_amount(order['chat_id'], new_amount):
            message = "❌ Failed: DB Error" if is_group_chat(
                update) else "⚠️ 更新订单金额失败"
            await update.message.reply_text(message)
            return

        group_id = order['group_id']

        # 1. 违约回款统计
        update_all_stats('breach_end', amount, 1, group_id)

        # 2. 流动资金增加
        update_liquid_capital(amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text(f"✅ Breach Payment: {amount:.2f}\nRemaining: {new_amount:.2f}")
        else:
            await update.message.reply_text(
                f"✅ 违约协商还款成功！\n"
                f"订单ID: {order['order_id']}\n"
                f"还款金额: {amount:.2f}\n"
                f"剩余金额: {new_amount:.2f}"
            )
    except Exception as e:
        logger.error(f"处理违约还款时出错: {e}", exc_info=True)
        message = "❌ Error" if is_group_chat(update) else "⚠️ 处理时发生错误，请稍后重试"
        await update.message.reply_text(message)


async def process_interest(update: Update, order: dict, amount: float):
    """处理利息收入"""
    try:
        if amount <= 0:
            message = "❌ Failed: Amount must be positive." if is_group_chat(
                update) else "❌ 金额必须大于0"
            await update.message.reply_text(message)
            return

        group_id = order['group_id']

        # 1. 利息收入
        update_all_stats('interest', amount, 0, group_id)

        # 2. 流动资金增加
        update_liquid_capital(amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text("✅ Interest Received")
        else:
            financial_data = db_operations.get_financial_data()
            await update.message.reply_text(
                f"✅ 利息收入记录成功！\n"
                f"本次金额: {amount:.2f}\n"
                f"当前总利息: {financial_data['interest']:.2f}"
            )
    except Exception as e:
        logger.error(f"处理利息收入时出错: {e}", exc_info=True)
        message = "❌ Error" if is_group_chat(update) else "⚠️ 处理时发生错误，请稍后重试"
        await update.message.reply_text(message)


async def set_normal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """转为正常状态"""
    try:
        chat_id = update.message.chat_id

        order = db_operations.get_order_by_chat_id(chat_id)
        if not order:
            message = "❌ Failed: No active order." if is_group_chat(
                update) else "❌ 本群没有订单"
            await update.message.reply_text(message)
            return

        if order['state'] != 'overdue':
            message = "❌ Failed: Order must be overdue." if is_group_chat(
                update) else "❌ 只有逾期状态的订单才能转为正常状态"
            await update.message.reply_text(message)
            return

        if not db_operations.update_order_state(chat_id, 'normal'):
            message = "❌ Failed: DB Error" if is_group_chat(
                update) else "⚠️ 更新状态失败"
            await update.message.reply_text(message)
            return

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text(f"✅ Status Updated: normal\nOrder ID: {order['order_id']}")
        else:
            await update.message.reply_text(
                f"✅ 订单状态已更新为正常\n"
                f"订单ID: {order['order_id']}\n"
                f"当前状态: normal"
            )
    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}", exc_info=True)
        message = "❌ Error" if is_group_chat(update) else "⚠️ 处理时发生错误，请稍后重试"
        await update.message.reply_text(message)


async def set_overdue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """转为逾期状态"""
    try:
        chat_id = update.message.chat_id

        order = db_operations.get_order_by_chat_id(chat_id)
        if not order:
            message = "❌ Failed: No active order." if is_group_chat(
                update) else "❌ 本群没有订单"
            await update.message.reply_text(message)
            return

        if order['state'] != 'normal':
            message = "❌ Failed: Order must be normal." if is_group_chat(
                update) else "❌ 只有正常状态的订单才能转为逾期"
            await update.message.reply_text(message)
            return

        if not db_operations.update_order_state(chat_id, 'overdue'):
            message = "❌ Failed: DB Error" if is_group_chat(
                update) else "⚠️ 更新状态失败"
            await update.message.reply_text(message)
            return

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text(f"✅ Status Updated: overdue\nOrder ID: {order['order_id']}")
        else:
            await update.message.reply_text(
                f"✅ 订单状态已更新为逾期\n"
                f"订单ID: {order['order_id']}\n"
                f"当前状态: overdue"
            )
    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}", exc_info=True)
        message = "❌ Error" if is_group_chat(update) else "⚠️ 处理时发生错误，请稍后重试"
        await update.message.reply_text(message)


async def set_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """标记订单为完成"""
    chat_id = update.message.chat_id

    order = db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed: No active order." if is_group_chat(
            update) else "本群没有订单"
        await update.message.reply_text(message)
        return

    if order['state'] not in ('normal', 'overdue'):
        message = "❌ Failed: State must be normal or overdue." if is_group_chat(
            update) else "只有正常或逾期状态的订单才能标记为完成"
        await update.message.reply_text(message)
        return

    # 更新订单状态
    db_operations.update_order_state(chat_id, 'end')
    group_id = order['group_id']
    amount = order['amount']

    # 1. 有效订单减少
    update_all_stats('valid', -amount, -1, group_id)

    # 2. 完成订单增加
    update_all_stats('completed', amount, 1, group_id)

    # 3. 流动资金增加
    update_liquid_capital(amount)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await update.message.reply_text(f"✅ Order Completed\nAmount: {amount:.2f}")
    else:
        await update.message.reply_text(
            f"订单已完成！\n"
            f"订单ID: {order['order_id']}\n"
            f"完成金额: {amount:.2f}"
        )


async def set_breach(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """标记为违约"""
    chat_id = update.message.chat_id

    order = db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed: No active order." if is_group_chat(
            update) else "本群没有订单"
        await update.message.reply_text(message)
        return

    if order['state'] != 'overdue':
        message = "❌ Failed: Order must be overdue." if is_group_chat(
            update) else "只有逾期状态的订单才能标记为违约"
        await update.message.reply_text(message)
        return

    # 更新订单状态
    db_operations.update_order_state(chat_id, 'breach')
    group_id = order['group_id']
    amount = order['amount']

    # 1. 有效订单减少
    update_all_stats('valid', -amount, -1, group_id)

    # 2. 违约订单增加
    update_all_stats('breach', amount, 1, group_id)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await update.message.reply_text(f"✅ Marked as Breach\nAmount: {amount:.2f}")
    else:
        await update.message.reply_text(
            f"订单已标记为违约！\n"
            f"订单ID: {order['order_id']}\n"
            f"违约金额: {amount:.2f}"
        )


async def set_breach_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """违约订单完成 - 步骤1：请求金额"""
    chat_id = update.message.chat_id

    order = db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed: No active order." if is_group_chat(
            update) else "本群没有订单"
        await update.message.reply_text(message)
        return

    if order['state'] != 'breach':
        message = "❌ Failed: Order must be in breach." if is_group_chat(
            update) else "只有违约状态的订单才能标记为违约完成"
        await update.message.reply_text(message)
        return

    # 询问金额
    if is_group_chat(update):
        await update.message.reply_text(
            "Please enter the final amount for this breach order (e.g., 5000).\n"
            "This amount will be recorded as liquid capital inflow."
        )
    else:
        await update.message.reply_text("请输入违约完成金额（含本金+收益）：")

    # 设置状态，等待输入
    context.user_data['state'] = 'WAITING_BREACH_END_AMOUNT'
    context.user_data['breach_end_chat_id'] = chat_id


async def generate_report_text(period_type: str, start_date: str, end_date: str, group_id: str = None) -> str:
    """生成报表文本"""
    import pytz

    # 获取当前状态数据（资金和有效订单）
    if group_id:
        current_data = db_operations.get_grouped_data(group_id)
        report_title = f"归属ID {group_id} 的报表"
    else:
        current_data = db_operations.get_financial_data()
        report_title = "全局报表"

    # 获取周期统计数据
    stats = db_operations.get_stats_by_date_range(
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
                "📅 本月报表", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}"),
            InlineKeyboardButton(
                "📆 按日期查询", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
        ],
        [
            InlineKeyboardButton(
                "🏢 公司开销", callback_data="report_record_company"),
            InlineKeyboardButton("📝 其他开销", callback_data="report_record_other")
        ],
        [
            InlineKeyboardButton(
                "🔍 查找 & 锁定", callback_data="search_lock_start"),
            InlineKeyboardButton("📢 群发通知", callback_data="broadcast_start")
        ]
    ]

    # 如果是全局报表，显示归属查询按钮
    if not group_id:
        keyboard.append([
            InlineKeyboardButton(
                "🔍 归属报表查询", callback_data="report_menu_attribution"),
            InlineKeyboardButton(
                "🔍 查找 & 锁定", callback_data="search_lock_start"),
            InlineKeyboardButton("📢 群发通知", callback_data="broadcast_start")
        ])
    else:
        keyboard.append([InlineKeyboardButton(
            "🔙 返回全局", callback_data="report_view_today_ALL")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(report_text, reply_markup=reply_markup)


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

    # 确定发送消息的方法
    if update.callback_query:
        send_msg = update.callback_query.message.reply_text
    else:
        send_msg = update.message.reply_text

    await send_msg(f"ℹ️ 已锁定 {len(locked_groups)} 个群组，可使用群发功能。")

    # 格式化输出
    if len(orders) == 1:
        order = orders[0]
        chat_id = order['chat_id']

        # 尝试获取群组信息
        chat_title = None
        chat_username = None
        try:
            chat = await context.bot.get_chat(chat_id)
            chat_title = chat.title or "未命名群组"
            if hasattr(chat, 'username') and chat.username:
                chat_username = chat.username
        except Exception as e:
            logger.debug(f"无法获取群组 {chat_id} 的信息: {e}")

        # 构建结果消息
        result = "📍 找到订单所在群组：\n\n"

        if chat_title:
            result += f"📋 群组名称: {chat_title}\n"

        result += (
            f"🆔 群组ID: `{chat_id}`\n"
            f"📝 订单ID: {order['order_id']}\n"
            f"💰 金额: {order['amount']:.2f}\n"
            f"📊 状态: {order['state']}\n"
        )

        # 添加跳转方式
        if chat_username:
            result += f"\n🔗 直接跳转: @{chat_username}"
        else:
            result += f"\n💡 在Telegram中搜索群组ID: {chat_id}"
            result += f"\n   或使用: tg://openmessage?chat_id={chat_id}"
    else:
        result = f"📍 找到 {len(orders)} 个订单的群组：\n\n"
        for i, order in enumerate(orders[:20], 1):  # 最多显示20个
            chat_id = order['chat_id']
            chat_title = None
            try:
                chat = await context.bot.get_chat(chat_id)
                chat_title = chat.title or "未命名群组"
            except:
                pass

            if chat_title:
                result += f"{i}. 📋 {chat_title}\n"
            else:
                result += f"{i}. 🆔 群组ID: {chat_id}\n"

            result += (
                f"   📝 订单: {order['order_id']} | "
                f"💰 {order['amount']:.2f} | "
                f"📊 {order['state']}\n"
                f"   🔗 tg://openmessage?chat_id={chat_id}\n\n"
            )
        if len(orders) > 20:
            result += f"⚠️ 还有 {len(orders) - 20} 个订单未显示"

    await send_msg(result, parse_mode='Markdown')


async def handle_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理搜索相关的回调"""
    query = update.callback_query
    data = query.data

    if data == "search_menu_state":
        keyboard = [
            [InlineKeyboardButton(
                "正常 (Normal)", callback_data="search_do_state_normal")],
            [InlineKeyboardButton(
                "逾期 (Overdue)", callback_data="search_do_state_overdue")],
            [InlineKeyboardButton(
                "违约 (Breach)", callback_data="search_do_state_breach")],
            [InlineKeyboardButton(
                "完成 (End)", callback_data="search_do_state_end")],
            [InlineKeyboardButton("违约完成 (Breach End)",
                                  callback_data="search_do_state_breach_end")],
            [InlineKeyboardButton("🔙 返回", callback_data="search_start")]
        ]
        await query.edit_message_text("请选择状态：", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_menu_attribution":
        group_ids = db_operations.get_all_group_ids()
        if not group_ids:
            await query.edit_message_text("⚠️ 暂无归属ID数据",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回", callback_data="search_start")]]))
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
            "🔙 返回", callback_data="search_start")])
        await query.edit_message_text("请选择归属ID：", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_menu_group":
        keyboard = [
            [InlineKeyboardButton("周一", callback_data="search_do_group_一"), InlineKeyboardButton(
                "周二", callback_data="search_do_group_二"), InlineKeyboardButton("周三", callback_data="search_do_group_三")],
            [InlineKeyboardButton("周四", callback_data="search_do_group_四"), InlineKeyboardButton(
                "周五", callback_data="search_do_group_五"), InlineKeyboardButton("周六", callback_data="search_do_group_六")],
            [InlineKeyboardButton("周日", callback_data="search_do_group_日")],
            [InlineKeyboardButton("🔙 返回", callback_data="search_start")]
        ]
        await query.edit_message_text("请选择星期分组：", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_start":
        keyboard = [
            [
                InlineKeyboardButton(
                    "按状态查找", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "按归属查找", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "按群组查找", callback_data="search_menu_group")
            ]
        ]
        await query.edit_message_text("🔍 请选择查找方式：", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_lock_start":
        await query.message.reply_text(
            "🔍 请输入查找条件（支持混合条件）：\n"
            "格式：条件1=值1 条件2=值2\n"
            "示例：`group_id=S01 state=normal`\n"
            "请输入：",
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

        orders = db_operations.search_orders_advanced(criteria)
        await display_search_results_helper(update, context, orders)
        return


async def handle_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理报表相关的回调"""
    query = update.callback_query
    data = query.data

    if data == "report_record_company":
        date = get_daily_period_date()
        records = db_operations.get_expense_records(date, date, 'company')

        msg = f"🏢 今日公司开销 ({date}):\n\n"
        if not records:
            msg += "暂无记录\n"
        else:
            total = 0
            for i, r in enumerate(records, 1):
                msg += f"{i}. {r['amount']:.2f} - {r['note'] or '无备注'}\n"
                total += r['amount']
            msg += f"\n总计: {total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "➕ 新增开销", callback_data="report_add_expense_company")],
            [
                InlineKeyboardButton(
                    "📅 本月", callback_data="report_expense_month_company"),
                InlineKeyboardButton(
                    "📆 查询", callback_data="report_expense_query_company")
            ],
            [InlineKeyboardButton(
                "🔙 返回报表", callback_data="report_view_today_ALL")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_month_company":
        import pytz
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        records = db_operations.get_expense_records(
            start_date, end_date, 'company')

        msg = f"🏢 本月公司开销 ({start_date} 至 {end_date}):\n\n"
        if not records:
            msg += "暂无记录\n"
        else:
            total = 0
            # 限制显示数量，防止消息过长
            display_records = records[-20:] if len(records) > 20 else records

            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or '无备注'}\n"
                total += r['amount']

            # 计算总额（所有记录）
            real_total = sum(r['amount'] for r in records)
            if len(records) > 20:
                msg += f"\n... (共 {len(records)} 条记录，仅显示最近20条)\n"
            msg += f"\n总计: {real_total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "🔙 返回", callback_data="report_record_company")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_query_company":
        await query.message.reply_text(
            "🏢 请输入查询日期范围：\n"
            "格式1 (单日): 2024-01-01\n"
            "格式2 (范围): 2024-01-01 2024-01-31\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'QUERY_EXPENSE_COMPANY'
        return

    if data == "report_add_expense_company":
        await query.message.reply_text(
            "🏢 请输入公司开销金额和备注：\n"
            "格式：金额 备注\n"
            "示例：100 服务器费用"
        )
        context.user_data['state'] = 'WAITING_EXPENSE_COMPANY'
        return

    if data == "report_record_other":
        date = get_daily_period_date()
        records = db_operations.get_expense_records(date, date, 'other')

        msg = f"📝 今日其他开销 ({date}):\n\n"
        if not records:
            msg += "暂无记录\n"
        else:
            total = 0
            for i, r in enumerate(records, 1):
                msg += f"{i}. {r['amount']:.2f} - {r['note'] or '无备注'}\n"
                total += r['amount']
            msg += f"\n总计: {total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "➕ 新增开销", callback_data="report_add_expense_other")],
            [
                InlineKeyboardButton(
                    "📅 本月", callback_data="report_expense_month_other"),
                InlineKeyboardButton(
                    "📆 查询", callback_data="report_expense_query_other")
            ],
            [InlineKeyboardButton(
                "🔙 返回报表", callback_data="report_view_today_ALL")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_month_other":
        import pytz
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        records = db_operations.get_expense_records(
            start_date, end_date, 'other')

        msg = f"📝 本月其他开销 ({start_date} 至 {end_date}):\n\n"
        if not records:
            msg += "暂无记录\n"
        else:
            display_records = records[-20:] if len(records) > 20 else records
            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or '无备注'}\n"

            real_total = sum(r['amount'] for r in records)
            if len(records) > 20:
                msg += f"\n... (共 {len(records)} 条记录，仅显示最近20条)\n"
            msg += f"\n总计: {real_total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton("🔙 返回", callback_data="report_record_other")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_query_other":
        await query.message.reply_text(
            "📝 请输入查询日期范围：\n"
            "格式1 (单日): 2024-01-01\n"
            "格式2 (范围): 2024-01-01 2024-01-31\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'QUERY_EXPENSE_OTHER'
        return

    if data == "report_add_expense_other":
        await query.message.reply_text(
            "📝 请输入其他开销金额和备注：\n"
            "格式：金额 备注\n"
            "示例：50 办公用品"
        )
        context.user_data['state'] = 'WAITING_EXPENSE_OTHER'
        return

    if data == "report_menu_attribution":
        group_ids = db_operations.get_all_group_ids()
        if not group_ids:
            await query.edit_message_text(
                "⚠️ 暂无归属ID数据",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 返回", callback_data="report_view_today_ALL")]])
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
            "🔙 返回", callback_data="report_view_today_ALL")])
        await query.edit_message_text("请选择归属ID查看报表：", reply_markup=InlineKeyboardMarkup(keyboard))
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
                    "📅 本月报表", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}"),
                InlineKeyboardButton(
                    "📆 按日期查询", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
            ],
            [
                InlineKeyboardButton(
                    "🏢 公司开销", callback_data="report_record_company"),
                InlineKeyboardButton(
                    "📝 其他开销", callback_data="report_record_other")
            ]
        ]
        # 全局视图添加通用按钮
        if not group_id:
            keyboard.append([
                InlineKeyboardButton(
                    "🔍 归属报表查询", callback_data="report_menu_attribution"),
                InlineKeyboardButton(
                    "🔍 查找 & 锁定", callback_data="search_lock_start"),
                InlineKeyboardButton("📢 群发通知", callback_data="broadcast_start")
            ])
        else:
            keyboard.append([InlineKeyboardButton(
                "🔙 返回全局", callback_data="report_view_today_ALL")])

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
                    "📄 今日报表", callback_data=f"report_view_today_{group_id if group_id else 'ALL'}"),
                InlineKeyboardButton(
                    "📆 按日期查询", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
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
    # 注意：如果 answer 抛错（比如过期），后面的逻辑可能不会执行，或者抛出异常被 error_handler 捕获
    # 通常建议先执行逻辑再 answer，或者 answer 不带参数。
    # 但在这里为了用户体验先 answer
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
            await query.message.reply_text("⚠️ 当前没有锁定的群组，请先使用查找功能锁定群组。")
            return

        await query.message.reply_text(
            f"📢 准备向 {len(locked_groups)} 个群组发送通知。\n"
            "请输入要发送的消息内容：\n"
            "(输入 'cancel' 取消)"
        )
        context.user_data['state'] = 'BROADCASTING'
    else:
        logger.warning(f"Unhandled callback data: {data}")
        await query.message.reply_text(f"⚠️ 未知的操作: {data}")


async def show_current_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示当前订单状态"""
    chat_id = update.message.chat_id

    order = db_operations.get_order_by_chat_id(chat_id)
    if not order:
        await update.message.reply_text("本群没有订单")
        return

    await update.message.reply_text(
        f"当前订单状态:\n"
        f"订单ID: {order['order_id']}\n"
        f"归属ID: {order['group_id']}\n"
        f"创建日期: {order['date']}\n"
        f"分组: {order['weekday_group']}\n"
        f"客户类型: {order['customer']}\n"
        f"当前金额: {order['amount']:.2f}\n"
        f"状态: {order['state']}"
    )


@error_handler
@admin_required
async def adjust_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """调整流动资金余额命令"""
    # ... simplified logic ...
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
    update_liquid_capital(amount)

    financial_data = db_operations.get_financial_data()
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
    existing_groups = db_operations.get_all_group_ids()
    if group_id in existing_groups:
        await update.message.reply_text(f"⚠️ 归属ID {group_id} 已存在")
        return

    # 创建分组数据记录
    db_operations.update_grouped_data(group_id, 'valid_orders', 0)
    await update.message.reply_text(f"✅ 成功创建归属ID {group_id}")


async def list_attributions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出所有归属ID"""
    group_ids = db_operations.get_all_group_ids()

    if not group_ids:
        await update.message.reply_text("暂无归属ID，使用 /create_attribution <ID> 创建")
        return

    message = "📋 所有归属ID:\n\n"
    for i, group_id in enumerate(sorted(group_ids), 1):
        data = db_operations.get_grouped_data(group_id)
        message += (
            f"{i}. {group_id}\n"
            f"   有效订单: {data['valid_orders']} | "
            f"金额: {data['valid_amount']:.2f}\n"
        )

    await update.message.reply_text(message)


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理文本输入（用于搜索和群发）"""
    user_state = context.user_data.get('state')

    # 如果没有状态，或者不是在私聊中，或者是快捷操作，交给其他处理器
    if not user_state or update.effective_chat.type != 'private' or update.message.text.startswith('+'):
        return

    text = update.message.text.strip()

    if text.lower() == 'cancel':
        context.user_data['state'] = None
        await update.message.reply_text("✅ 操作已取消")
        return

    if user_state == 'WAITING_BREACH_END_AMOUNT':
        try:
            amount = float(text)
            if amount <= 0:
                await update.message.reply_text("❌ 金额必须大于0")
                return

            chat_id = context.user_data.get('breach_end_chat_id')
            if not chat_id:
                await update.message.reply_text("❌ 状态错误，请重新执行命令")
                context.user_data['state'] = None
                return

            order = db_operations.get_order_by_chat_id(chat_id)
            if not order or order['state'] != 'breach':
                await update.message.reply_text("❌ 订单状态已改变或不存在")
                context.user_data['state'] = None
                return

            # 执行完成逻辑
            # 更新订单状态
            db_operations.update_order_state(chat_id, 'breach_end')
            group_id = order['group_id']

            # 违约完成订单增加，金额增加
            update_all_stats('breach_end', amount, 1, group_id)

            # 更新流动资金 (Liquid Flow & Cash Balance)
            update_liquid_capital(amount)

            msg_en = f"✅ Breach Order Ended\nAmount: {amount:.2f}"
            msg_cn = (
                f"违约订单已完成！\n"
                f"订单ID: {order['order_id']}\n"
                f"完成金额: {amount:.2f}\n"
                f"状态: breach_end"
            )

            # 如果是在群里操作的，或者需要通知群
            # set_breach_end 记录的 chat_id 是订单所在的群/私聊ID
            # 如果是在私聊中操作，但 update_liquid_capital 记录了...

            # 我们直接回复当前操作者
            await update.message.reply_text("✅ 操作成功")

            # 如果当前聊天不是订单所在的聊天（例如私聊操作群订单），通知群组
            if update.effective_chat.id != chat_id:
                await context.bot.send_message(chat_id=chat_id, text=msg_en)
            elif is_group_chat(update):
                await update.message.reply_text(msg_en)
            else:
                await update.message.reply_text(msg_cn)

            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ 请输入有效的数字金额")
        except Exception as e:
            logger.error(f"处理违约完成时出错: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 处理出错: {e}")
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
                await update.message.reply_text("❌ 格式错误。请输入 'YYYY-MM-DD' 或 'YYYY-MM-DD YYYY-MM-DD'")
                return

            # 验证日期格式
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")

            expense_type = 'company' if user_state == 'QUERY_EXPENSE_COMPANY' else 'other'
            records = db_operations.get_expense_records(
                start_date, end_date, expense_type)

            title = "公司开销" if expense_type == 'company' else "其他开销"
            msg = f"🔍 {title}查询 ({start_date} 至 {end_date}):\n\n"

            if not records:
                msg += "暂无记录\n"
            else:
                total = 0
                # 限制显示数量，防止消息过长
                display_records = records[-20:] if len(
                    records) > 20 else records

                for r in display_records:
                    msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or '无备注'}\n"
                    total += r['amount']

                # 计算总额（所有记录）
                real_total = sum(r['amount'] for r in records)
                if len(records) > 20:
                    msg += f"\n... (共 {len(records)} 条记录，仅显示最近20条)\n"
                msg += f"\n总计: {real_total:.2f}\n"

            back_callback = "report_record_company" if expense_type == 'company' else "report_record_other"
            keyboard = [[InlineKeyboardButton(
                "🔙 返回", callback_data=back_callback)]]
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
        except Exception as e:
            logger.error(f"查询开销出错: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 查询出错: {e}")
        return

    if user_state in ['WAITING_EXPENSE_COMPANY', 'WAITING_EXPENSE_OTHER']:
        try:
            # 格式: 金额 备注
            parts = text.strip().split(maxsplit=1)
            if len(parts) < 2:
                amount_str = parts[0]
                note = "无备注"
            else:
                amount_str, note = parts

            amount = float(amount_str)
            if amount <= 0:
                await update.message.reply_text("❌ 金额必须大于0")
                return

            expense_type = 'company' if user_state == 'WAITING_EXPENSE_COMPANY' else 'other'
            date_str = get_daily_period_date()

            # 记录开销
            db_operations.record_expense(date_str, expense_type, amount, note)

            financial_data = db_operations.get_financial_data()
            await update.message.reply_text(
                f"✅ 开销记录成功\n"
                f"类型: {'公司开销' if expense_type == 'company' else '其他开销'}\n"
                f"金额: {amount:.2f}\n"
                f"备注: {note}\n"
                f"当前现金余额: {financial_data['liquid_funds']:.2f}"
            )
            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ 金额格式错误。示例: 100 服务器费用")
        except Exception as e:
            logger.error(f"记录开销时出错: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 处理出错: {e}")
        return

    if user_state == 'SEARCHING':
        # ... (keep existing search logic) ...
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
                await update.message.reply_text("❌ 无法识别搜索条件", parse_mode='Markdown')
                return

            orders = db_operations.search_orders_advanced(criteria)

            if not orders:
                await update.message.reply_text("❌ 未找到匹配的订单")
                context.user_data['state'] = None
                return

            # 锁定群组
            locked_groups = list(set(order['chat_id'] for order in orders))
            context.user_data['locked_groups'] = locked_groups

            await update.message.reply_text(
                f"✅ 找到 {len(orders)} 个订单，涉及 {len(locked_groups)} 个群组。\n"
                f"已锁定这些群组，您现在可以使用【群发通知】功能发送消息。\n"
                f"输入 'cancel' 退出锁定状态（但保留锁定列表）。"
            )
            # 退出输入状态，但保留 locked_groups
            context.user_data['state'] = None

        except Exception as e:
            logger.error(f"搜索出错: {e}")
            await update.message.reply_text(f"⚠️ 搜索出错: {e}")
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
                await update.message.reply_text("❌ 格式错误。请输入 'YYYY-MM-DD' 或 'YYYY-MM-DD YYYY-MM-DD'")
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
                        "📄 今日报表", callback_data=f"report_view_today_{group_id if group_id else 'ALL'}"),
                    InlineKeyboardButton(
                        "📅 本月报表", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}")
                ]
            ]

            await update.message.reply_text(report_text, reply_markup=InlineKeyboardMarkup(keyboard))
            context.user_data['state'] = None

        except ValueError:
            await update.message.reply_text("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
        except Exception as e:
            logger.error(f"查询报表出错: {e}")
            await update.message.reply_text(f"⚠️ 查询出错: {e}")
            context.user_data['state'] = None

    elif user_state == 'BROADCASTING':
        locked_groups = context.user_data.get('locked_groups', [])
        if not locked_groups:
            await update.message.reply_text("⚠️ 锁定列表为空")
            context.user_data['state'] = None
            return

        success_count = 0
        fail_count = 0

        await update.message.reply_text(f"⏳ 正在发送消息到 {len(locked_groups)} 个群组...")

        for chat_id in locked_groups:
            try:
                await context.bot.send_message(chat_id=chat_id, text=text)
                success_count += 1
            except Exception as e:
                logger.error(f"群发失败 {chat_id}: {e}")
                fail_count += 1

        await update.message.reply_text(
            f"✅ 群发完成\n"
            f"成功: {success_count}\n"
            f"失败: {fail_count}"
        )
        context.user_data['state'] = None


async def search_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查找订单（支持交互式菜单和旧命令方式）"""
    # 如果没有参数，显示交互式菜单
    if not context.args:
        keyboard = [
            [
                InlineKeyboardButton(
                    "按状态查找", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "按归属查找", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "按群组查找", callback_data="search_menu_group")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🔍 请选择查找方式：", reply_markup=reply_markup)
        return

    # 如果参数不足2个，提示用法（兼容旧习惯，或者直接忽略参数显示菜单？）
    # 既然用户想要按键方式，这里我们如果参数不对也显示菜单，或者保留原有提示。
    if len(context.args) < 2:
        keyboard = [
            [
                InlineKeyboardButton(
                    "按状态查找", callback_data="search_menu_state"),
                InlineKeyboardButton(
                    "按归属查找", callback_data="search_menu_attribution"),
                InlineKeyboardButton(
                    "按群组查找", callback_data="search_menu_group")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🔍 请选择查找方式：", reply_markup=reply_markup)
        return

    search_type = context.args[0].lower()
    orders = []

    # 构建 criteria 字典
    criteria = {}

    try:
        if search_type == 'order_id':
            if len(context.args) < 2:
                await update.message.reply_text("请提供订单ID")
                return
            criteria['order_id'] = context.args[1]
        elif search_type == 'group_id':
            if len(context.args) < 2:
                await update.message.reply_text("请提供归属ID")
                return
            criteria['group_id'] = context.args[1]
        elif search_type == 'customer':
            if len(context.args) < 2:
                await update.message.reply_text("请提供客户类型 (A 或 B)")
                return
            criteria['customer'] = context.args[1].upper()
        elif search_type == 'state':
            if len(context.args) < 2:
                await update.message.reply_text("请提供状态")
                return
            criteria['state'] = context.args[1]
        elif search_type == 'date':
            if len(context.args) < 3:
                await update.message.reply_text("请提供开始日期和结束日期 (格式: YYYY-MM-DD)")
                return
            criteria['date_range'] = (context.args[1], context.args[2])
        elif search_type == 'group':  # 支持按群组(星期)查找
            if len(context.args) < 2:
                await update.message.reply_text("请提供群组 (如: 一, 周一)")
                return
            val = context.args[1]
            if val.startswith('周') and len(val) == 2:
                val = val[1]
            criteria['weekday_group'] = val
        else:
            await update.message.reply_text(f"未知的搜索类型: {search_type}")
            return

        orders = db_operations.search_orders_advanced(criteria)
        await display_search_results_helper(update, context, orders)

    except Exception as e:
        logger.error(f"搜索订单时出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 搜索时出错: {str(e)}")


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

    # 自动订单创建（新成员入群监听）
    application.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_chat_members))

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
        authorized_required(button_callback)))

    # 启动机器人
    try:
        print("✅ 机器人已启动，等待消息...")
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
