import logging
from datetime import datetime, date
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


def private_chat_only(func):
    """检查是否在私聊中使用命令的装饰器"""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_chat.type != "private":
            await update.message.reply_text("⚠️ 此命令只能在私聊中使用")
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
    """获取当前日结周期对应的日期（11:00-23:00为一个周期）"""
    from datetime import datetime, timedelta
    import pytz

    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    current_hour = now.hour

    # 如果当前时间在23:00-11:00之间，使用昨天的日期
    # 如果当前时间在11:00-23:00之间，使用今天的日期
    if current_hour < 11:
        # 23:00-11:00之间，使用昨天的日期
        period_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # 11:00-23:00之间，使用今天的日期
        period_date = now.strftime("%Y-%m-%d")

    return period_date


def generate_order_id():
    """生成订单ID"""
    return db_operations.get_next_order_id()


def update_grouped_data(group_id, field, amount):
    """更新分组数据"""
    db_operations.update_grouped_data(group_id, field, amount)


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
        "/list_attributions - 列出所有归属ID\n\n"
        "⚠️ 所有操作都需要管理员权限".format(financial_data['liquid_funds'])
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

    # 创建订单
    order_id = generate_order_id()
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

    # 更新财务数据
    db_operations.update_financial_data('valid_orders', 1)
    db_operations.update_financial_data('valid_amount', amount)
    db_operations.update_financial_data('liquid_funds', -amount)

    if customer == 'A':
        db_operations.update_financial_data('new_clients', 1)
        db_operations.update_financial_data('new_clients_amount', amount)
    else:
        db_operations.update_financial_data('old_clients', 1)
        db_operations.update_financial_data('old_clients_amount', amount)

    # 更新分组数据
    update_grouped_data(group_id, 'valid_orders', 1)
    update_grouped_data(group_id, 'valid_amount', amount)
    if customer == 'A':
        update_grouped_data(group_id, 'new_clients', 1)
        update_grouped_data(group_id, 'new_clients_amount', amount)
    else:
        update_grouped_data(group_id, 'old_clients', 1)
        update_grouped_data(group_id, 'old_clients_amount', amount)

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


async def handle_amount_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理金额操作（需要管理员权限）"""
    # 检查是否有消息对象
    if not update.message or not update.message.text:
        return

    # 权限检查
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id or user_id not in ADMIN_IDS:
        logger.debug(f"用户 {user_id} 无权限执行快捷操作")
        return  # 非管理员不处理

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
                message = "❌ Failed" if is_group_chat(
                    update) else "❌ 本群没有订单，无法进行本金减少操作"
                await update.message.reply_text(message)
                return
            amount = float(amount_text[:-1])
            await process_principal_reduction(update, order, amount)
        elif amount_text.endswith('c'):
            # 违约协商还款 - 需要订单
            if not order:
                message = "❌ Failed" if is_group_chat(
                    update) else "❌ 本群没有订单，无法进行违约协商还款操作"
                await update.message.reply_text(message)
                return
            amount = float(amount_text[:-1])
            await process_breach_payment(update, order, amount)
        else:
            # 利息收入 - 不需要订单，但如果有订单会关联到订单的归属ID
            try:
                amount = float(amount_text)
                if order:
                    # 如果有订单，关联到订单的归属ID
                    await process_interest(update, order, amount)
                else:
                    # 如果没有订单，只更新全局财务数据
                    db_operations.update_financial_data('interest', amount)
                    db_operations.update_financial_data('liquid_funds', amount)
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
                message = "❌ Failed" if is_group_chat(
                    update) else "❌ 金额格式错误，请输入有效的数字"
                await update.message.reply_text(message)
    except ValueError:
        message = "❌ Failed" if is_group_chat(
            update) else "❌ 金额格式错误，请输入有效的数字\n示例：+1000 或 +1000b 或 +1000c"
        await update.message.reply_text(message)
    except Exception as e:
        logger.error(f"处理金额操作时出错: {e}", exc_info=True)
        message = "❌ Failed" if is_group_chat(
            update) else f"⚠️ 处理时发生错误: {str(e)}"
        await update.message.reply_text(message)


async def process_principal_reduction(update: Update, order: dict, amount: float):
    """处理本金减少"""
    try:
        if order['state'] not in ('normal', 'overdue'):
            message = "❌ Failed" if is_group_chat(
                update) else "❌ 当前订单状态不支持本金减少操作"
            await update.message.reply_text(message)
            return

        if amount <= 0:
            message = "❌ Failed" if is_group_chat(update) else "❌ 金额必须大于0"
            await update.message.reply_text(message)
            return

        if amount > order['amount']:
            message = "❌ Failed" if is_group_chat(update) else (
                f"❌ 金额超过订单金额\n"
                f"订单金额: {order['amount']:.2f}\n"
                f"输入金额: {amount:.2f}"
            )
            await update.message.reply_text(message)
            return

        # 更新订单金额
        new_amount = order['amount'] - amount
        if not db_operations.update_order_amount(order['chat_id'], new_amount):
            message = "❌ Failed" if is_group_chat(update) else "⚠️ 更新订单金额失败"
            await update.message.reply_text(message)
            return

        group_id = order['group_id']

        # 更新财务数据
        db_operations.update_financial_data('valid_amount', -amount)
        db_operations.update_financial_data('completed_amount', amount)
        db_operations.update_financial_data('liquid_funds', amount)

        # 更新分组数据
        update_grouped_data(group_id, 'valid_amount', -amount)
        update_grouped_data(group_id, 'completed_amount', amount)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text("✅ Success")
        else:
            await update.message.reply_text(
                f"✅ 本金减少成功！\n"
                f"订单ID: {order['order_id']}\n"
                f"减少金额: {amount:.2f}\n"
                f"剩余金额: {new_amount:.2f}"
            )
    except Exception as e:
        logger.error(f"处理本金减少时出错: {e}", exc_info=True)
        await update.message.reply_text("⚠️ 处理时发生错误，请稍后重试")


async def process_breach_payment(update: Update, order: dict, amount: float):
    """处理违约协商还款"""
    try:
        if order['state'] != 'breach':
            message = "❌ Failed" if is_group_chat(
                update) else "❌ 只有违约状态的订单才能进行协商还款"
            await update.message.reply_text(message)
            return

        if amount <= 0:
            message = "❌ Failed" if is_group_chat(update) else "❌ 金额必须大于0"
            await update.message.reply_text(message)
            return

        if amount > order['amount']:
            message = "❌ Failed" if is_group_chat(update) else (
                f"❌ 金额超过订单金额\n"
                f"订单金额: {order['amount']:.2f}\n"
                f"输入金额: {amount:.2f}"
            )
            await update.message.reply_text(message)
            return

        # 更新订单金额
        new_amount = order['amount'] - amount
        if not db_operations.update_order_amount(order['chat_id'], new_amount):
            await update.message.reply_text("⚠️ 更新订单金额失败")
            return

        group_id = order['group_id']

        # 更新财务数据
        db_operations.update_financial_data('breach_end_amount', amount)
        db_operations.update_financial_data('breach_end_orders', 1)
        db_operations.update_financial_data('liquid_funds', amount)

        # 更新分组数据
        update_grouped_data(group_id, 'breach_end_amount', amount)
        update_grouped_data(group_id, 'breach_end_orders', 1)

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text("✅ Success")
        else:
            await update.message.reply_text(
                f"✅ 违约协商还款成功！\n"
                f"订单ID: {order['order_id']}\n"
                f"还款金额: {amount:.2f}\n"
                f"剩余金额: {new_amount:.2f}"
            )
    except Exception as e:
        logger.error(f"处理违约还款时出错: {e}", exc_info=True)
        await update.message.reply_text("⚠️ 处理时发生错误，请稍后重试")


async def process_interest(update: Update, order: dict, amount: float):
    """处理利息收入"""
    try:
        if amount <= 0:
            await update.message.reply_text("❌ 金额必须大于0")
            return

        # 更新财务数据
        db_operations.update_financial_data('interest', amount)
        db_operations.update_financial_data('liquid_funds', amount)

        # 更新分组数据
        update_grouped_data(order['group_id'], 'interest', amount)

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
    except Exception as e:
        logger.error(f"处理利息收入时出错: {e}", exc_info=True)
        await update.message.reply_text("⚠️ 处理时发生错误，请稍后重试")


async def set_normal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """转为正常状态"""
    try:
        chat_id = update.message.chat_id

        order = db_operations.get_order_by_chat_id(chat_id)
        if not order:
            message = "❌ Failed" if is_group_chat(update) else "❌ 本群没有订单"
            await update.message.reply_text(message)
            return

        if order['state'] != 'overdue':
            message = "❌ Failed" if is_group_chat(
                update) else "❌ 只有逾期状态的订单才能转为正常状态"
            await update.message.reply_text(message)
            return

        if not db_operations.update_order_state(chat_id, 'normal'):
            message = "❌ Failed" if is_group_chat(update) else "⚠️ 更新状态失败"
            await update.message.reply_text(message)
            return

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text("✅ Success")
        else:
            await update.message.reply_text(
                f"✅ 订单状态已更新为正常\n"
                f"订单ID: {order['order_id']}\n"
                f"当前状态: normal"
            )
    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}", exc_info=True)
        await update.message.reply_text("⚠️ 处理时发生错误，请稍后重试")


async def set_overdue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """转为逾期状态"""
    try:
        chat_id = update.message.chat_id

        order = db_operations.get_order_by_chat_id(chat_id)
        if not order:
            message = "❌ Failed" if is_group_chat(update) else "❌ 本群没有订单"
            await update.message.reply_text(message)
            return

        if order['state'] != 'normal':
            message = "❌ Failed" if is_group_chat(
                update) else "❌ 只有正常状态的订单才能转为逾期"
            await update.message.reply_text(message)
            return

        if not db_operations.update_order_state(chat_id, 'overdue'):
            message = "❌ Failed" if is_group_chat(update) else "⚠️ 更新状态失败"
            await update.message.reply_text(message)
            return

        # 群组只回复成功，私聊显示详情
        if is_group_chat(update):
            await update.message.reply_text("✅ Success")
        else:
            await update.message.reply_text(
                f"✅ 订单状态已更新为逾期\n"
                f"订单ID: {order['order_id']}\n"
                f"当前状态: overdue"
            )
    except Exception as e:
        logger.error(f"更新订单状态时出错: {e}", exc_info=True)
        await update.message.reply_text("⚠️ 处理时发生错误，请稍后重试")


async def set_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """标记订单为完成"""
    chat_id = update.message.chat_id

    order = db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed" if is_group_chat(update) else "本群没有订单"
        await update.message.reply_text(message)
        return

    if order['state'] not in ('normal', 'overdue'):
        message = "❌ Failed" if is_group_chat(
            update) else "只有正常或逾期状态的订单才能标记为完成"
        await update.message.reply_text(message)
        return

    # 更新订单状态
    db_operations.update_order_state(chat_id, 'end')
    group_id = order['group_id']
    amount = order['amount']

    # 更新财务数据
    db_operations.update_financial_data('valid_orders', -1)
    db_operations.update_financial_data('valid_amount', -amount)
    db_operations.update_financial_data('completed_orders', 1)
    db_operations.update_financial_data('completed_amount', amount)
    db_operations.update_financial_data('liquid_funds', amount)

    # 更新分组数据
    update_grouped_data(group_id, 'valid_orders', -1)
    update_grouped_data(group_id, 'valid_amount', -amount)
    update_grouped_data(group_id, 'completed_orders', 1)
    update_grouped_data(group_id, 'completed_amount', amount)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await update.message.reply_text("✅ Success")
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
        message = "❌ Failed" if is_group_chat(update) else "本群没有订单"
        await update.message.reply_text(message)
        return

    if order['state'] != 'overdue':
        message = "❌ Failed" if is_group_chat(update) else "只有逾期状态的订单才能标记为违约"
        await update.message.reply_text(message)
        return

    # 更新订单状态
    db_operations.update_order_state(chat_id, 'breach')
    group_id = order['group_id']
    amount = order['amount']

    # 更新财务数据
    db_operations.update_financial_data('valid_orders', -1)
    db_operations.update_financial_data('valid_amount', -amount)
    db_operations.update_financial_data('breach_orders', 1)
    db_operations.update_financial_data('breach_amount', amount)

    # 更新分组数据
    update_grouped_data(group_id, 'valid_orders', -1)
    update_grouped_data(group_id, 'valid_amount', -amount)
    update_grouped_data(group_id, 'breach_orders', 1)
    update_grouped_data(group_id, 'breach_amount', amount)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await update.message.reply_text("✅ Success")
    else:
        await update.message.reply_text(
            f"订单已标记为违约！\n"
            f"订单ID: {order['order_id']}\n"
            f"违约金额: {amount:.2f}"
        )


async def set_breach_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """违约订单完成"""
    chat_id = update.message.chat_id

    order = db_operations.get_order_by_chat_id(chat_id)
    if not order:
        message = "❌ Failed" if is_group_chat(update) else "本群没有订单"
        await update.message.reply_text(message)
        return

    if order['state'] != 'breach':
        message = "❌ Failed" if is_group_chat(update) else "只有违约状态的订单才能标记为违约完成"
        await update.message.reply_text(message)
        return

    # 更新订单状态
    db_operations.update_order_state(chat_id, 'breach_end')
    group_id = order['group_id']

    # 更新财务数据
    db_operations.update_financial_data('breach_end_orders', 1)

    # 更新分组数据
    update_grouped_data(group_id, 'breach_end_orders', 1)

    # 群组只回复成功，私聊显示详情
    if is_group_chat(update):
        await update.message.reply_text("✅ Success")
    else:
        await update.message.reply_text(
            f"违约订单已完成！\n"
            f"订单ID: {order['order_id']}\n"
            f"状态: breach_end"
        )


async def show_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示报表"""
    from datetime import datetime
    import pytz

    # 获取当前时间（北京时间）
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")

    # 检查是否有参数（特定归属ID的报表）
    if context.args:
        group_id = context.args[0]
        data = db_operations.get_grouped_data(group_id)
        if data and data.get('group_id'):
            report_type = f"归属ID {group_id} 的报表"
        else:
            await update.message.reply_text(f"找不到归属ID {group_id} 的数据")
            return
    else:
        data = db_operations.get_financial_data()
        report_type = "全局报表"

    # 获取日结数据（11:00-23:00为一个周期）
    daily_data = db_operations.get_daily_data(current_date)

    # 生成报表文本 - 有效订单和有效金额置顶
    report = (
        f"=== {report_type} ===\n"
        f"📅 {current_date} {current_time}\n"
        f"{'─' * 25}\n"
        f"📊 【累计数据】\n"
        f"有效订单数: {data['valid_orders']}\n"
        f"有效订单金额: {data['valid_amount']:.2f}\n"
        f"流动资金: {data['liquid_funds']:.2f}\n"
        f"{'─' * 25}\n"
        f"📈 【今日数据】(11:00-23:00)\n"
        f"新客户数: {daily_data['new_clients']}\n"
        f"新客户金额: {daily_data['new_clients_amount']:.2f}\n"
        f"老客户数: {daily_data['old_clients']}\n"
        f"老客户金额: {daily_data['old_clients_amount']:.2f}\n"
        f"利息收入: {daily_data['interest']:.2f}\n"
        f"完成订单数: {daily_data['completed_orders']}\n"
        f"完成订单金额: {daily_data['completed_amount']:.2f}\n"
        f"违约订单数: {daily_data['breach_orders']}\n"
        f"违约订单金额: {daily_data['breach_amount']:.2f}\n"
        f"违约完成订单数: {daily_data['breach_end_orders']}\n"
        f"违约完成金额: {daily_data['breach_end_amount']:.2f}\n"
    )

    # 添加键盘按钮用于查看分组报表
    if not context.args:  # 如果是全局报表，才显示分组按钮
        keyboard = []
        group_ids = db_operations.get_all_group_ids()
        for group_id in sorted(group_ids):
            keyboard.append([InlineKeyboardButton(
                f"查看 {group_id} 报表", callback_data=f"report_{group_id}")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(report, reply_markup=reply_markup)
    else:
        await update.message.reply_text(report)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理按钮回调"""
    query = update.callback_query
    await query.answer()

    if query.data.startswith("report_"):
        from datetime import datetime
        import pytz

        group_id = query.data[7:]
        data = db_operations.get_grouped_data(group_id)
        if data and data.get('group_id'):
            tz = pytz.timezone('Asia/Shanghai')
            now = datetime.now(tz)
            current_date = now.strftime("%Y-%m-%d")
            current_time = now.strftime("%H:%M")

            daily_data = db_operations.get_daily_data(current_date, group_id)

            report = (
                f"=== 归属ID {group_id} 的报表 ===\n"
                f"📅 {current_date} {current_time}\n"
                f"{'─' * 25}\n"
                f"📊 【累计数据】\n"
                f"有效订单数: {data['valid_orders']}\n"
                f"有效订单金额: {data['valid_amount']:.2f}\n"
                f"{'─' * 25}\n"
                f"📈 【今日数据】(11:00-23:00)\n"
                f"新客户数: {daily_data['new_clients']}\n"
                f"新客户金额: {daily_data['new_clients_amount']:.2f}\n"
                f"老客户数: {daily_data['old_clients']}\n"
                f"老客户金额: {daily_data['old_clients_amount']:.2f}\n"
                f"利息收入: {daily_data['interest']:.2f}\n"
                f"完成订单数: {daily_data['completed_orders']}\n"
                f"完成订单金额: {daily_data['completed_amount']:.2f}\n"
                f"违约订单数: {daily_data['breach_orders']}\n"
                f"违约订单金额: {daily_data['breach_amount']:.2f}\n"
                f"违约完成订单数: {daily_data['breach_end_orders']}\n"
                f"违约完成金额: {daily_data['breach_end_amount']:.2f}\n"
            )
            await query.edit_message_text(text=report)
        else:
            await query.edit_message_text(text=f"找不到归属ID {group_id} 的数据")


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


async def adjust_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """调整流动资金余额命令"""
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "❌ 用法: /adjust <金额> [备注]\n"
            "示例: /adjust +5000 收入备注\n"
            "      /adjust -3000 支出备注"
        )
        return

    try:
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
        db_operations.update_financial_data('liquid_funds', amount)

        financial_data = db_operations.get_financial_data()
        await update.message.reply_text(
            f"✅ 资金调整成功\n"
            f"调整类型: {'增加' if amount > 0 else '减少'}\n"
            f"调整金额: {abs(amount):.2f}\n"
            f"调整后余额: {financial_data['liquid_funds']:.2f}\n"
            f"备注: {note}"
        )

    except ValueError:
        await update.message.reply_text("❌ 金额格式错误，请输入有效的数字")
    except Exception as e:
        logger.error(f"调整资金时出错: {e}")
        await update.message.reply_text("⚠️ 调整资金时发生错误")


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


async def search_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查找订单"""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "用法: /search <类型> <值> [值2]\n"
            "类型:\n"
            "  order_id <订单ID> - 按订单ID查找\n"
            "  group_id <归属ID> - 按归属ID查找\n"
            "  customer <A/B> - 按客户类型查找\n"
            "  state <状态> - 按状态查找\n"
            "  date <开始日期> <结束日期> - 按日期范围查找\n"
            "示例:\n"
            "  /search order_id 0001\n"
            "  /search group_id S01\n"
            "  /search customer A\n"
            "  /search state normal\n"
            "  /search date 2024-01-01 2024-01-31"
        )
        return

    search_type = context.args[0].lower()
    orders = []

    try:
        if search_type == 'order_id':
            if len(context.args) < 2:
                await update.message.reply_text("请提供订单ID")
                return
            order = db_operations.get_order_by_order_id(context.args[1])
            if order:
                orders = [order]
        elif search_type == 'group_id':
            if len(context.args) < 2:
                await update.message.reply_text("请提供归属ID")
                return
            orders = db_operations.search_orders_by_group_id(context.args[1])
        elif search_type == 'customer':
            if len(context.args) < 2:
                await update.message.reply_text("请提供客户类型 (A 或 B)")
                return
            customer = context.args[1].upper()
            if customer not in ('A', 'B'):
                await update.message.reply_text("客户类型必须是 A 或 B")
                return
            orders = db_operations.search_orders_by_customer(customer)
        elif search_type == 'state':
            if len(context.args) < 2:
                await update.message.reply_text("请提供状态")
                return
            orders = db_operations.search_orders_by_state(context.args[1])
        elif search_type == 'date':
            if len(context.args) < 3:
                await update.message.reply_text("请提供开始日期和结束日期 (格式: YYYY-MM-DD)")
                return
            start_date = context.args[1]
            end_date = context.args[2]
            orders = db_operations.search_orders_by_date_range(
                start_date, end_date)
        else:
            await update.message.reply_text(f"未知的搜索类型: {search_type}")
            return

        if not orders:
            await update.message.reply_text("❌ 未找到匹配的订单")
            return

        # 格式化输出：只显示群组定位信息
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

        # 使用 parse_mode='Markdown' 以便显示代码格式的chat_id
        await update.message.reply_text(result, parse_mode='Markdown')

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
        "start", private_chat_only(admin_required(start))))
    application.add_handler(CommandHandler(
        "report", private_chat_only(admin_required(show_report))))
    application.add_handler(CommandHandler(
        "search", private_chat_only(admin_required(search_orders))))

    # 其他需要管理员权限的命令
    application.add_handler(CommandHandler(
        "create", admin_required(create_order)))
    application.add_handler(CommandHandler(
        "normal", admin_required(set_normal)))
    application.add_handler(CommandHandler(
        "overdue", admin_required(set_overdue)))
    application.add_handler(CommandHandler("end", admin_required(set_end)))
    application.add_handler(CommandHandler(
        "breach", admin_required(set_breach)))
    application.add_handler(CommandHandler(
        "breach_end", admin_required(set_breach_end)))
    application.add_handler(CommandHandler(
        "order", admin_required(show_current_order)))

    # 资金和归属ID管理
    application.add_handler(CommandHandler(
        "adjust", private_chat_only(admin_required(adjust_funds))))
    application.add_handler(CommandHandler(
        "create_attribution", private_chat_only(admin_required(create_attribution))))
    application.add_handler(CommandHandler(
        "list_attributions", private_chat_only(admin_required(list_attributions))))

    # 添加消息处理器（金额操作）- 需要管理员权限
    # 只处理以 + 开头的消息（快捷操作）
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\+'),
        handle_amount_operation),
        group=1)  # 设置优先级组

    # 添加回调查询处理器
    application.add_handler(CallbackQueryHandler(
        admin_required(button_callback)))

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
