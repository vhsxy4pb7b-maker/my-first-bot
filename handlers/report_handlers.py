"""报表相关处理器"""
import logging
from datetime import datetime
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.date_helpers import get_daily_period_date
from decorators import error_handler, authorized_required, private_chat_only

logger = logging.getLogger(__name__)


async def generate_report_text(period_type: str, start_date: str, end_date: str, group_id: str = None) -> str:
    """生成报表文本"""
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


@error_handler
@private_chat_only
@authorized_required
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

    # 构建按钮（中文）
    keyboard = [
        [
            InlineKeyboardButton(
                "📅 月报", callback_data=f"report_view_month_{group_id if group_id else 'ALL'}"),
            InlineKeyboardButton(
                "📆 日期查询", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
        ],
        [
            InlineKeyboardButton(
                "🏢 公司开销", callback_data="report_record_company"),
            InlineKeyboardButton(
                "📝 其他开销", callback_data="report_record_other")
        ]
    ]

    # 如果是全局报表，显示归属查询和查找功能按钮
    if not group_id:
        keyboard.append([
            InlineKeyboardButton(
                "🔍 按归属查询", callback_data="report_menu_attribution"),
            InlineKeyboardButton(
                "🔎 查找订单", callback_data="report_search_orders")
        ])
    else:
        keyboard.append([InlineKeyboardButton(
            "🔙 返回", callback_data="report_view_today_ALL")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(report_text, reply_markup=reply_markup)

