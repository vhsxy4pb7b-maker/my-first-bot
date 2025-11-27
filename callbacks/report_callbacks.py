"""报表相关回调处理器"""
from datetime import datetime
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.date_helpers import get_daily_period_date
from handlers.report_handlers import generate_report_text


async def handle_report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理报表相关的回调"""
    query = update.callback_query
    data = query.data

    if data == "report_record_company":
        date = get_daily_period_date()
        records = await db_operations.get_expense_records(date, date, 'company')

        msg = f"🏢 公司开销今日 ({date}):\n\n"
        if not records:
            msg += "无记录\n"
        else:
            total = 0
            for i, r in enumerate(records, 1):
                msg += f"{i}. {r['amount']:.2f} - {r['note'] or '无备注'}\n"
                total += r['amount']
            msg += f"\n总计: {total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "➕ 添加开销", callback_data="report_add_expense_company")],
            [
                InlineKeyboardButton(
                    "📅 本月", callback_data="report_expense_month_company"),
                InlineKeyboardButton(
                    "📆 查询", callback_data="report_expense_query_company")
            ],
            [InlineKeyboardButton(
                "🔙 返回", callback_data="report_view_today_ALL")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_month_company":
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        records = await db_operations.get_expense_records(
            start_date, end_date, 'company')

        msg = f"🏢 公司开销本月 ({start_date} 至 {end_date}):\n\n"
        if not records:
            msg += "无记录\n"
        else:
            # 限制显示数量，防止消息过长
            display_records = records[-20:] if len(records) > 20 else records

            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or '无备注'}\n"

            # 计算总额（所有记录）
            real_total = sum(r['amount'] for r in records)
            if len(records) > 20:
                msg += f"\n... (共 {len(records)} 条记录，显示最后20条)\n"
            msg += f"\n总计: {real_total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "🔙 返回", callback_data="report_record_company")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_query_company":
        await query.message.reply_text(
            "🏢 请输入日期范围：\n"
            "格式1 (单日): 2024-01-01\n"
            "格式2 (范围): 2024-01-01 2024-01-31\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'QUERY_EXPENSE_COMPANY'
        return

    if data == "report_add_expense_company":
        await query.message.reply_text(
            "🏢 请输入金额和备注：\n"
            "格式: 金额 备注\n"
            "示例: 100 服务器费用"
        )
        context.user_data['state'] = 'WAITING_EXPENSE_COMPANY'
        return

    if data == "report_record_other":
        date = get_daily_period_date()
        records = await db_operations.get_expense_records(date, date, 'other')

        msg = f"📝 其他开销今日 ({date}):\n\n"
        if not records:
            msg += "无记录\n"
        else:
            total = 0
            for i, r in enumerate(records, 1):
                msg += f"{i}. {r['amount']:.2f} - {r['note'] or '无备注'}\n"
                total += r['amount']
            msg += f"\n总计: {total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "➕ 添加开销", callback_data="report_add_expense_other")],
            [
                InlineKeyboardButton(
                    "📅 本月", callback_data="report_expense_month_other"),
                InlineKeyboardButton(
                    "📆 查询", callback_data="report_expense_query_other")
            ],
            [InlineKeyboardButton(
                "🔙 返回", callback_data="report_view_today_ALL")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_month_other":
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        start_date = now.replace(day=1).strftime("%Y-%m-%d")
        end_date = get_daily_period_date()

        records = await db_operations.get_expense_records(
            start_date, end_date, 'other')

        msg = f"📝 其他开销本月 ({start_date} 至 {end_date}):\n\n"
        if not records:
            msg += "无记录\n"
        else:
            display_records = records[-20:] if len(records) > 20 else records
            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or '无备注'}\n"

            real_total = sum(r['amount'] for r in records)
            if len(records) > 20:
                msg += f"\n... (共 {len(records)} 条记录，显示最后20条)\n"
            msg += f"\n总计: {real_total:.2f}\n"

        keyboard = [
            [InlineKeyboardButton(
                "🔙 返回", callback_data="report_record_other")]
        ]
        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_expense_query_other":
        await query.message.reply_text(
            "📝 请输入日期范围：\n"
            "格式1 (单日): 2024-01-01\n"
            "格式2 (范围): 2024-01-01 2024-01-31\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'QUERY_EXPENSE_OTHER'
        return

    if data == "report_add_expense_other":
        await query.message.reply_text(
            "📝 请输入金额和备注：\n"
            "格式: 金额 备注\n"
            "示例: 50 办公用品"
        )
        context.user_data['state'] = 'WAITING_EXPENSE_OTHER'
        return

    if data == "report_menu_attribution":
        # 直接显示归属ID列表供选择查看报表
        group_ids = await db_operations.get_all_group_ids()
        if not group_ids:
            await query.edit_message_text(
                "⚠️ 无归属数据",
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
        await query.edit_message_text("请选择归属ID查看报表:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "report_search_orders":
        await query.message.reply_text(
            "🔍 查找订单\n\n"
            "输入查询条件：\n\n"
            "单一查询：\n"
            "• S01（按归属查询）\n"
            "• 三（按星期分组查询）\n"
            "• 正常（按状态查询）\n\n"
            "综合查询：\n"
            "• 三 正常（周三的正常订单）\n"
            "• S01 正常（S01的正常订单）\n\n"
            "请输入:（输入 'cancel' 取消）"
        )
        context.user_data['state'] = 'REPORT_SEARCHING'
        return

    if data == "report_change_attribution":
        # 获取查找结果
        orders = context.user_data.get('report_search_orders', [])
        if not orders:
            await query.answer("❌ 没有找到订单，请先使用查找功能")
            return

        # 获取所有归属ID列表
        all_group_ids = await db_operations.get_all_group_ids()
        if not all_group_ids:
            await query.answer("❌ 没有可用的归属ID")
            return

        # 显示归属ID选择界面
        keyboard = []
        row = []
        for gid in sorted(all_group_ids):
            row.append(InlineKeyboardButton(
                gid, callback_data=f"report_change_to_{gid}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton(
            "🔙 取消", callback_data="report_view_today_ALL")])

        order_count = len(orders)
        total_amount = sum(order.get('amount', 0) for order in orders)

        await query.edit_message_text(
            f"🔄 修改归属\n\n"
            f"找到订单: {order_count} 个\n"
            f"订单金额: {total_amount:,.2f}\n\n"
            f"请选择新的归属ID:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("report_change_to_"):
        # 处理归属变更
        new_group_id = data[17:]  # 提取新的归属ID

        orders = context.user_data.get('report_search_orders', [])
        if not orders:
            await query.answer("❌ 没有找到订单")
            return

        # 执行归属变更
        from handlers.attribution_handlers import change_orders_attribution
        success_count, fail_count = await change_orders_attribution(
            update, context, orders, new_group_id
        )

        result_msg = (
            f"✅ 归属变更完成\n\n"
            f"成功: {success_count} 个订单\n"
            f"失败: {fail_count} 个订单"
        )

        await query.edit_message_text(result_msg)
        await query.answer("✅ 归属变更完成")

        # 清除查找结果
        context.user_data.pop('report_search_orders', None)
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
        # 全局视图添加通用按钮
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

        await query.edit_message_text(report_text, reply_markup=InlineKeyboardMarkup(keyboard))

    elif view_type == 'month':
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
                    "📆 日期查询", callback_data=f"report_view_query_{group_id if group_id else 'ALL'}")
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
