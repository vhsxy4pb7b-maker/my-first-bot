"""搜索相关回调处理器"""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.message_helpers import display_search_results_helper


async def handle_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理搜索相关的回调"""
    query = update.callback_query
    data = query.data

    if data == "search_menu_state":
        keyboard = [
            [InlineKeyboardButton(
                "正常", callback_data="search_do_state_normal")],
            [InlineKeyboardButton(
                "逾期", callback_data="search_do_state_overdue")],
            [InlineKeyboardButton(
                "违约", callback_data="search_do_state_breach")],
            [InlineKeyboardButton(
                "完成", callback_data="search_do_state_end")],
            [InlineKeyboardButton("违约完成",
                                  callback_data="search_do_state_breach_end")],
            [InlineKeyboardButton("🔙 返回", callback_data="search_start")]
        ]
        await query.edit_message_text("请选择状态:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_menu_attribution":
        group_ids = await db_operations.get_all_group_ids()
        if not group_ids:
            await query.edit_message_text("⚠️ 无归属数据",
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
        await query.edit_message_text("请选择归属ID:", reply_markup=InlineKeyboardMarkup(keyboard))
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
        await query.edit_message_text("请选择星期分组:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_start":
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
        await query.edit_message_text("🔍 查找方式:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "search_lock_start":
        await query.message.reply_text(
            "🔍 请输入查询条件（支持综合查询）：\n\n"
            "单一查询：\n"
            "• S01（按归属查询）\n"
            "• 三（按星期分组查询）\n"
            "• 正常（按状态查询）\n\n"
            "综合查询：\n"
            "• 三 正常（周三的正常订单）\n"
            "• S01 正常（S01的正常订单）\n\n"
            "请输入:",
            parse_mode='Markdown'
        )
        context.user_data['state'] = 'SEARCHING'
        return

    if data == "search_change_attribution":
        # 获取查找结果
        orders = context.user_data.get('search_orders', [])
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
                gid, callback_data=f"search_change_to_{gid}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton(
            "🔙 取消", callback_data="search_start")])

        order_count = len(orders)
        total_amount = sum(order.get('amount', 0) for order in orders)

        await query.edit_message_text(
            f"🔄 更改归属\n\n"
            f"找到订单: {order_count} 个\n"
            f"订单金额: {total_amount:,.2f}\n\n"
            f"请选择新的归属ID:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("search_change_to_"):
        # 处理归属变更
        new_group_id = data[17:]  # 提取新的归属ID

        orders = context.user_data.get('search_orders', [])
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
        context.user_data.pop('search_orders', None)
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
