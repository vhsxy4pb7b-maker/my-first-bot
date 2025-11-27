"""支付账号回调处理器"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from decorators import authorized_required

logger = logging.getLogger(__name__)


@authorized_required
async def handle_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理支付账号相关的回调"""
    query = update.callback_query
    if not query:
        logger.error("handle_payment_callback: query is None")
        return

    data = query.data
    if not data:
        logger.error("handle_payment_callback: data is None")
        return

    try:
        await query.answer()
    except Exception:
        pass

    if data == "payment_select_account":
        # 在群聊中选择账户
        keyboard = [
            [
                InlineKeyboardButton(
                    "💳 GCASH", callback_data="payment_choose_gcash_type"),
                InlineKeyboardButton(
                    "💳 PayMaya", callback_data="payment_choose_paymaya_type")
            ],
            [
                InlineKeyboardButton("🔙 返回", callback_data="order_action_back")
            ]
        ]

        await query.edit_message_text(
            "💳 选择要发送的账户：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data == "payment_choose_gcash_type":
        # 显示GCASH所有账户名字列表
        accounts = await db_operations.get_payment_accounts_by_type('gcash')

        if not accounts or not any(acc.get('account_name') for acc in accounts):
            await query.answer("❌ 没有可用的GCASH账户", show_alert=True)
            return

        keyboard = []
        for account in accounts:
            account_name = account.get('account_name', '')
            if account_name:  # 只显示有名字的账户
                account_id = account.get('id')
                keyboard.append([
                    InlineKeyboardButton(
                        f"💳 {account_name}",
                        callback_data=f"payment_send_account_{account_id}"
                    )
                ])

        if not keyboard:
            await query.answer("❌ 没有可用的GCASH账户", show_alert=True)
            return

        keyboard.append([
            InlineKeyboardButton(
                "🔙 返回", callback_data="payment_select_account")
        ])

        await query.edit_message_text(
            "💳 GCASH - 选择账户：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data == "payment_choose_paymaya_type":
        # 显示PayMaya所有账户名字列表
        accounts = await db_operations.get_payment_accounts_by_type('paymaya')

        if not accounts or not any(acc.get('account_name') for acc in accounts):
            await query.answer("❌ 没有可用的PayMaya账户", show_alert=True)
            return

        keyboard = []
        for account in accounts:
            account_name = account.get('account_name', '')
            if account_name:  # 只显示有名字的账户
                account_id = account.get('id')
                keyboard.append([
                    InlineKeyboardButton(
                        f"💳 {account_name}",
                        callback_data=f"payment_send_account_{account_id}"
                    )
                ])

        if not keyboard:
            await query.answer("❌ 没有可用的PayMaya账户", show_alert=True)
            return

        keyboard.append([
            InlineKeyboardButton(
                "🔙 返回", callback_data="payment_select_account")
        ])

        await query.edit_message_text(
            "💳 PayMaya - 选择账户：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("payment_send_account_"):
        # 根据账户ID发送完整账户信息到群组
        try:
            account_id = int(data.split("_")[-1])
        except (ValueError, IndexError):
            await query.answer("❌ 无效的账户ID", show_alert=True)
            return

        account = await db_operations.get_payment_account_by_id(account_id)
        if not account:
            await query.answer("❌ 账户不存在", show_alert=True)
            return

        if not account.get('account_number'):
            await query.answer("❌ 账户号码未设置", show_alert=True)
            return

        account_type = account.get('account_type', '').upper()
        account_number = account.get('account_number', '')
        account_name = account.get('account_name', '')
        balance = account.get('balance', 0)

        message = (
            f"💳 {account_type} Payment Account\n\n"
            f"Account Number: {account_number}\n"
            f"Account Name: {account_name}\n"
            f"Current Balance: {balance:,.2f}"
        )

        chat_id = query.message.chat_id
        try:
            await context.bot.send_message(chat_id=chat_id, text=message)
            await query.answer("✅ 账户已发送到群组")
            await query.edit_message_text("✅ 账户已发送", reply_markup=None)
        except Exception as e:
            logger.error(f"发送账户失败: {e}", exc_info=True)
            await query.answer(f"❌ 发送失败: {e}", show_alert=True)
        return

    if data == "order_action_back":
        # 返回到订单界面
        chat_id = query.message.chat_id
        order = await db_operations.get_order_by_chat_id(chat_id)
        if not order:
            await query.edit_message_text("❌ 当前群组没有活跃订单")
            return

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

        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    if data == "payment_send_gcash":
        try:
            account = await db_operations.get_payment_account('gcash')
            if not account or not account.get('account_number'):
                await query.answer("❌ GCASH账号未设置", show_alert=True)
                return

            account_number = account.get('account_number', '')
            account_name = account.get('account_name', '')
            balance = account.get('balance', 0)

            # 格式化消息，方便发送给客户
            message = (
                f"💳 GCASH Payment Account\n\n"
                f"Account Number: `{account_number}`\n"
                f"Account Name: {account_name}\n"
                f"Current Balance: {balance:,.2f}\n\n"
                f"请将上述账号信息发送给客户。"
            )

            keyboard = [
                [InlineKeyboardButton(
                    "📋 复制账号号码", callback_data="payment_copy_gcash")],
                [InlineKeyboardButton(
                    "🔙 返回", callback_data="payment_back_gcash")]
            ]

            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            await query.answer("✅ 账号信息已显示，可以复制发送给客户")
        except Exception as e:
            logger.error(f"处理payment_send_gcash出错: {e}", exc_info=True)
            await query.answer(f"❌ 错误: {e}", show_alert=True)

    elif data == "payment_send_paymaya":
        try:
            account = await db_operations.get_payment_account('paymaya')
            if not account or not account.get('account_number'):
                await query.answer("❌ PayMaya账号未设置", show_alert=True)
                return

            account_number = account.get('account_number', '')
            account_name = account.get('account_name', '')
            balance = account.get('balance', 0)

            # 格式化消息，方便发送给客户
            message = (
                f"💳 PayMaya Payment Account\n\n"
                f"Account Number: `{account_number}`\n"
                f"Account Name: {account_name}\n"
                f"Current Balance: {balance:,.2f}\n\n"
                f"请将上述账号信息发送给客户。"
            )

            keyboard = [
                [InlineKeyboardButton(
                    "📋 复制账号号码", callback_data="payment_copy_paymaya")],
                [InlineKeyboardButton(
                    "🔙 返回", callback_data="payment_back_paymaya")]
            ]

            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            await query.answer("✅ 账号信息已显示，可以复制发送给客户")
        except Exception as e:
            logger.error(f"处理payment_send_paymaya出错: {e}", exc_info=True)
            await query.answer(f"❌ 错误: {e}", show_alert=True)

    elif data == "payment_update_balance_gcash":
        await query.message.reply_text(
            "请输入新的GCASH余额：\n"
            "格式: 数字（如：5000 或 5000.50）\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'UPDATING_BALANCE_GCASH'
        await query.answer()

    elif data == "payment_update_balance_paymaya":
        await query.message.reply_text(
            "请输入新的PayMaya余额：\n"
            "格式: 数字（如：5000 或 5000.50）\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'UPDATING_BALANCE_PAYMAYA'
        await query.answer()

    elif data == "payment_edit_gcash":
        await query.message.reply_text(
            "请输入GCASH账号信息：\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 张三\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'EDITING_ACCOUNT_GCASH'
        await query.answer()

    elif data == "payment_edit_paymaya":
        await query.message.reply_text(
            "请输入PayMaya账号信息：\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 李四\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'EDITING_ACCOUNT_PAYMAYA'
        await query.answer()

    elif data == "payment_back_gcash":
        from handlers.payment_handlers import show_gcash
        await show_gcash(update, context)

    elif data == "payment_back_paymaya":
        from handlers.payment_handlers import show_paymaya
        await show_paymaya(update, context)

    elif data == "payment_copy_gcash":
        account = await db_operations.get_payment_account('gcash')
        if account:
            account_number = account.get('account_number', '')
            await query.answer(f"账号号码: {account_number}", show_alert=True)
        else:
            await query.answer("❌ 账号未设置", show_alert=True)

    elif data == "payment_copy_paymaya":
        account = await db_operations.get_payment_account('paymaya')
        if account:
            account_number = account.get('account_number', '')
            await query.answer(f"账号号码: {account_number}", show_alert=True)
        else:
            await query.answer("❌ 账号未设置", show_alert=True)

    elif data == "payment_view_gcash":
        from handlers.payment_handlers import show_gcash
        await show_gcash(update, context)

    elif data == "payment_view_paymaya":
        from handlers.payment_handlers import show_paymaya
        await show_paymaya(update, context)

    elif data == "payment_refresh_table":
        from handlers.payment_handlers import show_all_accounts
        await show_all_accounts(update, context)

    elif data == "payment_add_account":
        # 选择要添加的账户类型
        keyboard = [
            [
                InlineKeyboardButton(
                    "💳 添加GCASH账户", callback_data="payment_add_gcash"),
                InlineKeyboardButton(
                    "💳 添加PayMaya账户", callback_data="payment_add_paymaya")
            ],
            [
                InlineKeyboardButton(
                    "🔙 返回", callback_data="payment_refresh_table")
            ]
        ]
        await query.edit_message_text(
            "💳 选择要添加的账户类型：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        await query.answer()

    elif data == "payment_add_gcash":
        await query.message.reply_text(
            "请输入新的GCASH账户信息：\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 张三\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'ADDING_ACCOUNT_GCASH'
        await query.answer()

    elif data == "payment_add_paymaya":
        await query.message.reply_text(
            "请输入新的PayMaya账户信息：\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 李四\n"
            "输入 'cancel' 取消"
        )
        context.user_data['state'] = 'ADDING_ACCOUNT_PAYMAYA'
        await query.answer()

    elif data.startswith("payment_edit_account_"):
        # 编辑指定ID的账户
        try:
            account_id = int(data.split("_")[-1])
            account = await db_operations.get_payment_account_by_id(account_id)
            if not account:
                await query.answer("❌ 账户不存在", show_alert=True)
                return

            context.user_data['editing_account_id'] = account_id
            account_type = account.get('account_type', '')

            await query.message.reply_text(
                f"请输入账户信息：\n"
                f"格式: <账号号码> <账户名称>\n"
                f"示例: 09171234567 张三\n"
                f"输入 'cancel' 取消\n\n"
                f"💡 提示：输入 'delete' 可以删除此账户"
            )

            if account_type == 'gcash':
                context.user_data['state'] = 'EDITING_ACCOUNT_BY_ID_GCASH'
            else:
                context.user_data['state'] = 'EDITING_ACCOUNT_BY_ID_PAYMAYA'

            await query.answer()
        except (ValueError, IndexError):
            await query.answer("❌ 无效的账户ID", show_alert=True)
