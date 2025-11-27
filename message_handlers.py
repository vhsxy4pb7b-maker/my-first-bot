"""消息处理器（群组事件、文本输入等）"""
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.chat_helpers import is_group_chat
from utils.order_helpers import try_create_order_from_title, update_order_state_from_title
from utils.date_helpers import get_daily_period_date
from utils.message_helpers import display_search_results_helper
from utils.stats_helpers import update_all_stats, update_liquid_capital
from constants import USER_STATES

logger = logging.getLogger(__name__)


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


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理文本输入（用于搜索和群发）"""
    user_state = context.user_data.get('state')

    # 1. 检查是否是快捷操作（+开头），如果是，交给 handle_amount_operation 处理
    if update.message.text.startswith('+'):
        return

    # 2. 检查状态是否需要处理群组消息
    allow_group = False
    if user_state in ['WAITING_BREACH_END_AMOUNT', 'BROADCAST_PAYMENT']:
        allow_group = True

    # 3. 检查聊天类型
    if update.effective_chat.type != 'private' and not allow_group:
        return

    # 如果没有状态，忽略
    if not user_state:
        return

    text = update.message.text.strip()

    # 通用取消逻辑
    if text.lower() == 'cancel':
        context.user_data['state'] = None
        msg = "✅ Operation Cancelled"
        await update.message.reply_text(msg)
        return

    if user_state == 'WAITING_BREACH_END_AMOUNT':
        await _handle_breach_end_amount(update, context, text)
        return

    if user_state == 'BROADCAST_PAYMENT':
        from handlers.broadcast_handlers import handle_broadcast_payment_input
        await handle_broadcast_payment_input(update, context, text)
        return

    # 以下状态仅限私聊
    if update.effective_chat.type != 'private':
        return

    if user_state in ['QUERY_EXPENSE_COMPANY', 'QUERY_EXPENSE_OTHER']:
        await _handle_expense_query(update, context, text, user_state)
        return

    if user_state in ['WAITING_EXPENSE_COMPANY', 'WAITING_EXPENSE_OTHER']:
        await _handle_expense_input(update, context, text, user_state)
        return

    if user_state == 'SEARCHING':
        await _handle_search_input(update, context, text)
        return

    if user_state == 'REPORT_QUERY':
        await _handle_report_query(update, context, text)
        return

    if user_state == 'REPORT_SEARCHING':
        await _handle_report_search(update, context, text)
        return

    if user_state == 'BROADCASTING':
        await _handle_broadcast(update, context, text)
        return

    if user_state == 'UPDATING_BALANCE_GCASH':
        await _handle_update_balance(update, context, text, 'gcash')
        return

    if user_state == 'UPDATING_BALANCE_PAYMAYA':
        await _handle_update_balance(update, context, text, 'paymaya')
        return

    if user_state == 'EDITING_ACCOUNT_GCASH':
        await _handle_edit_account(update, context, text, 'gcash')
        return

    if user_state == 'EDITING_ACCOUNT_PAYMAYA':
        await _handle_edit_account(update, context, text, 'paymaya')
        return

    if user_state == 'ADDING_ACCOUNT_GCASH':
        await _handle_add_account(update, context, text, 'gcash')
        return

    if user_state == 'ADDING_ACCOUNT_PAYMAYA':
        await _handle_add_account(update, context, text, 'paymaya')
        return

    if user_state == 'EDITING_ACCOUNT_BY_ID_GCASH':
        await _handle_edit_account_by_id(update, context, text, 'gcash')
        return

    if user_state == 'EDITING_ACCOUNT_BY_ID_PAYMAYA':
        await _handle_edit_account_by_id(update, context, text, 'paymaya')
        return

    # 处理定时播报输入
    if user_state and user_state.startswith('SCHEDULE_'):
        from handlers.schedule_handlers import handle_schedule_input
        handled = await handle_schedule_input(update, context)
        if handled:
            return


async def _handle_breach_end_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理违约完成金额输入"""
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

        # 更新流动资金
        await update_liquid_capital(amount)

        msg_en = f"✅ Breach Order Ended\nAmount: {amount:.2f}"

        # 如果当前聊天不是订单所在的聊天，通知群组
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


async def _handle_expense_query(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user_state: str):
    """处理开销查询"""
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
            display_records = records[-20:] if len(records) > 20 else records
            real_total = sum(r['amount'] for r in records)

            for r in display_records:
                msg += f"[{r['date']}] {r['amount']:.2f} - {r['note'] or 'No Note'}\n"

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


async def _handle_expense_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user_state: str):
    """处理开销输入"""
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


async def _handle_search_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理搜索输入"""
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
                        if value.startswith('周') and len(value) == 2:
                            value = value[1]

                    if key in ['group_id', 'state', 'customer', 'order_id', 'weekday_group']:
                        criteria[key] = value
        else:
            # 智能识别
            val = text.strip()
            # 1. 星期分组
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
            # 4. 归属ID
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


async def _handle_report_search(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理报表查找输入"""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    # 解析搜索条件
    criteria = {}
    try:
        # 支持空格分隔的多个条件
        parts = text.strip().split()

        for part in parts:
            part = part.strip()
            # 1. 星期分组（一、二、三、四、五、六、日）
            if part in ['一', '二', '三', '四', '五', '六', '日']:
                criteria['weekday_group'] = part
            elif part.startswith('周') and len(part) == 2 and part[1] in ['一', '二', '三', '四', '五', '六', '日']:
                criteria['weekday_group'] = part[1]
            # 2. 状态（正常、逾期、违约、完成、违约完成）
            elif part in ['正常', '逾期', '违约', '完成', '违约完成', 'normal', 'overdue', 'breach', 'end', 'breach_end']:
                state_map = {
                    '正常': 'normal', '逾期': 'overdue', '违约': 'breach',
                    '完成': 'end', '违约完成': 'breach_end'
                }
                criteria['state'] = state_map.get(part, part)
            # 3. 归属ID（S01格式）
            elif len(part) == 3 and part[0].isalpha() and part[1:].isdigit():
                criteria['group_id'] = part.upper()
            # 4. 客户类型
            elif part.upper() in ['A', 'B']:
                criteria['customer'] = part.upper()

        if not criteria:
            await update.message.reply_text("❌ 无法识别查询条件\n\n示例：\n• S01\n• 三 正常\n• S01 正常")
            return

        # 执行查找（报表查找包含所有状态的订单）
        orders = await db_operations.search_orders_advanced_all_states(criteria)

        if not orders:
            await update.message.reply_text("❌ 未找到匹配的订单")
            context.user_data['state'] = None
            return

        # 计算订单数量和金额
        order_count = len(orders)
        total_amount = sum(order.get('amount', 0) for order in orders)

        # 锁定群组
        locked_groups = list(set(order['chat_id'] for order in orders))
        context.user_data['locked_groups'] = locked_groups

        # 显示结果
        result_msg = (
            f"📊 查找结果\n\n"
            f"订单数量: {order_count}\n"
            f"订单金额: {total_amount:,.2f}\n"
            f"群组数量: {len(locked_groups)}"
        )

        # 保存查找结果到context，用于后续修改归属
        context.user_data['report_search_orders'] = orders

        # 添加群发和修改归属按钮
        keyboard = [
            [
                InlineKeyboardButton(
                    "📢 群发消息", callback_data="broadcast_start"),
                InlineKeyboardButton(
                    "🔄 修改归属", callback_data="report_change_attribution")
            ],
            [InlineKeyboardButton(
                "🔙 返回", callback_data="report_menu_attribution")]
        ]

        await update.message.reply_text(
            result_msg,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

        # 退出输入状态，但保留 locked_groups 和查找结果
        context.user_data['state'] = None

    except Exception as e:
        logger.error(f"报表查找出错: {e}", exc_info=True)
        await update.message.reply_text(f"⚠️ 查找出错: {e}")
        context.user_data['state'] = None


async def _handle_update_balance(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str):
    """处理更新余额输入"""
    try:
        new_balance = float(text)
        success = await db_operations.update_payment_account(account_type, balance=new_balance)

        if success:
            account_name = 'GCASH' if account_type == 'gcash' else 'PayMaya'
            await update.message.reply_text(
                f"✅ {account_name}余额已更新为: {new_balance:,.2f}"
            )
            # 重新显示账号信息
            if account_type == 'gcash':
                from handlers.payment_handlers import show_gcash
                await show_gcash(update, context)
            else:
                from handlers.payment_handlers import show_paymaya
                await show_paymaya(update, context)
        else:
            await update.message.reply_text("❌ 更新失败")

        context.user_data['state'] = None
    except ValueError:
        await update.message.reply_text("❌ 请输入有效的数字")


async def _handle_add_account(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str):
    """处理添加账户输入"""
    parts = text.strip().split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 格式错误\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 张三"
        )
        return

    account_number = parts[0]
    account_name = parts[1]

    account_id = await db_operations.create_payment_account(
        account_type,
        account_number,
        account_name
    )

    if account_id:
        account_name_display = 'GCASH' if account_type == 'gcash' else 'PayMaya'
        await update.message.reply_text(
            f"✅ {account_name_display}账户已添加\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
        # 重新显示账户列表
        if account_type == 'gcash':
            from handlers.payment_handlers import show_gcash
            await show_gcash(update, context)
        else:
            from handlers.payment_handlers import show_paymaya
            await show_paymaya(update, context)
    else:
        await update.message.reply_text("❌ 添加失败")

    context.user_data['state'] = None


async def _handle_edit_account(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str):
    """处理编辑账号输入（兼容旧代码）"""
    parts = text.strip().split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 格式错误\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 张三"
        )
        return

    account_number = parts[0]
    account_name = parts[1]

    success = await db_operations.update_payment_account(
        account_type,
        account_number=account_number,
        account_name=account_name
    )

    if success:
        account_name_display = 'GCASH' if account_type == 'gcash' else 'PayMaya'
        await update.message.reply_text(
            f"✅ {account_name_display}账号信息已更新\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
        # 重新显示账号信息
        if account_type == 'gcash':
            from handlers.payment_handlers import show_gcash
            await show_gcash(update, context)
        else:
            from handlers.payment_handlers import show_paymaya
            await show_paymaya(update, context)
    else:
        await update.message.reply_text("❌ 更新失败")

    context.user_data['state'] = None


async def _handle_edit_account_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, account_type: str):
    """处理编辑账户输入（按ID）"""
    account_id = context.user_data.get('editing_account_id')
    if not account_id:
        await update.message.reply_text("❌ 错误：找不到账户ID")
        context.user_data['state'] = None
        return

    # 检查是否要删除账户
    if text.strip().lower() == 'delete':
        success = await db_operations.delete_payment_account(account_id)
        if success:
            account_name_display = 'GCASH' if account_type == 'gcash' else 'PayMaya'
            await update.message.reply_text(f"✅ {account_name_display}账户已删除")
            # 重新显示账户列表
            if account_type == 'gcash':
                from handlers.payment_handlers import show_gcash
                await show_gcash(update, context)
            else:
                from handlers.payment_handlers import show_paymaya
                await show_paymaya(update, context)
        else:
            await update.message.reply_text("❌ 删除失败")
        context.user_data['state'] = None
        context.user_data.pop('editing_account_id', None)
        return

    parts = text.strip().split(maxsplit=1)

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ 格式错误\n"
            "格式: <账号号码> <账户名称>\n"
            "示例: 09171234567 张三\n\n"
            "💡 提示：输入 'delete' 可以删除此账户"
        )
        return

    account_number = parts[0]
    account_name = parts[1]

    success = await db_operations.update_payment_account_by_id(
        account_id,
        account_number=account_number,
        account_name=account_name
    )

    if success:
        account_name_display = 'GCASH' if account_type == 'gcash' else 'PayMaya'
        await update.message.reply_text(
            f"✅ {account_name_display}账户信息已更新\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
        # 重新显示账户列表
        if account_type == 'gcash':
            from handlers.payment_handlers import show_gcash
            await show_gcash(update, context)
        else:
            from handlers.payment_handlers import show_paymaya
            await show_paymaya(update, context)
    else:
        await update.message.reply_text("❌ 更新失败")

    context.user_data['state'] = None
    context.user_data.pop('editing_account_id', None)


async def _handle_report_query(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理报表查询"""
    from handlers.report_handlers import generate_report_text

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


async def _handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理群发消息"""
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
