"""支付账号管理处理器"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from decorators import authorized_required, private_chat_only, admin_required

logger = logging.getLogger(__name__)


@authorized_required
async def show_all_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示所有账户数据表格"""
    # 检查是否在私聊中
    if update.effective_chat.type != "private":
        if update.message:
            await update.message.reply_text("⚠️ 此命令只能在私聊中使用")
        elif update.callback_query:
            await update.callback_query.answer("⚠️ 此命令只能在私聊中使用", show_alert=True)
        return
    
    # 获取所有账户
    accounts = await db_operations.get_all_payment_accounts()
    
    if not accounts:
        msg = "❌ 没有账户数据"
        if update.message:
            await update.message.reply_text(msg)
        elif update.callback_query:
            await update.callback_query.edit_message_text(msg)
        return
    
    # 构建表格（使用等宽字体格式）
    table = "💳 账户数据表格\n\n"
    table += "┌──────────────┬──────────────────────┬───────────────┐\n"
    table += "│ 账户类型     │ 账号号码              │ 余额          │\n"
    table += "├──────────────┼──────────────────────┼───────────────┤\n"
    
    for account in accounts:
        account_type = account.get('account_type', '')
        account_number = account.get('account_number', '未设置')
        balance = account.get('balance', 0)
        
        # 格式化显示
        type_name = 'GCASH' if account_type == 'gcash' else 'PayMaya'
        type_display = type_name.ljust(14)
        
        # 账号号码显示（如果太长则截断）
        if len(account_number) > 20:
            number_display = account_number[:18] + '..'
        else:
            number_display = account_number.ljust(22)
        
        balance_display = f"{balance:,.2f}".rjust(13)
        
        table += f"│ {type_display} │ {number_display} │ {balance_display} │\n"
    
    table += "└──────────────┴──────────────────────┴───────────────┘\n\n"
    
    # 添加详细信息
    table += "📋 详细信息：\n\n"
    for account in accounts:
        account_type = account.get('account_type', '')
        account_number = account.get('account_number', '未设置')
        account_name = account.get('account_name', '未设置')
        balance = account.get('balance', 0)
        
        type_name = 'GCASH' if account_type == 'gcash' else 'PayMaya'
        table += f"💳 {type_name}\n"
        table += f"   账号号码: {account_number}\n"
        table += f"   账户名称: {account_name}\n"
        table += f"   当前余额: {balance:,.2f}\n\n"
    
    # 添加操作按钮
    keyboard = [
        [
            InlineKeyboardButton("💳 GCASH", callback_data="payment_view_gcash"),
            InlineKeyboardButton("💳 PayMaya", callback_data="payment_view_paymaya")
        ],
        [
            InlineKeyboardButton("➕ 添加账户", callback_data="payment_add_account")
        ],
        [
            InlineKeyboardButton("🔄 刷新", callback_data="payment_refresh_table")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text(table, reply_markup=reply_markup, parse_mode=None)
    elif update.callback_query:
        await update.callback_query.edit_message_text(table, reply_markup=reply_markup, parse_mode=None)


@authorized_required
async def show_gcash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示GCASH账户列表"""
    # 检查是否在私聊中
    if update.effective_chat.type != "private":
        if update.message:
            await update.message.reply_text("⚠️ 此命令只能在私聊中使用")
        elif update.callback_query:
            await update.callback_query.answer("⚠️ 此命令只能在私聊中使用", show_alert=True)
        return
    
    accounts = await db_operations.get_payment_accounts_by_type('gcash')
    
    if not accounts:
        msg = "❌ 没有GCASH账户\n\n点击下方按钮添加账户"
        keyboard = [
            [
                InlineKeyboardButton("➕ 添加GCASH账户", callback_data="payment_add_gcash")
            ],
            [
                InlineKeyboardButton("🔙 返回", callback_data="payment_refresh_table")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.message:
            await update.message.reply_text(msg, reply_markup=reply_markup)
        elif update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup)
        return
    
    msg = "💳 GCASH账户列表\n\n"
    keyboard = []
    
    for account in accounts:
        account_id = account.get('id')
        account_number = account.get('account_number', '未设置')
        account_name = account.get('account_name', '未设置')
        balance = account.get('balance', 0)
        
        display_name = account_name if account_name and account_name != '未设置' else account_number
        if len(display_name) > 20:
            display_name = display_name[:18] + '..'
        
        msg += f"💳 {display_name}\n"
        msg += f"   账号: {account_number}\n"
        msg += f"   余额: {balance:,.2f}\n\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"✏️ {display_name}",
                callback_data=f"payment_edit_account_{account_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton("➕ 添加GCASH账户", callback_data="payment_add_gcash")
    ])
    keyboard.append([
        InlineKeyboardButton("🔙 返回", callback_data="payment_refresh_table")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text(msg, reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup)


@authorized_required
async def show_paymaya(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示PayMaya账户列表"""
    # 检查是否在私聊中
    if update.effective_chat.type != "private":
        if update.message:
            await update.message.reply_text("⚠️ 此命令只能在私聊中使用")
        elif update.callback_query:
            await update.callback_query.answer("⚠️ 此命令只能在私聊中使用", show_alert=True)
        return
    
    accounts = await db_operations.get_payment_accounts_by_type('paymaya')
    
    if not accounts:
        msg = "❌ 没有PayMaya账户\n\n点击下方按钮添加账户"
        keyboard = [
            [
                InlineKeyboardButton("➕ 添加PayMaya账户", callback_data="payment_add_paymaya")
            ],
            [
                InlineKeyboardButton("🔙 返回", callback_data="payment_refresh_table")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.message:
            await update.message.reply_text(msg, reply_markup=reply_markup)
        elif update.callback_query:
            await update.callback_query.edit_message_text(msg, reply_markup=reply_markup)
        return
    
    msg = "💳 PayMaya账户列表\n\n"
    keyboard = []
    
    for account in accounts:
        account_id = account.get('id')
        account_number = account.get('account_number', '未设置')
        account_name = account.get('account_name', '未设置')
        balance = account.get('balance', 0)
        
        display_name = account_name if account_name and account_name != '未设置' else account_number
        if len(display_name) > 20:
            display_name = display_name[:18] + '..'
        
        msg += f"💳 {display_name}\n"
        msg += f"   账号: {account_number}\n"
        msg += f"   余额: {balance:,.2f}\n\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"✏️ {display_name}",
                callback_data=f"payment_edit_account_{account_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton("➕ 添加PayMaya账户", callback_data="payment_add_paymaya")
    ])
    keyboard.append([
        InlineKeyboardButton("🔙 返回", callback_data="payment_refresh_table")
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text(msg, reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.edit_message_text(msg, reply_markup=reply_markup)


@admin_required
@private_chat_only
async def update_payment_balance(update: Update, context: ContextTypes.DEFAULT_TYPE, account_type: str):
    """更新支付账号余额"""
    if not context.args:
        await update.message.reply_text(
            f"请输入新的余额金额\n"
            f"格式: /{'gcash' if account_type == 'gcash' else 'paymaya'}_balance <金额>\n"
            f"示例: /{'gcash' if account_type == 'gcash' else 'paymaya'}_balance 5000"
        )
        return
    
    try:
        new_balance = float(context.args[0])
        success = await db_operations.update_payment_account(account_type, balance=new_balance)
        
        if success:
            await update.message.reply_text(
                f"✅ {account_type.upper()}余额已更新为: {new_balance:,.2f}"
            )
        else:
            await update.message.reply_text("❌ 更新失败")
    except ValueError:
        await update.message.reply_text("❌ 请输入有效的数字")


@admin_required
@private_chat_only
async def edit_payment_account(update: Update, context: ContextTypes.DEFAULT_TYPE, account_type: str):
    """编辑支付账号信息"""
    if len(context.args) < 2:
        await update.message.reply_text(
            f"请输入账号信息\n"
            f"格式: /edit_{account_type} <账号号码> <账户名称>\n"
            f"示例: /edit_{account_type} 09171234567 张三"
        )
        return
    
    account_number = context.args[0]
    account_name = " ".join(context.args[1:])
    
    success = await db_operations.update_payment_account(
        account_type, 
        account_number=account_number,
        account_name=account_name
    )
    
    if success:
        await update.message.reply_text(
            f"✅ {account_type.upper()}账号信息已更新\n\n"
            f"账号号码: {account_number}\n"
            f"账户名称: {account_name}"
        )
    else:
        await update.message.reply_text("❌ 更新失败")

