"""定时播报回调处理器"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from utils.schedule_executor import reload_scheduled_broadcasts

logger = logging.getLogger(__name__)


async def handle_schedule_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理定时播报回调"""
    query = update.callback_query
    
    # 必须先 answer，防止客户端转圈
    try:
        await query.answer()
    except Exception:
        pass
    
    data = query.data
    
    if data == "schedule_refresh":
        # 刷新菜单
        broadcasts = await db_operations.get_all_scheduled_broadcasts()
        
        slots = {1: None, 2: None, 3: None}
        for broadcast in broadcasts:
            slots[broadcast['slot']] = broadcast
        
        message = "⏰ 定时播报管理\n\n"
        for slot in [1, 2, 3]:
            broadcast = slots[slot]
            if broadcast and broadcast['is_active']:
                status = "✅ 激活"
                time_str = broadcast['time']
                chat_str = broadcast['chat_title'] or f"群组ID: {broadcast['chat_id']}" if broadcast['chat_id'] else "未设置"
                msg_preview = broadcast['message'][:20] + "..." if len(broadcast['message']) > 20 else broadcast['message']
            else:
                status = "❌ 未设置"
                time_str = "未设置"
                chat_str = "未设置"
                msg_preview = "未设置"
            
            message += f"📌 播报 {slot}:\n"
            message += f"   状态: {status}\n"
            message += f"   时间: {time_str}\n"
            message += f"   群组: {chat_str}\n"
            message += f"   内容: {msg_preview}\n\n"
        
        keyboard = []
        for slot in [1, 2, 3]:
            broadcast = slots[slot]
            if broadcast:
                button_text = f"编辑播报 {slot}" if broadcast['is_active'] else f"设置播报 {slot}"
            else:
                button_text = f"设置播报 {slot}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"schedule_setup_{slot}")])
        
        keyboard.append([InlineKeyboardButton("刷新", callback_data="schedule_refresh")])
        
        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("schedule_setup_"):
        # 设置播报
        slot = int(data.split("_")[-1])
        
        # 检查是否已有播报
        existing = await db_operations.get_scheduled_broadcast(slot)
        
        if existing:
            message = f"📝 编辑定时播报 {slot}\n\n"
            message += f"当前设置:\n"
            message += f"时间: {existing['time']}\n"
            message += f"群组: {existing['chat_title'] or f'群组ID: {existing[\"chat_id\"]}' if existing['chat_id'] else '未设置'}\n"
            message += f"内容: {existing['message']}\n\n"
            message += "请选择要编辑的项："
        else:
            message = f"📝 设置定时播报 {slot}\n\n"
            message += "请按顺序设置以下内容：\n"
            message += "1. 时间（每天的时间点）\n"
            message += "2. 群组（群名或群组ID）\n"
            message += "3. 内容（播报消息）\n\n"
            message += "首先，请输入时间："
        
        keyboard = [
            [
                InlineKeyboardButton("⏰ 设置时间", callback_data=f"schedule_time_{slot}"),
                InlineKeyboardButton("👥 设置群组", callback_data=f"schedule_chat_{slot}")
            ],
            [
                InlineKeyboardButton("📝 设置内容", callback_data=f"schedule_message_{slot}")
            ],
            [
                InlineKeyboardButton("❌ 删除播报", callback_data=f"schedule_delete_{slot}"),
                InlineKeyboardButton("🔙 返回", callback_data="schedule_refresh")
            ]
        ]
        
        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("schedule_time_"):
        slot = int(data.split("_")[-1])
        context.user_data['state'] = f'SCHEDULE_TIME_{slot}'
        await query.edit_message_text(
            f"⏰ 设置播报 {slot} 的时间\n\n"
            "请输入时间（24小时制）：\n"
            "格式：小时（如 22）或 小时:分钟（如 22:30）\n\n"
            "示例：\n"
            "- 22 （表示22:00）\n"
            "- 22:30 （表示22:30）\n\n"
            "输入 'cancel' 取消"
        )
    
    elif data.startswith("schedule_chat_"):
        slot = int(data.split("_")[-1])
        context.user_data['state'] = f'SCHEDULE_CHAT_{slot}'
        await query.edit_message_text(
            f"👥 设置播报 {slot} 的群组\n\n"
            "请输入群组名称或群组ID：\n\n"
            "示例：\n"
            "- 群组三\n"
            "- -1001234567890 （群组ID）\n\n"
            "输入 'cancel' 取消"
        )
    
    elif data.startswith("schedule_message_"):
        slot = int(data.split("_")[-1])
        context.user_data['state'] = f'SCHEDULE_MESSAGE_{slot}'
        await query.edit_message_text(
            f"📝 设置播报 {slot} 的内容\n\n"
            "请输入要播报的消息内容：\n\n"
            "示例：\n"
            "- 请大家准时换钱 有惊喜\n\n"
            "输入 'cancel' 取消"
        )
    
    elif data.startswith("schedule_delete_"):
        slot = int(data.split("_")[-1])
        await db_operations.delete_scheduled_broadcast(slot)
        # 重新加载定时任务
        await reload_scheduled_broadcasts(context.bot)
        await query.answer("✅ 播报已删除")
        await query.edit_message_text("✅ 定时播报已删除\n\n使用 /schedule 查看所有定时播报")
    
    elif data.startswith("schedule_toggle_"):
        slot = int(data.split("_")[-1])
        existing = await db_operations.get_scheduled_broadcast(slot)
        if existing:
            new_status = 0 if existing['is_active'] else 1
            await db_operations.toggle_scheduled_broadcast(slot, new_status)
            # 重新加载定时任务
            await reload_scheduled_broadcasts(context.bot)
            status_text = "激活" if new_status else "停用"
            await query.answer(f"✅ 播报已{status_text}")
            # 刷新菜单
            await handle_schedule_callback(update, context)

