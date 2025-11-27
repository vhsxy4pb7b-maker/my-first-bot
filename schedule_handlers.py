"""定时播报处理器"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import db_operations
from constants import USER_STATES

logger = logging.getLogger(__name__)


async def show_schedule_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示定时播报菜单"""
    broadcasts = await db_operations.get_all_scheduled_broadcasts()
    
    # 创建槽位字典
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
    
    await update.message.reply_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_schedule_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理定时播报的文本输入"""
    user_state = context.user_data.get('state', '')
    
    if not user_state.startswith('SCHEDULE_'):
        return False
    
    # 解析状态：SCHEDULE_TIME_1, SCHEDULE_CHAT_1, SCHEDULE_MESSAGE_1
    parts = user_state.split('_')
    if len(parts) < 3:
        return False
    
    field = parts[1]  # TIME, CHAT, MESSAGE
    slot = int(parts[2])  # 1, 2, 3
    
    text = update.message.text.strip()
    
    if field == 'TIME':
        # 验证时间格式 (HH:MM 或 HH)
        time_parts = text.split(':')
        if len(time_parts) == 1:
            # 只有小时，如 "22"
            try:
                hour = int(time_parts[0])
                if 0 <= hour <= 23:
                    time_str = f"{hour:02d}:00"
                else:
                    await update.message.reply_text("❌ 小时必须在0-23之间")
                    return True
            except ValueError:
                await update.message.reply_text("❌ 时间格式错误，请输入小时（0-23）或小时:分钟（如 22:30）")
                return True
        elif len(time_parts) == 2:
            # 小时:分钟，如 "22:30"
            try:
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    time_str = f"{hour:02d}:{minute:02d}"
                else:
                    await update.message.reply_text("❌ 时间格式错误，小时必须在0-23之间，分钟必须在0-59之间")
                    return True
            except ValueError:
                await update.message.reply_text("❌ 时间格式错误，请输入小时（0-23）或小时:分钟（如 22:30）")
                return True
        else:
            await update.message.reply_text("❌ 时间格式错误，请输入小时（0-23）或小时:分钟（如 22:30）")
            return True
        
        # 保存时间
        if 'schedule_data' not in context.user_data:
            context.user_data['schedule_data'] = {}
        if slot not in context.user_data['schedule_data']:
            context.user_data['schedule_data'][slot] = {}
        context.user_data['schedule_data'][slot]['time'] = time_str
        
        await update.message.reply_text(f"✅ 时间已设置为: {time_str}\n\n请选择或输入群组：")
        context.user_data['state'] = f'SCHEDULE_CHAT_{slot}'
        return True
    
    elif field == 'CHAT':
        # 尝试查找群组
        # 首先尝试通过群名查找（从订单中）
        # 这里简化处理：如果输入的是数字，当作chat_id；否则当作群名
        chat_id = None
        chat_title = None
        
        try:
            # 尝试作为chat_id解析
            chat_id = int(text)
            chat_title = f"群组ID: {chat_id}"
        except ValueError:
            # 作为群名处理
            chat_title = text
            # 尝试从订单中查找匹配的chat_id
            # 这里简化：直接使用输入的文本作为群名
        
        # 保存群组信息
        if 'schedule_data' not in context.user_data:
            context.user_data['schedule_data'] = {}
        if slot not in context.user_data['schedule_data']:
            context.user_data['schedule_data'][slot] = {}
        context.user_data['schedule_data'][slot]['chat_id'] = chat_id
        context.user_data['schedule_data'][slot]['chat_title'] = chat_title
        
        await update.message.reply_text(f"✅ 群组已设置为: {chat_title}\n\n请输入播报内容：")
        context.user_data['state'] = f'SCHEDULE_MESSAGE_{slot}'
        return True
    
    elif field == 'MESSAGE':
        # 保存消息内容
        if 'schedule_data' not in context.user_data:
            context.user_data['schedule_data'] = {}
        if slot not in context.user_data['schedule_data']:
            context.user_data['schedule_data'][slot] = {}
        context.user_data['schedule_data'][slot]['message'] = text
        
        # 检查是否所有字段都已填写
        slot_data = context.user_data['schedule_data'][slot]
        if 'time' in slot_data and 'message' in slot_data:
            # 保存到数据库
            time_str = slot_data['time']
            chat_id = slot_data.get('chat_id')
            chat_title = slot_data.get('chat_title')
            message = slot_data['message']
            
            await db_operations.create_or_update_scheduled_broadcast(
                slot, time_str, chat_id, chat_title, message, is_active=1
            )
            
            # 重新加载定时任务
            from utils.schedule_executor import reload_scheduled_broadcasts
            await reload_scheduled_broadcasts(context.bot)
            
            # 清除状态和数据
            context.user_data.pop('state', None)
            context.user_data['schedule_data'].pop(slot, None)
            
            await update.message.reply_text(
                f"✅ 定时播报 {slot} 已设置成功！\n\n"
                f"时间: {time_str}\n"
                f"群组: {chat_title}\n"
                f"内容: {message}\n\n"
                f"使用 /schedule 查看所有定时播报"
            )
        else:
            await update.message.reply_text("❌ 数据不完整，请重新设置")
        
        return True
    
    return False

