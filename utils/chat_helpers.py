"""聊天相关工具函数"""
from telegram import Update
from constants import WEEKDAY_GROUP
from datetime import date


def is_group_chat(update: Update) -> bool:
    """判断是否是群组聊天"""
    return update.effective_chat.type in ['group', 'supergroup']


def get_current_group():
    """获取当前星期对应的分组"""
    today = date.today().weekday()
    return WEEKDAY_GROUP[today]


def reply_in_group(update: Update, message: str):
    """在群组中回复消息"""
    if is_group_chat(update):
        return update.message.reply_text(message)
    else:
        # 私聊保持中文
        return update.message.reply_text(message)
