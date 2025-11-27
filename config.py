"""配置管理模块"""
import os
import sys
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_config():
    """加载配置，优先从环境变量，其次从user_config.py文件"""
    # 先尝试从环境变量读取
    token = os.getenv("BOT_TOKEN")
    admin_ids_str = os.getenv("ADMIN_USER_IDS", "")

    # 如果环境变量没有，尝试从user_config.py读取（避免循环导入）
    if not token or not admin_ids_str:
        user_config_path = Path(__file__).parent / "user_config.py"
        if user_config_path.exists():
            try:
                # 使用importlib避免循环导入
                import importlib.util
                spec = importlib.util.spec_from_file_location("user_config", user_config_path)
                user_config = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(user_config)
                
                token = token or getattr(user_config, 'BOT_TOKEN', None)
                admin_ids_str = admin_ids_str or getattr(user_config, 'ADMIN_USER_IDS', '')
            except Exception as e:
                logger.debug(f"加载user_config.py失败: {e}")

    # 验证token
    if not token:
        raise ValueError(
            "BOT_TOKEN 未设置！\n"
            "请选择以下方式之一设置：\n"
            "1. 设置环境变量 BOT_TOKEN\n"
            "2. 创建 user_config.py 文件，添加：BOT_TOKEN = '你的token'\n"
            "   示例：BOT_TOKEN = '1234567890:ABCdefGHIjklMNOpqrsTUVwxyz'\n"
            "3. 参考 user_config.example.py 文件"
        )

    # 解析管理员ID
    admin_ids = [int(id.strip())
                 for id in admin_ids_str.split(",") if id.strip()]

    if not admin_ids:
        raise ValueError(
            "ADMIN_USER_IDS 未设置！\n"
            "请选择以下方式之一设置：\n"
            "1. 设置环境变量 ADMIN_USER_IDS（多个ID用逗号分隔）\n"
            "   示例：ADMIN_USER_IDS='123456789,987654321'\n"
            "2. 创建 user_config.py 文件，添加：ADMIN_USER_IDS = '你的用户ID1,你的用户ID2'\n"
            "   示例：ADMIN_USER_IDS = '123456789,987654321'\n"
            "3. 参考 user_config.example.py 文件"
        )

    return token, admin_ids


# 加载配置
BOT_TOKEN, ADMIN_IDS = load_config()
