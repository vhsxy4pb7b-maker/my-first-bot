"""Telegram订单管理机器人主入口"""
import logging
from telegram import error as telegram_error
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler
)
import init_db
from config import BOT_TOKEN, ADMIN_IDS
from handlers import (
    start,
    create_order,
    show_current_order,
    adjust_funds,
    create_attribution,
    list_attributions,
    add_employee,
    remove_employee,
    list_employees,
    set_normal,
    set_overdue,
    set_end,
    set_breach,
    set_breach_end,
    handle_amount_operation,
    show_report,
    search_orders,
    handle_new_chat_members,
    handle_new_chat_title,
    handle_text_input,
    broadcast_payment,
    show_gcash,
    show_paymaya,
    show_all_accounts,
    show_schedule_menu
)
from callbacks import button_callback, handle_order_action_callback, handle_schedule_callback
from utils.schedule_executor import setup_scheduled_broadcasts
from decorators import error_handler, admin_required, authorized_required, private_chat_only, group_chat_only

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def main() -> None:
    """启动机器人"""
    # 验证配置
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN 未设置，无法启动机器人")
        print("\n❌ 错误: BOT_TOKEN 未设置")
        print("请检查 config.py 文件或环境变量")
        return

    if not ADMIN_IDS:
        logger.error("ADMIN_USER_IDS 未设置，无法启动机器人")
        print("\n❌ 错误: ADMIN_USER_IDS 未设置")
        print("请检查 config.py 文件或环境变量")
        return

    logger.info(f"机器人启动中... 管理员数量: {len(ADMIN_IDS)}")
    try:
        print(f"\n机器人启动中...")
        print(f"管理员数量: {len(ADMIN_IDS)}")
    except UnicodeEncodeError:
        print("\nBot starting...")
        print(f"Admin count: {len(ADMIN_IDS)}")

    # 初始化数据库（如果不存在）
    try:
        print("检查数据库...")
    except UnicodeEncodeError:
        print("Checking database...")
    try:
        init_db.init_database()
        try:
            print("数据库已就绪")
        except UnicodeEncodeError:
            print("Database ready")
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        try:
            print(f"数据库初始化失败: {e}")
        except UnicodeEncodeError:
            print(f"Database init failed: {e}")
        return

    try:
        # 创建Application并传入bot的token
        application = Application.builder().token(BOT_TOKEN).build()
    except Exception as e:
        logger.error(f"创建应用时出错: {e}")
        print(f"\n❌ 创建应用时出错: {e}")
        return

    # 添加命令处理器
    # 基础命令（私聊，需要授权）
    application.add_handler(CommandHandler(
        "start", private_chat_only(authorized_required(error_handler(start)))))
    application.add_handler(CommandHandler(
        "report", private_chat_only(authorized_required(error_handler(show_report)))))
    application.add_handler(CommandHandler(
        "search", private_chat_only(authorized_required(error_handler(search_orders)))))
    application.add_handler(CommandHandler(
        "accounts", private_chat_only(authorized_required(error_handler(show_all_accounts)))))
    application.add_handler(CommandHandler(
        "gcash", private_chat_only(authorized_required(error_handler(show_gcash)))))
    application.add_handler(CommandHandler(
        "paymaya", private_chat_only(authorized_required(error_handler(show_paymaya)))))
    application.add_handler(CommandHandler(
        "schedule", private_chat_only(authorized_required(error_handler(show_schedule_menu)))))

    # 订单操作命令（群组，需要授权）
    application.add_handler(CommandHandler(
        "create", authorized_required(group_chat_only(create_order))))
    application.add_handler(CommandHandler(
        "normal", authorized_required(group_chat_only(set_normal))))
    application.add_handler(CommandHandler(
        "overdue", authorized_required(group_chat_only(set_overdue))))
    application.add_handler(CommandHandler(
        "end", authorized_required(group_chat_only(set_end))))
    application.add_handler(CommandHandler(
        "breach", authorized_required(group_chat_only(set_breach))))
    application.add_handler(CommandHandler(
        "breach_end", authorized_required(group_chat_only(set_breach_end))))
    application.add_handler(CommandHandler(
        "order", authorized_required(group_chat_only(show_current_order))))
    application.add_handler(CommandHandler(
        "broadcast", authorized_required(group_chat_only(broadcast_payment))))

    # 资金和归属ID管理（私聊，仅管理员）
    application.add_handler(CommandHandler(
        "adjust", private_chat_only(admin_required(adjust_funds))))
    application.add_handler(CommandHandler(
        "create_attribution", private_chat_only(admin_required(create_attribution))))
    application.add_handler(CommandHandler(
        "list_attributions", private_chat_only(admin_required(list_attributions))))

    # 员工管理（私聊，仅管理员）
    application.add_handler(CommandHandler(
        "add_employee", private_chat_only(admin_required(add_employee))))
    application.add_handler(CommandHandler(
        "remove_employee", private_chat_only(admin_required(remove_employee))))
    application.add_handler(CommandHandler(
        "list_employees", private_chat_only(admin_required(list_employees))))

    # 自动订单创建（新成员入群监听 & 群名变更监听）
    application.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_chat_members))
    application.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_TITLE, handle_new_chat_title))

    # 添加消息处理器（金额操作）- 需要管理员或员工权限
    # 只处理以 + 开头的消息（快捷操作）
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(
            r'^\+') & filters.ChatType.GROUPS,
        handle_amount_operation),
        group=1)  # 设置优先级组

    # 添加通用文本处理器（用于处理搜索和群发输入）
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\+'),
        handle_text_input),
        group=2)

    # 添加回调查询处理器
    application.add_handler(CallbackQueryHandler(
        authorized_required(handle_order_action_callback), pattern="^order_action_"))
    application.add_handler(CallbackQueryHandler(
        authorized_required(handle_schedule_callback), pattern="^schedule_"))
    application.add_handler(CallbackQueryHandler(
        authorized_required(button_callback)))

    # 启动机器人
    try:
        # 设置命令菜单
        commands = [
            ("create", "Create new order"),
            ("order", "Manage current order"),
            ("report", "View reports"),
            ("broadcast", "Broadcast payment reminder"),
            ("schedule", "Manage scheduled broadcasts"),
            ("accounts", "View all payment accounts"),
            ("gcash", "GCASH account info"),
            ("paymaya", "PayMaya account info"),
            ("start", "Start/Help")
        ]

        async def post_init(application: Application):
            await application.bot.set_my_commands(commands)
            try:
                print("命令菜单已更新")
            except UnicodeEncodeError:
                print("Commands menu updated")
            # 初始化定时播报任务
            await setup_scheduled_broadcasts(application.bot)
            try:
                print("定时播报任务已初始化")
            except UnicodeEncodeError:
                print("Scheduled broadcasts initialized")

        try:
            print("机器人已启动，等待消息...")
        except UnicodeEncodeError:
            print("Bot started, waiting for messages...")
        application.post_init = post_init
        # 启动机器人
        application.run_polling(drop_pending_updates=True)
    except telegram_error.InvalidToken:
        print("\n" + "="*60)
        print("❌ Token 无效或被拒绝！")
        print("="*60)
        print("\n可能的原因：")
        print("  1. Token 已过期或被撤销")
        print("  2. Token 格式不正确")
        print("  3. Token 不属于你的机器人")
        print("\n解决方法：")
        print("  1. 在 Telegram 中搜索 @BotFather")
        print("  2. 发送 /mybots 查看你的机器人列表")
        print("  3. 选择你的机器人，点击 'API Token'")
        print("  4. 复制新的 Token")
        print("  5. 更新 config.py 文件中的 BOT_TOKEN")
        print("\n当前使用的 Token（已隐藏部分）:")
        if BOT_TOKEN:
            masked_token = BOT_TOKEN[:10] + "..." + \
                BOT_TOKEN[-10:] if len(BOT_TOKEN) > 20 else "***"
            print(f"  {masked_token}")
        print("="*60)
        logger.error("Token 验证失败")
    except KeyboardInterrupt:
        print("\n\n👋 机器人已停止")
        logger.info("机器人被用户停止")
    except Exception as e:
        print(f"\n❌ 运行时发生错误: {e}")
        logger.error(f"运行时错误: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        # 不自动退出，让用户看到错误信息
        input("\n按Enter键退出...")


if __name__ == "__main__":
    main()
