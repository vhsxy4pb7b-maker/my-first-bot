"""归属变更处理器"""
import logging
from telegram import Update
from telegram.ext import ContextTypes
import db_operations
from utils.stats_helpers import update_all_stats

logger = logging.getLogger(__name__)


async def change_orders_attribution(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    orders: list,
    new_group_id: str
) -> tuple[int, int]:
    """
    批量修改订单归属
    
    Args:
        update: Telegram Update对象
        context: Context对象
        orders: 订单列表
        new_group_id: 新的归属ID
    
    Returns:
        (success_count, fail_count): 成功和失败的数量
    """
    success_count = 0
    fail_count = 0
    
    # 按旧归属ID分组，统计需要迁移的数据（只统计成功更新的订单）
    old_group_stats = {}  # {old_group_id: {'valid': {'count': 0, 'amount': 0}, 'breach': {...}}}
    
    for order in orders:
        chat_id = order['chat_id']
        old_group_id = order['group_id']
        amount = order.get('amount', 0)
        state = order.get('state', 'normal')
        
        # 跳过已完成和违约完成的订单（这些订单的统计数据已经固定，不需要迁移）
        if state in ['end', 'breach_end']:
            # 仍然更新订单的归属ID（虽然不迁移统计数据）
            try:
                if await db_operations.update_order_group_id(chat_id, new_group_id):
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                logger.error(f"更新订单归属出错: {e}", exc_info=True)
                fail_count += 1
            continue
        
        # 先更新订单的归属ID
        update_success = False
        try:
            if await db_operations.update_order_group_id(chat_id, new_group_id):
                update_success = True
                success_count += 1
            else:
                fail_count += 1
                logger.warning(f"更新订单归属失败: chat_id={chat_id}, new_group_id={new_group_id}")
        except Exception as e:
            logger.error(f"更新订单归属出错: {e}", exc_info=True)
            fail_count += 1
        
        # 只有成功更新的订单才统计迁移数据
        if not update_success:
            continue
        
        # 初始化旧归属统计
        if old_group_id not in old_group_stats:
            old_group_stats[old_group_id] = {
                'valid': {'count': 0, 'amount': 0},
                'breach': {'count': 0, 'amount': 0}
            }
        
        # 根据订单状态分类统计
        if state in ['normal', 'overdue']:
            old_group_stats[old_group_id]['valid']['count'] += 1
            old_group_stats[old_group_id]['valid']['amount'] += amount
        elif state == 'breach':
            old_group_stats[old_group_id]['breach']['count'] += 1
            old_group_stats[old_group_id]['breach']['amount'] += amount
    
    # 迁移统计数据
    # 从旧归属减少
    for old_group_id, stats in old_group_stats.items():
        # 减少有效订单
        if stats['valid']['count'] > 0:
            await update_all_stats(
                'valid',
                -stats['valid']['amount'],
                -stats['valid']['count'],
                old_group_id
            )
        
        # 减少违约订单
        if stats['breach']['count'] > 0:
            await update_all_stats(
                'breach',
                -stats['breach']['amount'],
                -stats['breach']['count'],
                old_group_id
            )
    
    # 汇总所有需要迁移的数据
    total_valid_count = sum(s['valid']['count'] for s in old_group_stats.values())
    total_valid_amount = sum(s['valid']['amount'] for s in old_group_stats.values())
    total_breach_count = sum(s['breach']['count'] for s in old_group_stats.values())
    total_breach_amount = sum(s['breach']['amount'] for s in old_group_stats.values())
    
    # 到新归属增加
    if total_valid_count > 0:
        await update_all_stats(
            'valid',
            total_valid_amount,
            total_valid_count,
            new_group_id
        )
    
    if total_breach_count > 0:
        await update_all_stats(
            'breach',
            total_breach_amount,
            total_breach_count,
            new_group_id
        )
    
    logger.info(
        f"归属变更完成: {success_count} 成功, {fail_count} 失败, "
        f"迁移到 {new_group_id}: 有效订单 {total_valid_count} 个 ({total_valid_amount:.2f}), "
        f"违约订单 {total_breach_count} 个 ({total_breach_amount:.2f})"
    )
    
    return success_count, fail_count

