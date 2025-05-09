#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 主程序入口

import sys
import time
import argparse
from loguru import logger
from data_checker import DataChecker
from config import load_config

def run_check():
    """执行一次数据一致性检查"""
    try:
        # 初始化数据检查器
        checker = DataChecker()
        
        # 执行检查
        result = checker.check_consistency()
        
        # 返回检查结果
        return result
    except Exception as e:
        logger.error(f"数据一致性检查过程中发生错误: {str(e)}")
        return False

def run_service():
    """作为服务运行，定期执行检查"""
    config = load_config()
    if not config:
        logger.error("配置加载失败，无法启动服务")
        return
    
    # 获取检查间隔时间(秒)
    check_interval = int(config.get('check', 'check_interval'))
    
    logger.info(f"数据一致性检查服务已启动，检查间隔: {check_interval}秒")
    
    try:
        while True:
            # 执行检查
            run_check()
            
            # 等待下一次检查
            logger.info(f"等待 {check_interval} 秒后进行下一次检查...")
            time.sleep(check_interval)
    except KeyboardInterrupt:
        logger.info("服务已手动停止")
    except Exception as e:
        logger.error(f"服务运行过程中发生错误: {str(e)}")

def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="MySQL和ElasticSearch数据一致性检查工具")
    parser.add_argument("--service", action="store_true", help="作为服务运行，定期执行检查")
    parser.add_argument("--sample", type=int, help="指定抽样数量，覆盖配置文件")
    
    args = parser.parse_args()
    
    # 如果指定了抽样数量，更新配置
    if args.sample:
        config = load_config()
        if config:
            config.set('check', 'sample_size', str(args.sample))
            # 保存更新后的配置
            with open(config.path, 'w') as f:
                config.write(f)
            logger.info(f"已更新抽样数量为: {args.sample}")
    
    # 决定运行模式
    if args.service:
        run_service()
    else:
        run_check()

if __name__ == "__main__":
    main()
