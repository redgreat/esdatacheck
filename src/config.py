#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 配置文件，包含数据库连接和企业微信配置

import os
import configparser
from loguru import logger

# 配置文件路径
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.ini')

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, 'data_check.log')

logger.add(
    log_file,
    rotation="1 days",
    retention="7 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    level="INFO",
)

def load_config():
    """加载配置文件"""
    if not os.path.exists(CONFIG_PATH):
        # 如果配置目录不存在，则创建
        config_dir = os.path.dirname(CONFIG_PATH)
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
        
        # 创建默认配置文件
        config = configparser.ConfigParser()
        
        # MySQL配置
        config['mysql'] = {
            'host': 'localhost',
            'port': '3306',
            'user': 'root',
            'password': 'password',
            'database': 'your_database',
            'charset': 'utf8mb4'
        }
        
        # ElasticSearch配置
        config['elasticsearch'] = {
            'host': 'localhost',
            'port': '9200',
            'user': '',
            'password': '',
            'index_name': 'your_index'
        }
        
        # 企业微信配置
        config['wechat'] = {
            'corp_id': 'your_corp_id',
            'corp_secret': 'your_corp_secret',
            'agent_id': 'your_agent_id',
            'to_user': '@all'  # 默认发送给所有人
        }
        
        # 数据检查配置
        config['check'] = {
            'sample_size': '10',  # 随机抽样数量
            'check_interval': '3600',  # 检查间隔（秒）
        }
        
        # 写入配置文件
        with open(CONFIG_PATH, 'w') as f:
            config.write(f)
        
        logger.info(f"已创建默认配置文件: {CONFIG_PATH}，请修改为实际配置后重新运行")
        return None
    
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config
