#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 数据库连接模块

import pymysql
from elasticsearch import Elasticsearch
from loguru import logger
from config import load_config

class DatabaseConnector:
    """数据库连接器，包含MySQL和ElasticSearch的连接方法"""
    
    def __init__(self):
        """初始化数据库连接器"""
        self.config = load_config()
        if not self.config:
            logger.error("配置加载失败，请检查配置文件")
            raise ValueError("配置加载失败")
        
        # MySQL连接配置
        self.mysql_config = {
            'host': self.config.get('mysql', 'host'),
            'port': int(self.config.get('mysql', 'port')),
            'user': self.config.get('mysql', 'user'),
            'password': self.config.get('mysql', 'password'),
            'database': self.config.get('mysql', 'database'),
            'charset': self.config.get('mysql', 'charset'),
            'cursorclass': pymysql.cursors.DictCursor
        }
        
        # ElasticSearch连接配置
        self.es_config = {
            'hosts': [f"http://{self.config.get('elasticsearch', 'host')}:{self.config.get('elasticsearch', 'port')}"],
            'http_auth': None
        }
        
        # 如果ES配置了用户名和密码，则添加认证
        es_user = self.config.get('elasticsearch', 'user')
        es_password = self.config.get('elasticsearch', 'password')
        if es_user and es_password:
            self.es_config['http_auth'] = (es_user, es_password)
        
        self.index_name = self.config.get('elasticsearch', 'index_name')
        
        # 初始化连接对象
        self.mysql_conn = None
        self.es_client = None
    
    def connect_mysql(self):
        """连接MySQL数据库"""
        try:
            self.mysql_conn = pymysql.connect(**self.mysql_config)
            logger.info("MySQL连接成功")
            return self.mysql_conn
        except Exception as e:
            logger.error(f"MySQL连接失败: {str(e)}")
            raise
    
    def connect_elasticsearch(self):
        """连接ElasticSearch"""
        try:
            self.es_client = Elasticsearch(**self.es_config, timeout=30)
            if not self.es_client.ping():
                raise ConnectionError("无法连接到ElasticSearch")
            logger.info("ElasticSearch连接成功")
            return self.es_client
        except Exception as e:
            logger.error(f"ElasticSearch连接失败: {str(e)}")
            raise
    
    def close_connections(self):
        """关闭所有数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.debug("MySQL连接已关闭")
        
        if self.es_client:
            self.es_client.close()
            logger.debug("ElasticSearch连接已关闭")
    
    def __enter__(self):
        """支持with语句的上下文管理器"""
        self.connect_mysql()
        self.connect_elasticsearch()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出时自动关闭连接"""
        self.close_connections()
