#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 企业微信通知模块 - 群机器人webhook方式

import requests
import json
from loguru import logger
from config import load_config

class WechatNotifier:
    """企业微信通知类 - 使用群机器人webhook"""
    
    def __init__(self):
        """初始化企业微信通知"""
        self.config = load_config()
        if not self.config:
            logger.error("配置加载失败，请检查配置文件")
            raise ValueError("配置加载失败")
        
        # 获取企业微信配置
        self.to_group_key = self.config.get("wechat", "to_group_key")
        self.to_user = self.config.get("wechat", "to_user")
        self.to_user = self.to_user.split(',') if self.to_user else []
        self.to_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.to_group_key}"
    
    def send_message(self, title, content):
        """发送企业微信消息
        
        Args:
            title: 消息标题
            content: 消息内容
        
        Returns:
            bool: 是否发送成功
        """
        # 格式化消息
        message = f"## {title}\n\n{content}"
        return self.send_wechat_alert(message)
    
    def send_wechat_alert(self, message):
        """发送企业微信告警"""
        try:
            response = requests.post(
                self.to_url, 
                json={"msgtype": "markdown",
                      "markdown": {
                          "content": message,
                          "mentioned_mobile_list": self.to_user
                      }
                },
                headers = {'Content-Type':'application/json'}
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    logger.info("企业微信消息发送成功")
                    return True
                else:
                    logger.error(f"企业微信消息发送失败: {result.get('errmsg')}")
            return False
        except Exception as e:
            logger.error(f"发送企业微信消息时发生错误: {str(e)}")
            return False
