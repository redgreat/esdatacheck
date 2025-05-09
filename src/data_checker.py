#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# 数据一致性检查模块

import random
import json
import pymysql
from loguru import logger
from datetime import datetime
from db_connect import DatabaseConnector
from wechat_notify import WechatNotifier
from config import load_config

class DataChecker:
    """MySQL与ElasticSearch数据一致性检查类"""
    
    def __init__(self):
        """初始化数据一致性检查"""
        self.config = load_config()
        if not self.config:
            logger.error("配置加载失败，请检查配置文件")
            raise ValueError("配置加载失败")
        
        self.sample_size = int(self.config.get('check', 'sample_size'))
        self.index_name = self.config.get('elasticsearch', 'index_name')
        self.db_connector = DatabaseConnector()
        self.wechat = WechatNotifier()
        
        # 获取ES索引名称
        self.main_index_name = self.index_name
        self.operating_index_name = "operating"
        self.custspecialconfig_index_name = "custspecialconfig"
        
        # 特殊表定义（对应单独索引）
        self.special_tables = {
            "tb_operatinginfo": {
                "es_index": self.operating_index_name,
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("AppCode", "AppCode"),
                    ("OperId", "OperId"),
                    ("OperCode", "OperCode"),
                    ("OperName", "OperName"),
                    ("TagType", "TagType"),
                    ("InsertTime", "InsertTime"),
                    ("Deleted", "Deleted")
                ]
            },
            "basic_custspecialconfig": {
                "es_index": self.custspecialconfig_index_name,
                "id_field": "CustomerId",
                "fields": [
                    ("Id", "Id"),
                    ("CustomerId", "CustomerId"),
                    ("CustomerName", "CustomerName"),
                    ("ConfigType", "ConfigType"),
                    ("ConfigKey", "ConfigKey"),
                    ("ConfigValue", "ConfigValue"),
                    ("Remark", "Remark"),
                    ("IsEnabled", "IsEnabled"),
                    ("CreatedById", "CreatedById"),
                    ("CreatedAt", "CreatedAt"),
                    ("UpdatedById", "UpdatedById"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("DeletedById", "DeletedById"),
                    ("DeletedAt", "DeletedAt"),
                    ("Deleted", "Deleted")
                ]
            }
        }
        
        # 表映射定义
        self.table_mappings = {
            # 主表映射
            "tb_workorderinfo": {
                "es_path": "",  # ES中主表字段在根级别
                "id_field": "Id",
                "fields": [
                    # 基本字段映射 MySQL字段名 -> ES字段名
                    ("Id", "Id"),
                    ("AppCode", "AppCode"),
                    ("SourceType", "SourceType"),
                    ("OrderType", "OrderType"),
                    ("CreateType", "CreateType"),
                    ("ServiceProviderCode", "ServiceProviderCode"),
                    ("WorkStatus", "WorkStatus"),
                    ("CustomerId", "CustomerId"),
                    ("CustomerName", "CustomerName"),
                    ("CustStoreId", "CustStoreId"),
                    ("CustStoreName", "CustStoreName"),
                    ("CustStoreCode", "CustStoreCode"),
                    ("PreCustStoreId", "PreCustStoreId"),
                    ("PreCustStoreName", "PreCustStoreName"),
                    ("CustSettleId", "CustSettleId"),
                    ("CustSettleName", "CustSettleName"),
                    ("IsCustomer", "IsCustomer"),
                    ("CustCoopType", "CustCoopType"),
                    ("ProCode", "ProCode"),
                    ("ProName", "ProName"),
                    ("CityCode", "CityCode"),
                    ("CityName", "CityName"),
                    ("AreaCode", "AreaCode"),
                    ("AreaName", "AreaName"),
                    ("InstallAddress", "InstallAddress"),
                    ("InstallTime", "InstallTime"),
                    ("RequiredTime", "RequiredTime"),
                    ("LinkMan", "LinkMan"),
                    ("LinkTel", "LinkTel"),
                    ("SecondLinkTel", "SecondLinkTel"),
                    ("SecondLinkMan", "SecondLinkMan"),
                    ("WarehouseId", "WarehouseId"),
                    ("WarehouseName", "WarehouseName"),
                    ("Remark", "Remark"),
                    ("CreatedAt", "CreatedAt"),
                    ("CreatedById", "CreatedById"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("UpdatedById", "UpdatedById"),
                    ("DeletedAt", "DeletedAt"),
                    ("DeletedById", "DeletedById"),
                    ("Deleted", "Deleted"),
                    ("CreatePersonCode", "CreatePersonCode"),
                    ("CreatePersonName", "CreatePersonName"),
                    ("CustUniqueSign", "CustUniqueSign"),
                    ("EffectiveTime", "EffectiveTime"),
                    ("EffectiveSuccessfulTime", "EffectiveSuccessfulTime"),
                    ("LastUpdateTimeStamp", "LastUpdateTimeStamp"),
                    ("IsUrgent", "IsUrgent"),
                ]
            },
            # 子表映射
            "tb_workcarinfo": {
                "es_path": "CarInfo",  # ES中的嵌套字段路径
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("PlateNumber", "PlateNumber"),
                    ("VinNumber", "VinNumber"),
                    ("EngineNumber", "EngineNumber"),
                    ("CarBrandId", "CarBrandId"),
                    ("CarBrandName", "CarBrandName"),
                    ("CarModelId", "CarModelId"),
                    ("CarModelName", "CarModelName"),
                    ("CarSeriesId", "CarSeriesId"),
                    ("CarSeriesName", "CarSeriesName"),
                    ("CarFullName", "CarFullName"),
                    ("CarType", "CarType"),
                    ("ShortVin", "ShortVin"),
                    ("ShortFourVin", "ShortFourVin"),
                    ("Color", "Color"),
                    ("PlateColor", "PlateColor"),
                    ("CarPrice", "CarPrice"),
                    ("UserName", "UserName"),
                    ("UserTel", "UserTel"),
                    ("UserCityCode", "UserCityCode"),
                    ("UserAddress", "UserAddress"),
                    ("Remark", "Remark"),
                    ("CreatedAt", "CreatedAt"),
                    ("CreatedById", "CreatedById"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("DeletedAt", "DeletedAt"),
                    ("DeletedById", "DeletedById"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_appointment": {
                "es_path": "AppointInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("AppCode", "AppCode"),
                    ("ApplyCode", "ApplyCode"),
                    ("AppointTime", "AppointTime"),
                    ("AppointStatus", "AppointStatus"),
                    ("AppointSource", "AppointSource"),
                    ("ProCode", "ProCode"),
                    ("ProName", "ProName"),
                    ("InstallAddress", "InstallAddress"),
                    ("AreaCode", "AreaCode"),
                    ("AreaName", "AreaName"),
                    ("CityCode", "CityCode"),
                    ("CityName", "CityName"),
                    ("OrderTime", "OrderTime"),
                    ("ApplyReason", "ApplyReason"),
                    ("OperatorCode", "OperatorCode"),
                    ("OperatorName", "OperatorName"),
                    ("FailCode", "FailCode"),
                    ("FailText", "FailText"),
                    ("NextContactTime", "NextContactTime"),
                    ("Remark", "Remark"),
                    ("ChangeRemark", "ChangeRemark"),
                    ("CreatedAt", "CreatedAt"),
                    ("CreatedById", "CreatedById"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_appointmentconcat": {
                "es_path": "ConcatInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("AppCode", "AppCode"),
                    ("ApplyCode", "ApplyCode"),
                    ("ApplyReason", "ApplyReason"),
                    ("AppointStatus", "AppointStatus"),
                    ("FirstSubmitTime", "FirstSubmitTime"),
                    ("FirstAppointTime", "FirstAppointTime"),
                    ("CorrectiveAppointTime", "CorrectiveAppointTime"),
                    ("RemarkConcat", "RemarkConcat"),
                    ("CallRemarkConcat", "CallRemarkConcat"),
                    ("LastRemark", "LastRemark"),
                    ("CreatedAt", "CreatedAt"),
                    ("CreatedById", "CreatedById"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("UpdatedById", "UpdatedById"),
                    ("DeletedAt", "DeletedAt"),
                    ("DeletedById", "DeletedById"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_workorderstatus": {
                "es_path": "StatusInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("NodeCode", "NodeCode"),
                    ("StepName", "StepName"),
                    ("StepStatus", "StepStatus"),
                    ("PreStepName", "PreStepName"),
                    ("PreStepStatus", "PreStepStatus"),
                    ("WorkStatus", "WorkStatus"),
                    ("WorkStatusCode", "WorkStatusCode"),
                    ("TypeStatus", "TypeStatus"),
                    ("SuspendStatus", "SuspendStatus"),
                    ("AuditStatus", "AuditStatus"),
                    ("IsMigration", "IsMigration"),
                    ("IsSwitch", "IsSwitch"),
                    ("IfUninstall", "IfUninstall"),
                    ("CloseReasonCode", "CloseReasonCode"),
                    ("CloseReasonName", "CloseReasonName"),
                    ("ClosePersonCode", "ClosePersonCode"),
                    ("ClosePersonName", "ClosePersonName"),
                    ("ClosedAt", "ClosedAt"),
                    ("Remark", "Remark"),
                    ("CreatedAt", "CreatedAt"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("DeletedAt", "DeletedAt"),
                    ("DeletedById", "DeletedById"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_workserviceinfo": {
                "es_path": "ServiceInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("ServiceId", "ServiceId"),
                    ("ServiceCode", "ServiceCode"),
                    ("ServiceName", "ServiceName"),
                    ("ServiceType", "ServiceType"),
                    ("Privoder", "Privoder"),
                    ("IsSelfService", "IsSelfService"),
                    ("IsPreInstall", "IsPreInstall"),
                    ("InstitutionCode", "InstitutionCode"),
                    ("WorkerId", "WorkerId"),
                    ("WorkerCode", "WorkerCode"),
                    ("WorkerName", "WorkerName"),
                    ("CarServiceRelation", "CarServiceRelation"),
                    ("CompleteTime", "CompleteTime"),
                    ("LastUpdateTimeStamp", "LastUpdateTimeStamp"),
                    ("Remark", "Remark"),
                    ("CreatedAt", "CreatedAt"),
                    ("CreatedById", "CreatedById"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("UpdatedById", "UpdatedById"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_worksignininfo": {
                "es_path": "SigninInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("SignType", "SignType"),
                    ("SignTime", "SignTime"),
                    ("SignLat", "SignLat"),
                    ("SignLng", "SignLng"),
                    ("InitialLat", "InitialLat"),
                    ("InitialLng", "InitialLng"),
                    ("SignAddr", "SignAddr"),
                    ("OriginalAddr", "OriginalAddr"),
                    ("SignAddrDistance", "SignAddrDistance"),
                    ("LastSignDistance", "LastSignDistance"),
                    ("IMEI", "IMEI"),
                    ("OrgCode", "OrgCode"),
                    ("Remark", "Remark"),
                    ("CreatedAt", "CreatedAt"),
                    ("CreatedById", "CreatedById"),
                    ("UpdatedAt", "UpdatedAt"),
                    ("UpdatedById", "UpdatedById"),
                    ("DeletedAt", "DeletedAt"),
                    ("DeletedById", "DeletedById"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_custcolumn": {
                "es_path": "ColumnInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("TypeCode", "TypeCode"),
                    ("TypeName", "TypeName"),
                    ("Value", "Value"),
                    ("InsertTime", "InsertTime"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_workbussinessjsoninfo": {
                "es_path": "JsonInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("BussinessJson", "BussinessJson"),
                    ("InsertTime", "InsertTime"),
                    ("Deleted", "Deleted"),
                ]
            },
            "tb_recordinfo": {
                "es_path": "RecordInfo",
                "id_field": "WorkOrderId",
                "fields": [
                    ("Id", "Id"),
                    ("WorkOrderId", "WorkOrderId"),
                    ("RecordPersonCode", "RecordPersonCode"),
                    ("RecordPersonName", "RecordPersonName"),
                    ("CompleteTime", "CompleteTime"),
                    ("InsertTime", "InsertTime"),
                    ("Deleted", "Deleted"),
                ]
            },
        }
    
    def get_random_orders(self):
        """从MySQL中随机获取订单ID进行抽查"""
        try:
            with self.db_connector as db:
                cursor = db.mysql_conn.cursor()
                
                # 获取最近3个月内有效的工单总数
                count_sql = """
                SELECT COUNT(Id) as total FROM tb_workorder 
                WHERE Deleted = 0 AND CreatedAt > DATE_SUB(NOW(), INTERVAL 3 MONTH)
                """
                cursor.execute(count_sql)
                total = cursor.fetchone()['total']
                
                if total == 0:
                    logger.warning("未找到符合条件的工单记录")
                    return []
                
                # 随机抽取工单ID
                sample_sql = """
                SELECT Id FROM tb_workorder 
                WHERE Deleted = 0 AND CreatedAt > DATE_SUB(NOW(), INTERVAL 3 MONTH)
                ORDER BY RAND() LIMIT %s
                """
                cursor.execute(sample_sql, (self.sample_size,))
                orders = cursor.fetchall()
                
                logger.info(f"随机抽取了 {len(orders)} 条工单记录进行检查")
                return [order['Id'] for order in orders]
        except Exception as e:
            logger.error(f"随机抽取工单时发生错误: {str(e)}")
            return []
    
    def get_mysql_data(self, order_id):
        """从MySQL获取指定订单ID的数据"""
        result = {}
        
        try:
            with self.db_connector as db:
                cursor = db.mysql_conn.cursor()
                
                # 获取主表名称
                main_table = "tb_workorderinfo"
                for table, mapping in self.table_mappings.items():
                    if mapping['es_path'] == "":
                        main_table = table
                        break
                
                # 查询主表数据
                main_sql = f"SELECT * FROM {main_table} WHERE Id = %s"
                cursor.execute(main_sql, (order_id,))
                main_data = cursor.fetchone()
                
                if not main_data:
                    logger.warning(f"MySQL中未找到工单 {order_id} 的数据")
                    return None
                
                # 存储主表数据
                result['main'] = main_data
                result['nested'] = {}
                result['special'] = {}  # 存储特殊表数据
                
                # 查询普通子表数据
                for table_name, mapping in self.table_mappings.items():
                    if mapping['es_path'] == "":  # 跳过主表
                        continue
                    
                    nested_sql = f"SELECT * FROM {table_name} WHERE {mapping['id_field']} = %s"
                    cursor.execute(nested_sql, (order_id,))
                    nested_data = cursor.fetchall()
                    
                    # 存储子表数据
                    result['nested'][table_name] = nested_data
                
                # 查询特殊表数据
                # tb_operatinginfo表（对应operating索引）
                nested_sql = "SELECT * FROM tb_operatinginfo WHERE WorkOrderId = %s"
                cursor.execute(nested_sql, (order_id,))
                operating_data = cursor.fetchall()
                result['special']['tb_operatinginfo'] = operating_data
                
                # basic_custspecialconfig表（对应custspecialconfig索引）
                # 需要根据 CustomerId 查询
                customer_id = main_data.get('CustomerId')
                if customer_id:
                    nested_sql = "SELECT * FROM basic_custspecialconfig WHERE CustomerId = %s AND Deleted = 0"
                    cursor.execute(nested_sql, (customer_id,))
                    custconfig_data = cursor.fetchall()
                    result['special']['basic_custspecialconfig'] = custconfig_data
                
                return result
        except Exception as e:
            logger.error(f"获取MySQL工单 {order_id} 数据时发生错误: {str(e)}")
            return None
    
    def get_es_data(self, order_id):
        """从ElasticSearch获取指定订单ID的数据"""
        try:
            with self.db_connector as db:
                result = {}
                
                # 查询主索引数据
                query = {
                    "query": {
                        "term": {
                            "Id": order_id
                        }
                    }
                }
                
                # 查询主索引
                response = db.es_client.search(
                    index=self.main_index_name,
                    body=query
                )
                
                # 检查是否有匹配结果
                hits = response.get('hits', {}).get('hits', [])
                if not hits:
                    logger.warning(f"ElasticSearch主索引中未找到工单 {order_id} 的数据")
                    return None
                
                # 获取主索引数据
                result = hits[0]['_source']
                
                # 查询operating索引数据（对应tb_operatinginfo表）
                operating_query = {
                    "query": {
                        "term": {
                            "WorkOrderId": order_id
                        }
                    },
                    "size": 100  # 最多获取100条记录
                }
                
                try:
                    operating_response = db.es_client.search(
                        index=self.operating_index_name,
                        body=operating_query
                    )
                    
                    operating_hits = operating_response.get('hits', {}).get('hits', [])
                    if operating_hits:
                        # 添加operating数据到结果中
                        result['operating_data'] = [hit['_source'] for hit in operating_hits]
                        logger.info(f"从operating索引中获取到 {len(operating_hits)} 条数据")
                except Exception as e:
                    logger.warning(f"查询operating索引时出错: {str(e)}")
                
                # 查询custspecialconfig索引数据（对应basic_custspecialconfig表）
                # 这里需要根据工单中的CustomerId字段来查询
                customer_id = result.get('CustomerId')
                if customer_id:
                    custconfig_query = {
                        "query": {
                            "term": {
                                "CustomerId": customer_id
                            }
                        },
                        "size": 100  # 最多获取100条记录
                    }
                    
                    try:
                        custconfig_response = db.es_client.search(
                            index=self.custspecialconfig_index_name,
                            body=custconfig_query
                        )
                        
                        custconfig_hits = custconfig_response.get('hits', {}).get('hits', [])
                        if custconfig_hits:
                            # 添加custspecialconfig数据到结果中
                            result['custspecialconfig_data'] = [hit['_source'] for hit in custconfig_hits]
                            logger.info(f"从custspecialconfig索引中获取到 {len(custconfig_hits)} 条数据")
                    except Exception as e:
                        logger.warning(f"查询custspecialconfig索引时出错: {str(e)}")
                
                return result
        except Exception as e:
            logger.error(f"获取ElasticSearch工单 {order_id} 数据时发生错误: {str(e)}")
            return None
    
    def compare_field_values(self, mysql_value, es_value, field_name):
        """比较MySQL和ES中字段值是否一致"""
        # 处理None值
        if mysql_value is None and (es_value is None or es_value == ""):
            return True
        
        # 处理日期字段的特殊比较
        if field_name in ('CreatedAt', 'UpdatedAt', 'DeletedAt', 'InstallTime', 'RequiredTime', 
                         'EffectiveTime', 'EffectiveSuccessfulTime', 'LastUpdateTimeStamp'):
            if mysql_value is not None:
                # 将MySQL的datetime转为字符串格式
                if isinstance(mysql_value, datetime):
                    mysql_value = mysql_value.strftime('%Y-%m-%d %H:%M:%S')
                
                # 如果ES中存储的是日期对象
                if isinstance(es_value, str):
                    # 移除可能的时区信息
                    es_value = es_value.split('+')[0].split('Z')[0].strip()
                    # 正规化日期格式
                    es_value = es_value.replace('T', ' ')
                    # 截取到秒级别比较
                    mysql_value = mysql_value[:19]
                    es_value = es_value[:19]
                    
                    return mysql_value == es_value
        
        # 处理布尔值和数字类型
        if isinstance(mysql_value, bool) and not isinstance(es_value, bool):
            return str(int(mysql_value)) == str(es_value)
        
        if isinstance(mysql_value, (int, float)) and isinstance(es_value, str):
            return str(mysql_value) == es_value
        
        # 处理字节类型
        if isinstance(mysql_value, bytes):
            mysql_value = mysql_value.decode('utf-8', errors='ignore')
        
        # 其他情况直接比较字符串表示
        return str(mysql_value) == str(es_value)
    
    def compare_data(self, mysql_data, es_data, order_id):
        """比较MySQL和ES中的数据是否一致"""
        if not mysql_data or not es_data:
            logger.warning(f"工单 {order_id} 在MySQL或ES中数据缺失，无法比较")
            return False, []
        
        discrepancies = []
        
        # 查找主表映射
        main_table = None
        for table_name, mapping in self.table_mappings.items():
            if mapping['es_path'] == "":
                main_table = table_name
                break
        
        if not main_table:
            logger.error("未找到主表映射定义")
            return False, []
        
        # 比较主表字段
        main_mapping = self.table_mappings[main_table]
        for mysql_field, es_field in main_mapping['fields']:
            mysql_value = mysql_data['main'].get(mysql_field)
            es_value = es_data.get(es_field)
            
            if not self.compare_field_values(mysql_value, es_value, mysql_field):
                discrepancies.append({
                    'table': main_table,
                    'field': mysql_field,
                    'mysql_value': mysql_value,
                    'es_value': es_value
                })
        
        # 比较正常子表数据
        for table_name, mapping in self.table_mappings.items():
            if mapping['es_path'] == "":  # 跳过主表
                continue
            
            # 如果是特殊表则跳过，特殊表在下面单独处理
            if table_name in self.special_tables:
                continue
            
            es_path = mapping['es_path']
            mysql_nested_data = mysql_data['nested'].get(table_name, [])
            es_nested_data = es_data.get(es_path, [])
            
            # 检查子表数据条数
            if len(mysql_nested_data) != len(es_nested_data):
                discrepancies.append({
                    'table': table_name,
                    'type': 'count_mismatch',
                    'mysql_count': len(mysql_nested_data),
                    'es_count': len(es_nested_data)
                })
            
            # 构建ID到记录的映射，以便精确比较
            mysql_id_map = {str(item['Id']): item for item in mysql_nested_data}
            es_id_map = {str(item['Id']): item for item in es_nested_data}
            
            # 检查MySQL中有但ES中没有的记录
            for id_val, mysql_item in mysql_id_map.items():
                if id_val not in es_id_map:
                    discrepancies.append({
                        'table': table_name,
                        'type': 'missing_in_es',
                        'id': id_val
                    })
                    continue
                
                # 比较每个字段的值
                es_item = es_id_map[id_val]
                for mysql_field, es_field in mapping['fields']:
                    mysql_value = mysql_item.get(mysql_field)
                    es_value = es_item.get(es_field)
                    
                    if not self.compare_field_values(mysql_value, es_value, mysql_field):
                        discrepancies.append({
                            'table': table_name,
                            'id': id_val,
                            'field': mysql_field,
                            'mysql_value': mysql_value,
                            'es_value': es_value
                        })
            
            # 检查ES中有但MySQL中没有的记录
            for id_val in es_id_map:
                if id_val not in mysql_id_map:
                    discrepancies.append({
                        'table': table_name,
                        'type': 'missing_in_mysql',
                        'id': id_val
                    })
        
        # 比较特殊表数据
        for table_name, special_config in self.special_tables.items():
            mysql_data_list = mysql_data['special'].get(table_name, [])
            
            # 根据表名确定使用的ES数据键名
            es_data_key = None
            if table_name == "tb_operatinginfo":
                es_data_key = "operating_data"
            elif table_name == "basic_custspecialconfig":
                es_data_key = "custspecialconfig_data"
            
            es_data_list = es_data.get(es_data_key, [])
            
            # 检查特殊表数据条数
            if len(mysql_data_list) != len(es_data_list):
                discrepancies.append({
                    'table': table_name,
                    'type': 'count_mismatch',
                    'mysql_count': len(mysql_data_list),
                    'es_count': len(es_data_list)
                })
            
            # 构建ID到记录的映射
            mysql_id_map = {str(item['Id']): item for item in mysql_data_list}
            es_id_map = {str(item['Id']): item for item in es_data_list}
            
            # 检查每个记录
            for id_val, mysql_item in mysql_id_map.items():
                if id_val not in es_id_map:
                    discrepancies.append({
                        'table': table_name,
                        'type': 'missing_in_es',
                        'id': id_val
                    })
                    continue
                
                # 特殊表直接比较各个字段（不使用字段映射）
                es_item = es_id_map[id_val]
                for field in mysql_item.keys():
                    mysql_value = mysql_item.get(field)
                    es_value = es_item.get(field)
                    
                    if not self.compare_field_values(mysql_value, es_value, field):
                        discrepancies.append({
                            'table': table_name,
                            'id': id_val,
                            'field': field,
                            'mysql_value': mysql_value,
                            'es_value': es_value
                        })
            
            # 检查ES中有但MySQL中没有的记录
            for id_val in es_id_map:
                if id_val not in mysql_id_map:
                    discrepancies.append({
                        'table': table_name,
                        'type': 'missing_in_mysql',
                        'id': id_val
                    })
        
        is_consistent = len(discrepancies) == 0
        return is_consistent, discrepancies
    
    def format_discrepancy_message(self, order_id, discrepancies):
        """格式化不一致消息，用于企业微信通知"""
        message = f"工单ID: {order_id}\n\n"
        
        for disc in discrepancies[:10]:  # 限制显示前10个不一致
            if disc.get('type') == 'count_mismatch':
                message += f"表 **{disc['table']}** 数据条数不一致：MySQL {disc['mysql_count']} vs ES {disc['es_count']}\n\n"
            elif disc.get('type') == 'missing_in_es':
                message += f"表 **{disc['table']}** 中ID为 {disc['id']} 的记录在ES中缺失\n\n"
            elif disc.get('type') == 'missing_in_mysql':
                message += f"表 **{disc['table']}** 中ID为 {disc['id']} 的记录在MySQL中缺失\n\n"
            else:
                # 字段值不一致
                mysql_value = str(disc['mysql_value'])[:50]
                es_value = str(disc['es_value'])[:50]
                
                # 对于超过50个字符的值，添加省略号
                if len(str(disc['mysql_value'])) > 50:
                    mysql_value += "..."
                if len(str(disc['es_value'])) > 50:
                    es_value += "..."
                
                message += f"表 **{disc['table']}** 字段 **{disc.get('field')}** 值不一致：\n"
                message += f"MySQL: {mysql_value}\n"
                message += f"ES: {es_value}\n\n"
        
        if len(discrepancies) > 10:
            message += f"还有 {len(discrepancies) - 10} 处不一致未显示...\n"
        
        message += f"\n检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        return message
    
    def check_consistency(self):
        """执行数据一致性检查"""
        logger.info("开始数据一致性检查...")
        
        # 随机获取工单ID
        order_ids = self.get_random_orders()
        if not order_ids:
            logger.error("未能获取工单ID进行检查")
            return
        
        inconsistent_count = 0
        
        for order_id in order_ids:
            logger.info(f"正在检查工单 {order_id}")
            
            # 获取MySQL数据
            mysql_data = self.get_mysql_data(order_id)
            
            # 获取ES数据
            es_data = self.get_es_data(order_id)
            
            # 比较数据一致性
            is_consistent, discrepancies = self.compare_data(mysql_data, es_data, order_id)
            
            if not is_consistent:
                inconsistent_count += 1
                logger.warning(f"工单 {order_id} 数据不一致，发现 {len(discrepancies)} 处差异")
                
                # 格式化消息并发送企业微信通知
                message = self.format_discrepancy_message(order_id, discrepancies)
                self.wechat.send_message(f"数据一致性检查 - 发现不一致", message)
            else:
                logger.info(f"工单 {order_id} 数据一致")
        
        # 检查结果汇总
        summary = f"数据一致性检查完成。共检查 {len(order_ids)} 条记录，发现 {inconsistent_count} 条不一致。"
        logger.info(summary)
        
        # 如果全部一致，也发送一个通知
        if inconsistent_count == 0 and order_ids:
            self.wechat.send_message("数据一致性检查 - 全部一致", 
                                    f"本次检查的 {len(order_ids)} 条工单数据全部一致。\n\n检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return inconsistent_count == 0
