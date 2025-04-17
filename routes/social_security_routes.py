from flask import Blueprint, request, jsonify
from mysql.connector import Error
from utils.db import get_db_connection
from datetime import datetime, date
import logging

# 配置日志记录，用于调试日期格式化和错误
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 创建 Flask 蓝图，用于组织社保相关的路由
social_security_bp = Blueprint('social_security', __name__)

@social_security_bp.route('/social_security_payments', methods=['GET'])
def query_social_security_payments():
    """
    查询社保缴纳记录，支持按 ID、日期范围和年份过滤，默认返回最新 20 条记录。
    返回的 date 字段格式为 YYYY-MM（例如 "2023-01"）。
    支持 datetime.date、datetime.datetime 和字符串格式的日期，记录解析失败的日志。
    """
    try:
        # 获取查询参数：记录 ID、开始日期、结束日期、年份
        id = request.args.get('id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        year = request.args.get('year')
        
        # 获取数据库连接和游标，dictionary=True 使查询结果返回字典格式
        connection, cursor = get_db_connection(dictionary=True)
        
        # 基础 SQL 查询，WHERE 1=1 便于动态添加条件
        query = "SELECT id, date, personal_payment, company_payment, remarks FROM social_security_payments WHERE 1=1"
        params = []
        
        # 如果提供了 ID，添加 ID 过滤条件
        if id:
            query += " AND id = %s"
            params.append(id)
        # 如果提供了开始日期，添加日期范围过滤（>=）
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        # 如果提供了结束日期，添加日期范围过滤（<=）
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        # 如果提供了年份，验证是整数并添加年份过滤
        if year:
            try:
                int(year)
                query += " AND YEAR(date) = %s"
                params.append(year)
            except ValueError:
                return jsonify({'error': 'Year must be a valid integer'}), 400
        
        # 默认按日期降序排序，限制返回 20 条记录
        query += " ORDER BY date DESC LIMIT 20"
        
        # 执行查询，获取结果
        cursor.execute(query, params)
        records = cursor.fetchall()
        
        # 格式化每条记录的 date 字段为 YYYY-MM
        for record in records:
            date_value = record['date']
            if date_value:
                if isinstance(date_value, (datetime, date)):
                    # 处理 datetime.datetime 或 datetime.date，直接格式化为 YYYY-MM
                    record['date'] = date_value.strftime('%Y-%m')
                elif isinstance(date_value, str):
                    # 处理字符串格式的日期，尝试多种解析格式
                    try:
                        # 尝试解析 RFC 1123 格式（例如 "Tue, 01 Dec 2020 00:00:00 GMT"）
                        parsed_date = datetime.strptime(date_value, '%a, %d %b %Y %H:%M:%S %Z')
                        record['date'] = parsed_date.strftime('%Y-%m')
                    except ValueError:
                        try:
                            # 尝试解析 YYYY-MM-DD 格式
                            parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
                            record['date'] = parsed_date.strftime('%Y-%m')
                        except ValueError:
                            # 记录解析失败的日期值
                            logger.error(f"Failed to parse date string: {date_value} for record ID: {record['id']}")
                            record['date'] = None
                else:
                    # 记录不支持的日期类型
                    logger.error(f"Unsupported date type: {type(date_value)} for record ID: {record['id']}")
                    record['date'] = None
            else:
                # 如果 date 为空或 None，保持 None
                record['date'] = None
        
        # 返回查询成功的 JSON 响应
        return jsonify({
            'message': 'Query successful',
            'records': records,
            'count': len(records)
        }), 200
        
    except Error as e:
        # 捕获 MySQL 相关错误，返回 500 状态码
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        # 关闭游标和数据库连接（如果已连接）
        if connection.is_connected():
            cursor.close()
            connection.close()

@social_security_bp.route('/social_security_payments', methods=['POST'])
def insert_social_security_payment():
    """
    插入单条社保缴纳记录，需提供日期（YYYY-MM-DD 格式）、个人缴纳金额、公司缴纳金额和备注。
    验证日期格式，防止插入无效数据。
    """
    connection = None
    cursor = None
    try:
        # 验证请求是否为 JSON 格式
        if not request.is_json:
            return jsonify({'error': 'Content-Type must be application/json'}), 400
        
        # 获取 JSON 数据
        data = request.get_json()
        required_fields = ['date', 'personal_payment', 'company_payment', 'remarks']
        # 检查必需字段是否齐全
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # 提取字段值
        date = data['date']
        personal_payment = data['personal_payment']
        company_payment = data['company_payment']
        remarks = data['remarks']
        
        # 验证日期格式为 YYYY-MM-DD
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Date must be in YYYY-MM-DD format'}), 400
        
        # 获取数据库连接和游标
        connection, cursor = get_db_connection()
        
        # 插入记录的 SQL 查询
        insert_query = """
        INSERT INTO `social_security_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        values = (date, personal_payment, company_payment, remarks)
        
        # 执行插入操作并提交
        cursor.execute(insert_query, values)
        connection.commit()
        
        # 返回插入成功的响应，包括新记录的 ID
        return jsonify({
            'message': 'Social security record inserted successfully',
            'id': cursor.lastrowid
        }), 201
        
    except Error as e:
        # 捕获 MySQL 错误，返回 500 状态码
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        # 捕获其他异常（如 JSON 解析错误），返回 400 状态码
        logger.error(f"Request error: {str(e)}")
        return jsonify({'error': f'Invalid request: {str(e)}'}), 400
        
    finally:
        # 安全关闭游标和连接
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

@social_security_bp.route('/social_security_payments/<int:id>', methods=['DELETE'])
def delete_social_security_payment(id):
    """
    删除指定 ID 的社保缴纳记录。
    """
    try:
        # 获取数据库连接和游标
        connection, cursor = get_db_connection()
        
        # 检查记录是否存在
        check_query = "SELECT id FROM social_security_payments WHERE id = %s"
        cursor.execute(check_query, (id,))
        if not cursor.fetchone():
            return jsonify({'error': f'Social security record with id {id} not found'}), 404
        
        # 删除记录的 SQL 查询
        delete_query = "DELETE FROM social_security_payments WHERE id = %s"
        cursor.execute(delete_query, (id,))
        connection.commit()
        
        # 返回删除成功的响应
        return jsonify({
            'message': f'Social security record with id {id} deleted successfully'
        }), 200
        
    except Error as e:
        # 捕获 MySQL 错误，返回 500 状态码
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        # 关闭游标和数据库连接
        if connection.is_connected():
            cursor.close()
            connection.close()