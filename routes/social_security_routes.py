from flask import Blueprint, request, jsonify
from mysql.connector import Error
from utils.db import get_db_connection
from datetime import datetime, date
import logging
import os

# 配置日志记录，级别由环境变量控制
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO), filename='app.log')
logger = logging.getLogger(__name__)

# 创建 Flask 蓝图，用于组织社保相关的路由
social_security_bp = Blueprint('social_security', __name__)

# 必需字段常量
REQUIRED_FIELDS = ['date', 'personal_payment', 'company_payment', 'personal_account', 'remarks']

def format_date(date_value, record_id):
    """将日期格式化为 YYYY-MM，记录解析错误"""
    if not date_value:
        return None
    if isinstance(date_value, (datetime, date)):
        return date_value.strftime('%Y-%m')
    if isinstance(date_value, str):
        try:
            parsed_date = datetime.strptime(date_value, '%a, %d %b %Y %H:%M:%S %Z')
            return parsed_date.strftime('%Y-%m')
        except ValueError:
            try:
                parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
                return parsed_date.strftime('%Y-%m')
            except ValueError:
                logger.error(f"Failed to parse date: {date_value} for record ID: {record_id}")
                return None
    logger.error(f"Unsupported date type: {type(date_value)} for record ID: {record_id}")
    return None

def validate_date(date_str):
    """验证日期格式为 YYYY-MM-DD"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def validate_payment(value, field_name):
    """验证金额为正数"""
    try:
        val = float(value)
        if val < 0:
            return False, f"{field_name} must be non-negative"
        return True, None
    except (ValueError, TypeError):
        return False, f"{field_name} must be a valid number"

@social_security_bp.route('/social_security_payments', methods=['GET'])
def query_social_security_payments():
    """
    查询社保缴纳记录，支持按 ID、日期范围、年份和金额过滤，默认返回最新 20 条记录。
    支持分页（page, per_page）。返回的 date 字段格式为 YYYY-MM（例如 "2023-01"）。
    """
    try:
        # 获取查询参数
        id = request.args.get('id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        year = request.args.get('year')
        personal_payment = request.args.get('personal_payment')
        company_payment = request.args.get('company_payment')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        # 验证分页参数
        if page < 1 or per_page < 1:
            return jsonify({'error': 'Page and per_page must be positive integers'}), 400
        
        # 验证日期格式
        if start_date and not validate_date(start_date):
            return jsonify({'error': 'start_date must be in YYYY-MM-DD format'}), 400
        if end_date and not validate_date(end_date):
            return jsonify({'error': 'end_date must be in YYYY-MM-DD format'}), 400
        
        # 验证金额
        if personal_payment:
            valid, error = validate_payment(personal_payment, 'personal_payment')
            if not valid:
                return jsonify({'error': error}), 400
        if company_payment:
            valid, error = validate_payment(company_payment, 'company_payment')
            if not valid:
                return jsonify({'error': error}), 400
        
        # 获取数据库连接和游标
        connection, cursor = get_db_connection(dictionary=True)
        
        # 基础 SQL 查询
        query = "SELECT id, date, personal_payment, company_payment, personal_account, remarks FROM social_security_payments WHERE 1=1"
        params = []
        
        # 动态添加过滤条件
        if id:
            query += " AND id = %s"
            params.append(id)
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        if year:
            try:
                int(year)
                query += " AND year = %s"  # 使用生成列 year
                params.append(year)
            except ValueError:
                return jsonify({'error': 'Year must be a valid integer'}), 400
        if personal_payment:
            query += " AND personal_payment = %s"
            params.append(float(personal_payment))
        if company_payment:
            query += " AND company_payment = %s"
            params.append(float(company_payment))
        
        # 添加分页和排序
        offset = (page - 1) * per_page
        query += " ORDER BY date DESC LIMIT %s OFFSET %s"
        params.extend([per_page, offset])
        
        # 执行查询
        cursor.execute(query, params)
        records = cursor.fetchall()
        
        # 格式化 date 字段
        for record in records:
            record['date'] = format_date(record['date'], record['id'])
        
        # 返回响应
        return jsonify({
            'message': 'Query successful',
            'records': records,
            'count': len(records),
            'page': page,
            'per_page': per_page
        }), 200
        
    except Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@social_security_bp.route('/social_security_payments', methods=['POST'])
def insert_social_security_payment():
    """
    插入单条社保缴纳记录，需提供日期（YYYY-MM-DD 格式）、个人缴纳金额、公司缴纳金额、个人账户金额和备注。
    验证日期和金额格式。
    """
    connection = None
    cursor = None
    try:
        if not request.is_json:
            return jsonify({'error': 'Content-Type must be application/json'}), 400
        
        data = request.get_json()
        for field in REQUIRED_FIELDS:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        date = data['date']
        personal_payment = data['personal_payment']
        company_payment = data['company_payment']
        personal_account = data['personal_account']
        remarks = data['remarks']
        
        # 验证日期
        if not validate_date(date):
            return jsonify({'error': 'Date must be in YYYY-MM-DD format'}), 400
        
        # 验证金额
        for field, value in [
            ('personal_payment', personal_payment),
            ('company_payment', company_payment),
            ('personal_account', personal_account)
        ]:
            valid, error = validate_payment(value, field)
            if not valid:
                return jsonify({'error': error}), 400
        
        connection, cursor = get_db_connection()
        
        insert_query = """
        INSERT INTO `social_security_payments` 
        (`date`, `personal_payment`, `company_payment`, `personal_account`, `remarks`) 
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (date, float(personal_payment), float(company_payment), float(personal_account), remarks)
        
        cursor.execute(insert_query, values)
        connection.commit()
        
        return jsonify({
            'message': 'Social security record inserted successfully',
            'id': cursor.lastrowid
        }), 201
        
    except Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Request error: {str(e)}")
        return jsonify({'error': f'Invalid request: {str(e)}'}), 400
        
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

@social_security_bp.route('/social_security_payments/batch', methods=['POST'])
def insert_social_security_payments_batch():
    """
    批量插入社保缴纳记录，需提供记录列表，每条记录包含日期（YYYY-MM-DD 格式）、金额和备注。
    验证格式并使用事务。
    """
    connection = None
    cursor = None
    try:
        if not request.is_json:
            return jsonify({'error': 'Content-Type must be application/json'}), 400
        
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({'error': 'Request body must be a list of records'}), 400
        
        for record in data:
            for field in REQUIRED_FIELDS:
                if field not in record:
                    return jsonify({'error': f'Missing required field in record: {field}'}), 400
            if not validate_date(record['date']):
                return jsonify({'error': f"Date must be in YYYY-MM-DD format in record: {record}"}), 400
            for field in ['personal_payment', 'company_payment', 'personal_account']:
                valid, error = validate_payment(record[field], field)
                if not valid:
                    return jsonify({'error': error}), 400
        
        connection, cursor = get_db_connection()
        
        insert_query = """
        INSERT INTO `social_security_payments` 
        (`date`, `personal_payment`, `company_payment`, `personal_account`, `remarks`) 
        VALUES (%s, %s, %s, %s, %s)
        """
        values = [(r['date'], float(r['personal_payment']), float(r['company_payment']), float(r['personal_account']), r['remarks']) for r in data]
        
        cursor.executemany(insert_query, values)
        connection.commit()
        
        return jsonify({
            'message': f'Successfully inserted {cursor.rowcount} social security records',
            'inserted_count': cursor.rowcount
        }), 201
        
    except Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Request error: {str(e)}")
        return jsonify({'error': f'Invalid request: {str(e)}'}), 400
        
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

@social_security_bp.route('/social_security_payments/<int:id>', methods=['DELETE'])
def delete_social_security_payment(id):
    """
    删除指定 ID 的社保缴纳记录。
    """
    connection = None
    cursor = None
    try:
        connection, cursor = get_db_connection()
        
        check_query = "SELECT id FROM social_security_payments WHERE id = %s"
        cursor.execute(check_query, (id,))
        if not cursor.fetchone():
            return jsonify({'error': f'Social security record with id {id} not found'}), 404
        
        delete_query = "DELETE FROM social_security_payments WHERE id = %s"
        cursor.execute(delete_query, (id,))
        connection.commit()
        
        return jsonify({
            'message': f'Social security record with id {id} deleted successfully'
        }), 200
        
    except Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

@social_security_bp.route('/social_security_payments/batch', methods=['DELETE'])
def delete_social_security_payments_batch():
    """
    批量删除指定 ID 列表的社保缴纳记录。
    请求体需为整数 ID 列表。
    """
    connection = None
    cursor = None
    try:
        data = request.get_json()
        if not isinstance(data, list) or not all(isinstance(id, int) for id in data):
            return jsonify({'error': 'Request body must be a list of integer IDs'}), 400
        if not data:
            return jsonify({'error': 'ID list cannot be empty'}), 400
        
        connection, cursor = get_db_connection()
        
        check_query = "SELECT id FROM social_security_payments WHERE id IN (%s)" % ','.join(['%s'] * len(data))
        cursor.execute(check_query, data)
        existing_ids = [row[0] for row in cursor.fetchall()]
        
        if not existing_ids:
            return jsonify({'error': 'No social security records found for provided IDs'}), 404
        
        delete_query = "DELETE FROM social_security_payments WHERE id IN (%s)" % ','.join(['%s'] * len(existing_ids))
        cursor.execute(delete_query, existing_ids)
        connection.commit()
        
        return jsonify({
            'message': f'Successfully deleted {cursor.rowcount} social security records',
            'deleted_count': cursor.rowcount,
            'deleted_ids': existing_ids
        }), 200
        
    except Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()