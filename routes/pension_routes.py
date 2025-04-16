from flask import Blueprint, request, jsonify
import mysql.connector
from mysql.connector import Error
from utils.db import get_db_connection

pension_bp = Blueprint('pension', __name__)

# 插入单条养老缴纳记录的接口
@pension_bp.route('/pension_payments', methods=['POST'])
def insert_pension_payment():
    try:
        data = request.get_json()
        required_fields = ['date', 'personal_payment', 'company_payment', 'remarks']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        date = data['date']
        personal_payment = data['personal_payment']
        company_payment = data['company_payment']
        remarks = data['remarks']
        
        connection, cursor = get_db_connection()
        
        insert_query = """
        INSERT INTO `pension_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        values = (date, personal_payment, company_payment, remarks)
        
        cursor.execute(insert_query, values)
        connection.commit()
        
        return jsonify({
            'message': 'Pension record inserted successfully',
            'id': cursor.lastrowid
        }), 201
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 批量插入养老缴纳记录的接口
@pension_bp.route('/pension_payments/batch', methods=['POST'])
def insert_pension_payments_batch():
    try:
        data = request.get_json()
        
        if not isinstance(data, list):
            return jsonify({'error': 'Request body must be a list of records'}), 400
        
        required_fields = ['date', 'personal_payment', 'company_payment', 'remarks']
        for record in data:
            for field in required_fields:
                if field not in record:
                    return jsonify({'error': f'Missing required field in record: {field}'}), 400
        
        connection, cursor = get_db_connection()
        
        insert_query = """
        INSERT INTO `pension_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        
        values = [(record['date'], record['personal_payment'], record['company_payment'], record['remarks']) for record in data]
        
        cursor.executemany(insert_query, values)
        connection.commit()
        
        return jsonify({
            'message': f'Successfully inserted {cursor.rowcount} pension records',
            'inserted_count': cursor.rowcount
        }), 201
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 查询养老缴纳记录的接口
@pension_bp.route('/pension_payments', methods=['GET'])
def query_pension_payments():
    try:
        id = request.args.get('id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        year = request.args.get('year')
        
        connection, cursor = get_db_connection(dictionary=True)
        
        # 使用 DATE_FORMAT 格式化 date 字段为 年-月
        query = """
        SELECT id, DATE_FORMAT(date, '%Y-%m') AS date, 
               personal_payment, company_payment, remarks 
        FROM pension_payments WHERE 1=1
        """
        params = []
        
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
            query += " AND YEAR(date) = %s"
            params.append(year)
        
        # 添加排序和默认限制
        query += " ORDER BY date DESC"
        if not (id or start_date or end_date or year):
            query += " LIMIT 20"
        
        cursor.execute(query, params)
        records = cursor.fetchall()
        
        return jsonify({
            'message': 'Query successful',
            'records': records,
            'count': len(records)
        }), 200
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 删除单条养老缴纳记录的接口
@pension_bp.route('/pension_payments/<int:id>', methods=['DELETE'])
def delete_pension_payment(id):
    try:
        connection, cursor = get_db_connection()
        
        check_query = "SELECT id FROM pension_payments WHERE id = %s"
        cursor.execute(check_query, (id,))
        if not cursor.fetchone():
            return jsonify({'error': f'Pension record with id {id} not found'}), 404
        
        delete_query = "DELETE FROM pension_payments WHERE id = %s"
        cursor.execute(delete_query, (id,))
        connection.commit()
        
        return jsonify({
            'message': f'Pension record with id {id} deleted successfully'
        }), 200
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()