from flask import Blueprint, request, jsonify
import mysql.connector
from mysql.connector import Error
from utils.db import get_db_connection

social_security_bp = Blueprint('medical_insurance', __name__)

# 插入单条社保缴纳记录的接口
@social_security_bp.route('/medical_insurance_payments', methods=['POST'])
def insert_social_security_payment():
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
        INSERT INTO `medical_insurance_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        values = (date, personal_payment, company_payment, remarks)
        
        cursor.execute(insert_query, values)
        connection.commit()
        
        return jsonify({
            'message': 'medical insurance record inserted successfully',
            'id': cursor.lastrowid
        }), 201
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 批量插入社保缴纳记录的接口
@social_security_bp.route('/medical_insurance_payments/batch', methods=['POST'])
def insert_social_security_payments_batch():
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
        INSERT INTO `medical_insurance_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        
        values = [(record['date'], record['personal_payment'], record['company_payment'], record['remarks']) for record in data]
        
        cursor.executemany(insert_query, values)
        connection.commit()
        
        return jsonify({
            'message': f'Successfully inserted {cursor.rowcount} social security records',
            'inserted_count': cursor.rowcount
        }), 201
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 查询社保缴纳记录的接口
@social_security_bp.route('/medical_insurance_payments', methods=['GET'])
def query_social_security_payments():
    try:
        id = request.args.get('id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        connection, cursor = get_db_connection(dictionary=True)
        
        query = "SELECT id, date, personal_payment, company_payment, remarks FROM medical_insurance_payments WHERE 1=1"
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

# 删除单条社保缴纳记录的接口
@social_security_bp.route('/medical_insurance_payments/<int:id>', methods=['DELETE'])
def delete_social_security_payment(id):
    try:
        connection, cursor = get_db_connection()
        
        check_query = "SELECT id FROM medical_insurance_payments WHERE id = %s"
        cursor.execute(check_query, (id,))
        if not cursor.fetchone():
            return jsonify({'error': f'Social security record with id {id} not found'}), 404
        
        delete_query = "DELETE FROM medical_insurance_payments WHERE id = %s"
        cursor.execute(delete_query, (id,))
        connection.commit()
        
        return jsonify({
            'message': f'Social security record with id {id} deleted successfully'
        }), 200
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# 批量删除社保缴纳记录的接口
@social_security_bp.route('/medical_insurance_payments/batch', methods=['DELETE'])
def delete_social_security_payments_batch():
    try:
        data = request.get_json()
        
        if not isinstance(data, list) or not all(isinstance(id, int) for id in data):
            return jsonify({'error': 'Request body must be a list of integer IDs'}), 400
        
        if not data:
            return jsonify({'error': 'ID list cannot be empty'}), 400
        
        connection, cursor = get_db_connection()
        
        check_query = "SELECT id FROM medical_insurance_payments WHERE id IN (%s)" % ','.join(['%s'] * len(data))
        cursor.execute(check_query, data)
        existing_ids = [row[0] for row in cursor.fetchall()]
        
        if not existing_ids:
            return jsonify({'error': 'No social security records found for provided IDs'}), 404
        
        delete_query = "DELETE FROM medical_insurance_payments WHERE id IN (%s)" % ','.join(['%s'] * len(existing_ids))
        cursor.execute(delete_query, existing_ids)
        connection.commit()
        
        return jsonify({
            'message': f'Successfully deleted {cursor.rowcount} social security records',
            'deleted_count': cursor.rowcount,
            'deleted_ids': existing_ids
        }), 200
        
    except Error as e:
        return jsonify({'error': str(e)}), 500
        
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()