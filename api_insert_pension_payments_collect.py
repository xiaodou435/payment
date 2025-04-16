from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

# 数据库配置
db_config = {
    'host': 'localhost',
    'database': 'your_database_name',  # 替换为您的数据库名
    'user': 'your_username',          # 替换为您的数据库用户名
    'password': 'your_password'       # 替换为您的数据库密码
}

# 批量插入养老缴纳记录的接口
@app.route('/api/pension_payments/batch', methods=['POST'])
def insert_pension_payments_batch():
    try:
        # 获取请求中的 JSON 数据
        data = request.get_json()
        
        # 验证数据是否为列表
        if not isinstance(data, list):
            return jsonify({'error': 'Request body must be a list of records'}), 400
        
        # 验证每条记录的必需字段
        required_fields = ['date', 'personal_payment', 'company_payment', 'remarks']
        for record in data:
            for field in required_fields:
                if field not in record:
                    return jsonify({'error': f'Missing required field in record: {field}'}), 400
        
        # 连接数据库
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # 插入数据的 SQL 语句
        insert_query = """
        INSERT INTO `pension_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        
        # 准备插入的值
        values = [
            (
                record['date'],
                record['personal_payment'],
                record['company_payment'],
                record['remarks']
            )
            for record in data
        ]
        
        # 批量执行插入
        cursor.executemany(insert_query, values)
        connection.commit()
        
        # 返回成功响应
        return jsonify({
            'message': f'Successfully inserted {cursor.rowcount} records',
            'inserted_count': cursor.rowcount
        }), 201
        
    except Error as e:
        # 返回错误响应
        return jsonify({'error': str(e)}), 500
        
    finally:
        # 关闭数据库连接
        if connection.is_connected():
            cursor.close()
            connection.close()

# 启动 Flask 应用
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)  # 使用 5001 端口，避免与之前的接口冲突