from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

# 数据库配置
db_config = {
    'host': 'localhost',
    'database': 'trojan',  # 替换为您的数据库名
    'user': 'root',          # 替换为您的数据库用户名
    'password': 'trojan'       # 替换为您的数据库密码
}

# 插入单条养老缴纳记录的接口
@app.route('/api/pension_payments', methods=['POST'])
def insert_pension_payment():
    try:
        # 获取请求中的 JSON 数据
        data = request.get_json()
        
        # 验证必需字段
        required_fields = ['date', 'personal_payment', 'company_payment', 'remarks']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # 提取数据
        date = data['date']
        personal_payment = data['personal_payment']
        company_payment = data['company_payment']
        remarks = data['remarks']
        
        # 连接数据库
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # 插入数据的 SQL 语句
        insert_query = """
        INSERT INTO `pension_payments` 
        (`date`, `personal_payment`, `company_payment`, `remarks`) 
        VALUES (%s, %s, %s, %s)
        """
        values = (date, personal_payment, company_payment, remarks)
        
        # 执行插入
        cursor.execute(insert_query, values)
        connection.commit()
        
        # 返回成功响应
        return jsonify({
            'message': 'Record inserted successfully',
            'id': cursor.lastrowid
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
    app.run(debug=True, host='0.0.0.0', port=5001)  # 使用 5001 端口，避免冲突
