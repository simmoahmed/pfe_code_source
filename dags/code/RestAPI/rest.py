from flask import Flask, jsonify
import mysql.connector

app = Flask(__name__)

# MySQL connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'db',
    'port': '3306',
}

@app.route('/employees', methods=['GET'])
def get_employees():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM employee')
        employees = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(employees)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
