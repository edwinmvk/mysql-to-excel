from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import mysql.connector
from openpyxl import Workbook
import tempfile
import os
import logging
from werkzeug.utils import secure_filename
import subprocess
import time

app = Flask(__name__)
CORS(app, expose_headers=["Content-Disposition"])


def wait_and_remove_file(file_path, max_attempts=5, delay=1):
    """Attempt to remove a file with multiple retries"""
    for attempt in range(max_attempts):
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
            return True
        except PermissionError:
            if attempt < max_attempts - 1:
                time.sleep(delay)
                continue
            logging.warning(f"Could not remove file {file_path} after {max_attempts} attempts")
            return False
        except Exception as e:
            logging.error(f"Error removing file {file_path}: {str(e)}")
            return False

def execute_sql_file(host, user, password, sql_file_path):
    """Execute the SQL file using mysql command line"""
    try:
        # Construct the mysql command
        command = f'mysql -h {host} -u {user} -p{password} < "{sql_file_path}"'
        
        # Execute the command
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Error executing SQL file: {stderr.decode()}")
        
        # Get the database name from the SQL file
        with open(sql_file_path, 'r') as file:
            content = file.read().lower()
            # Look for CREATE DATABASE or USE statements
            for line in content.split(';'):
                if 'create database' in line:
                    db_name = line.split('`')[1] if '`' in line else line.split()[-1]
                    return db_name.strip(';\n ')
                elif 'use' in line:
                    db_name = line.split('`')[1] if '`' in line else line.split()[-1]
                    return db_name.strip(';\n ')
        
        raise Exception("Could not determine database name from SQL file")
        
    except Exception as e:
        raise Exception(f"Error during SQL file execution: {str(e)}")

def export_to_excel(host, user, password, database):
    """Export MySQL database to Excel"""
    temp_excel_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    
    try:
        # Connect to database
        cnx = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database=database
        )
        
        cursor = cnx.cursor()
        
        # Create workbook
        book = Workbook(write_only=True)
        
        # Get all tables
        cursor.execute(f"SHOW TABLES FROM {database}")
        tables = [table[0] for table in cursor.fetchall()]
        
        if not tables:
            raise Exception("No tables found in database")
        
        # Process each table
        for table in tables:
            # Create sheet
            sheet = book.create_sheet(title=table[:31])
            
            # Get column headers
            cursor.execute(f"SHOW COLUMNS FROM {table}")
            columns = [col[0] for col in cursor.fetchall()]
            sheet.append(columns)
            
            # Get data
            cursor.execute(f"SELECT * FROM {table}")
            for row in cursor:
                sheet.append(row)
        
        # Save workbook
        book.save(temp_excel_file.name)
        return temp_excel_file.name
        
    except Exception as e:
        if os.path.exists(temp_excel_file.name):
            wait_and_remove_file(temp_excel_file.name)
        raise e
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals():
            cnx.close()

@app.route('/convert', methods=['POST'])
def convert_sql_to_excel():
    temp_sql_file = None
    excel_file_path = None
    
    try:
        # Check if file is present in request
        if 'sqlFile' not in request.files:
            return jsonify({'error': 'No SQL file provided'}), 400
        
        file = request.files['sqlFile']
        username = request.form.get('username')
        password = request.form.get('password')
        host = request.form.get('host', '127.0.0.1')
        
        if not username or not password:
            return jsonify({'error': 'Username and password are required'}), 400
        
        # Create temporary SQL file
        temp_sql_file = tempfile.NamedTemporaryFile(delete=False, suffix='.sql')
        file.save(temp_sql_file.name)
        
        try:
            # Execute SQL file and get database name
            database = execute_sql_file(host, username, password, temp_sql_file.name)
            
            # Generate Excel file
            excel_file_path = export_to_excel(host, username, password, database)
            
            # Send file
            return_value = send_file(
                excel_file_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f"{database}_export.xlsx"
            )

            # Schedule cleanup after response is sent
            @return_value.call_on_close
            def cleanup():
                if temp_sql_file and os.path.exists(temp_sql_file.name):
                    wait_and_remove_file(temp_sql_file.name)
                if excel_file_path and os.path.exists(excel_file_path):
                    wait_and_remove_file(excel_file_path)
            
            return return_value
            
        except Exception as e:
            # Clean up files in case of error
            if temp_sql_file and os.path.exists(temp_sql_file.name):
                wait_and_remove_file(temp_sql_file.name)
            if excel_file_path and os.path.exists(excel_file_path):
                wait_and_remove_file(excel_file_path)
            raise e
            
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)