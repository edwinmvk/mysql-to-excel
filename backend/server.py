from flask import Flask, request, send_file, jsonify, after_this_request
from flask_cors import CORS
import logging
from utils import execute_sql_file, export_to_excel, wait_and_remove_file  # Import the function here
import os
import tempfile
import time

app = Flask(__name__)
CORS(app, expose_headers=["Content-Disposition"])


@app.route('/convert', methods=['POST'])
def convert_sql_to_excel():
    temp_sql_file = None
    excel_file_path = None
    temp_sql_path = None
    
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
        temp_sql_path = temp_sql_file.name
        
        # Save and explicitly close the file
        file.save(temp_sql_path)
        temp_sql_file.close()
        
        try:
            # Execute SQL file and get database name
            database = execute_sql_file(host, username, password, temp_sql_path)
            
            # Generate Excel file
            excel_file_path = export_to_excel(host, username, password, database)
            
            # Register cleanup function to run after response
            @after_this_request
            def cleanup(response):
                def async_cleanup():
                    # Add a small delay to ensure file handles are released
                    time.sleep(0.5)
                    if temp_sql_path:
                        wait_and_remove_file(temp_sql_path)
                    if excel_file_path:
                        wait_and_remove_file(excel_file_path)
                
                # Start cleanup in a separate thread to not block the response
                from threading import Thread
                Thread(target=async_cleanup).start()
                return response
            
            # Create response
            return send_file(
                excel_file_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f"{database}_export.xlsx"
            )
            
        except Exception as e:
            # Clean up files in case of error
            if temp_sql_path:
                wait_and_remove_file(temp_sql_path)
            if excel_file_path:
                wait_and_remove_file(excel_file_path)
            raise e
            
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
