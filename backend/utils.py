import mysql.connector
from openpyxl import Workbook
import tempfile
import os
import logging
import subprocess
import time

def wait_and_remove_file(file_path, max_attempts=5, delay=1):
    """Attempt to remove a file with multiple retries"""
    if not file_path or not os.path.exists(file_path):
        return True
        
    for attempt in range(max_attempts):
        try:
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
