import psycopg2

def postgres_connection():
    connection = psycopg2.connect(host='localhost',
                                database='EmployeeRecord',
                                port=5433,
                                user='postgres',
                                password='7860')
    return connection





