import pyodbc

# Функция для создания подключения к базе данных MSSQL
def create_db_connection():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=DESKTOP-3CHV1JK;'
                          'DATABASE=Vibro-test;'
                          'UID=admin;'
                          'PWD=password')
    return conn

# Создаем подключение к базе данных
conn = create_db_connection()
cursor = conn.cursor()

# Выполняем запрос для получения всех данных из таблицы motor_health
cursor.execute("SELECT * FROM motor_health")

# Получаем все строки результата
rows = cursor.fetchall()

# Выводим результат
for row in rows:
    print(row)

# Закрываем соединение
conn.close()
