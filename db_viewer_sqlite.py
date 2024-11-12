import sqlite3

# Создаем подключение к базе данных
conn = sqlite3.connect('audio_features.db')
cursor = conn.cursor()

# Выполняем запрос для получения всех данных из таблицы
cursor.execute("SELECT * FROM audio_features")

# Получаем все строки результата
rows = cursor.fetchall()

# Выводим результат
for row in rows:
    print(row)

# Закрываем соединение
conn.close()
