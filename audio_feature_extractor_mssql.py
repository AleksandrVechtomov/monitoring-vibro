import os
import pyodbc
import time
from datetime import datetime


# Функция извлечения признаков (пример)
def extract_features(audio_file_path):
    import random
    time.sleep(2)  # Эмуляция времени обработки
    return random.random()


# Функция для создания соединения с базой данных Microsoft SQL Server
def create_db_connection():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=DESKTOP-3CHV1JK;'
                          'DATABASE=Vibro-test;'
                          'UID=admin;'
                          'PWD=password')
    return conn


# Функция для записи данных в базу данных (с заменой старых данных, если file_path совпадает)
def save_to_db(timestamp, motor_name, file_path, motor_health_index):
    conn = create_db_connection()
    cursor = conn.cursor()

    # Используем MERGE для обновления записи с тем же file_path
    cursor.execute('''
        MERGE INTO motor_health AS target
        USING (SELECT ? AS file_path, ? AS timestamp, ? AS motor_name, ? AS motor_health_index) AS source
        ON target.file_path = source.file_path
        WHEN MATCHED THEN
            UPDATE SET target.timestamp = source.timestamp, target.motor_name = source.motor_name, target.motor_health_index = source.motor_health_index
        WHEN NOT MATCHED THEN
            INSERT (timestamp, motor_name, file_path, motor_health_index)
            VALUES (source.timestamp, source.motor_name, source.file_path, source.motor_health_index);
    ''', (file_path, timestamp, motor_name, motor_health_index))

    conn.commit()
    conn.close()


# Функция для обработки всех файлов .wav в директории и вложенных папках
def process_all_wav_files(directory_to_watch):
    for root, _, files in os.walk(directory_to_watch):
        for filename in files:
            if filename.endswith(".wav"):
                file_path = os.path.join(root, filename)
                motor_name, timestamp_str = filename.split('_')
                timestamp_str = timestamp_str.split('.')[0]  # Убираем расширение

                # Преобразование временной метки в формат 'YYYY-MM-DD HH:MM:SS'
                year = timestamp_str[:4]
                month = timestamp_str[4:6]
                day = timestamp_str[6:8]
                hour = timestamp_str[8:10]
                minute = timestamp_str[10:12]

                # Формируем стандартный формат времени
                timestamp_str = f"{year}-{month}-{day} {hour}:{minute}:00"
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

                # Извлекаем индекс здоровья мотора
                motor_health_index = round(extract_features(file_path), 4)

                # Записываем в базу данных
                save_to_db(timestamp, motor_name, file_path, motor_health_index)
                print(f"Файл {file_path} обработан. Мотор: {motor_name}, Индекс здоровья мотора: {motor_health_index}")


# Создание таблицы (если она не существует) с уникальностью для file_path
conn = create_db_connection()
cursor = conn.cursor()
cursor.execute('''
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='motor_health' AND xtype='U')
    CREATE TABLE motor_health (
        timestamp DATETIME NOT NULL,
        motor_name NVARCHAR(255) NOT NULL,
        file_path NVARCHAR(255) NOT NULL UNIQUE,
        motor_health_index FLOAT NOT NULL
    )
''')
conn.commit()
conn.close()

# Директория для обработки
directory_to_watch = 'data'

# Обрабатываем все файлы в директории и её вложенных папках
process_all_wav_files(directory_to_watch)
