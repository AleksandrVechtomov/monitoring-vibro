import os
import sqlite3
import time


# Функция извлечения признаков (пример)
def extract_features(audio_file_path):
    import random
    time.sleep(5)  # Эмуляция времени обработки
    return random.random()


# Функция для создания соединения с базой данных
def create_db_connection():
    return sqlite3.connect('audio_features.db')


# Функция для записи данных в базу данных
def save_to_db(timestamp, motor_name, file_path, feature_value):
    conn = create_db_connection()
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO audio_features (timestamp, motor_name, file_path, feature_value)
                      VALUES (?, ?, ?, ?)''', (timestamp, motor_name, file_path, feature_value))
    conn.commit()
    conn.close()


# Функция для обработки всех файлов .wav в директории и вложенных папках
def process_all_wav_files(directory_to_watch):
    # os.walk() позволяет обходить все подкаталоги
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
                timestamp = f"{year}-{month}-{day} {hour}:{minute}:00"

                # Извлекаем признак
                feature_value = extract_features(file_path)

                # Записываем в базу данных
                save_to_db(timestamp, motor_name, file_path, feature_value)
                print(f"Файл {file_path} обработан. Мотор: {motor_name}, Признак: {feature_value:.4f}")


# Создание таблицы (если она не существует)
conn = create_db_connection()
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS audio_features (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    motor_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    feature_value REAL NOT NULL)''')
conn.commit()
conn.close()

# Директория для обработки
directory_to_watch = 'data'

# Обрабатываем все файлы в директории и её вложенных папках
process_all_wav_files(directory_to_watch)
