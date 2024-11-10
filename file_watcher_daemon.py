import os
import sqlite3
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# Функция извлечения признаков (пример)
def extract_features(audio_file_path):
    import random
    time.sleep(5)
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


# Обработчик событий для новых файлов
class AudioFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".wav"):  # Фильтрация по типу файла
            filename = os.path.basename(event.src_path)
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
            feature_value = extract_features(event.src_path)

            # Записываем в базу данных
            save_to_db(timestamp, motor_name, event.src_path, feature_value)
            print(f"Файл {event.src_path} обработан. Мотор: {motor_name}, Признак: {feature_value:.4f}")


# Функция для мониторинга директории
def start_watching(directory_to_watch):
    event_handler = AudioFileHandler()
    observer = Observer()
    observer.schedule(event_handler, directory_to_watch, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


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

directory_to_watch = 'data'  # Директория для отслеживания
start_watching(directory_to_watch)
