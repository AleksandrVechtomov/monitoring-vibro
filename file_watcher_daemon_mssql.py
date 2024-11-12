import os
import pyodbc
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


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
            timestamp_str = f"{year}-{month}-{day} {hour}:{minute}:00"
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

            # Извлекаем индекс здоровья мотора
            motor_health_index = round(extract_features(event.src_path), 4)

            # Записываем в базу данных
            save_to_db(timestamp, motor_name, event.src_path, motor_health_index)
            print(f"Файл {event.src_path} обработан. Мотор: {motor_name}, Индекс здоровья мотора: {motor_health_index}")


# Функция для мониторинга директории
def start_watching(directory_to_watch):
    print('ЗАПУЩЕНО!!!')
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

directory_to_watch = 'data'  # Директория для отслеживания
start_watching(directory_to_watch)
