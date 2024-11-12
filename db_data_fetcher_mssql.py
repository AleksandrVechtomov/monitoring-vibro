from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


# Функция для извлечения данных из базы в заданном интервале времени и с фильтрацией по motor_name
def get_data_in_time_range(start_time, end_time, motor_name=None):
    connection_string = (
        'mssql+pyodbc://admin:password@DESKTOP-3CHV1JK/Vibro-test?driver=ODBC+Driver+17+for+SQL+Server'
    )
    engine = create_engine(connection_string)

    # Формируем базовый SQL-запрос с фильтрацией по времени и сортировкой по timestamp
    query = """
    SELECT timestamp, motor_name, file_path, motor_health_index
    FROM motor_health
    WHERE timestamp BETWEEN ? AND ?
    """

    # Если motor_name не None, добавляем фильтрацию по мотору
    if motor_name:
        query += " AND motor_name = ?"
        params = (start_time, end_time, motor_name)
    else:
        params = (start_time, end_time)

    # Добавляем сортировку по столбцу timestamp
    query += " ORDER BY timestamp ASC"

    # Выполняем запрос и получаем результат в DataFrame
    df = pd.read_sql(query, engine, params=params)

    return df


# Задаем параметры для извлечения данных
start_time = '2024-10-09 11:00:00'
end_time = '2025-12-16 20:46:00'
motor_name = 'm1'

# Преобразуем строки с датой в объект datetime
start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

# Извлекаем данные из базы данных
df = get_data_in_time_range(start_time, end_time, motor_name)

# Преобразуем столбец 'timestamp' в тип datetime и сортируем данные по времени
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values(by='timestamp')

# Выводим данные (для проверки)
print(df)

# Устанавливаем границу для зоны отказа
failure_zone = 6.0

# Строим график
plt.figure(figsize=(10, 6))

# Добавляем горизонтальную линию для зоны отказа
plt.axhline(y=failure_zone, color='red', linestyle='--', linewidth=1)

# Строим график индекса здоровья мотора
plt.plot(df['timestamp'], df['motor_health_index'], linestyle='-', color='b')
plt.title(f'Индекс здоровья мотора {motor_name}')
plt.xlabel('Дата')
plt.ylabel('Motor Health Index')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y %H:%M'))
plt.xticks(rotation=90)
plt.grid(True)
plt.tight_layout()
plt.show()
