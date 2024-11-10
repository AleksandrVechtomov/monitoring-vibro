import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# Функция для извлечения данных из базы в заданном интервале времени и с фильтрацией по motor_name
def get_data_in_time_range(start_time, end_time, motor_name=None):
    db_path = 'C:\\Users\\AI-WIN\\PycharmProjects\\Monitor_vibro\\audio_features.db'
    conn = sqlite3.connect(db_path)  # Подключаемся к базе данных

    # Формируем базовый SQL-запрос с фильтрацией по времени
    query = """
    SELECT timestamp, motor_name, file_path, feature_value
    FROM audio_features
    WHERE timestamp BETWEEN ? AND ?
    """

    # Если motor_name не None, добавляем фильтрацию по мотору
    if motor_name:
        query += " AND motor_name = ?"
        params = (start_time, end_time, motor_name)
    else:
        params = (start_time, end_time)

    # Выполняем запрос
    df = pd.read_sql_query(query, conn, params=params)

    # Закрываем соединение с базой данных
    conn.close()

    return df


start_time = '2024-10-09 11:00:00'
end_time = '2024-12-16 20:46:00'
motor_name = 'm2'

df = get_data_in_time_range(start_time, end_time, motor_name)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values(by='timestamp')
print(df)

failure_zone = 0.45

plt.figure(figsize=(10, 6))

plt.axhline(y=failure_zone, color='red', linestyle='--', linewidth=1)

plt.plot(df['timestamp'], df['feature_value'], marker='o', linestyle='-', color='b')
plt.title(f'Состояние двигателя {motor_name}')
plt.xlabel('Время')
plt.ylabel('Feature Value')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y %H:%M'))
plt.xticks(rotation=90)
plt.grid(True)

plt.tight_layout()
plt.show()
