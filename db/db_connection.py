import psycopg2
import logging
from db.db_config import DB_CONFIGS


def get_connection(db_key: str):
    if db_key not in DB_CONFIGS:
        raise ValueError(f"No config found for database: {db_key}")
    return psycopg2.connect(**DB_CONFIGS[db_key])  # Подключаемся к базе через конфиг
    logging.info('Успешное подключение к БД')



# logging.basicConfig(level=logging.INFO)

# def check_connection(db_key: str):
#     # Проверка наличия конфигурации для базы данных
#     if db_key not in DB_CONFIGS:
#         logging.error(f"No config found for database: {db_key}")
#         return

#     try:
#         # Подключаемся к базе данных
#         connection = psycopg2.connect(**DB_CONFIGS[db_key])
#         logging.info(f"Успешное подключение к базе данных '{db_key}'.")

#         # Выполняем простой запрос, чтобы проверить соединение
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT version();")
#             version = cursor.fetchone()
#             logging.info(f"Версия базы данных: {version[0]}")
    
#     except psycopg2.Error as e:
#         logging.error(f"Ошибка при подключении к базе данных '{db_key}': {e}")
    
#     finally:
#         if 'connection' in locals():
#             connection.close()
#             logging.info("Соединение закрыто.")

# if __name__ == "__main__":
    # check_connection("qamqor")  # Здесь укажите ключ базы данных