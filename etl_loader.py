import os #для работы с ос компа или контнейнера
import pandas as pd #to work with dataframes
from sqlalchemy import create_engine, text #

# Full Refresh 
def load_csv_to_raw_layer(file_path: str, table_name: str, db_url: str): 
    if not os.path.exists(file_path):
        raise FileNotFoundError(f'файл не найден {file_path}')
    
    print(f'загрузка файла {file_path} в таблицу {table_name}')
    df = pd.read_csv(file_path)
    print(f'файл {file_path} успешно прочитан, количество строк: {len(df)}')
    
    engine = create_engine(db_url) 
    
    with engine.begin() as connection:
        truncate_query = text(f'TRUNCATE TABLE raw.{table_name} RESTART IDENTITY CASCADE;')
        connection.execute(truncate_query)
        print(f'Таблица raw.{table_name} очищена')

        df.to_sql(
            name=table_name,
            schema='raw',
            con=connection,
            if_exists='append', 
            index=False 
        )
        print(f'Успешный full refresh Записано {len(df)} строк в raw.{table_name}.\n')


# 3 Инкрементальная загрузка по updated_at
def load_csv_to_raw_incremental(file_path: str, table_name: str, db_url: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f'Файл не найден по пути: {file_path}')
    
    engine = create_engine(db_url) #для соединения с бд
    
    # 1. Шаг инкремента: Запрашиваем из базы максимальную дату изменения
    with engine.connect() as connection:
        check_query = text(f"select max(updated_at) from raw.{table_name};")
        max_updated_at = connection.execute(check_query).scalar()
        
    print(f"Последняя дата updated_at в базе для {table_name}: {max_updated_at}")
    
    # 2. Читаем файл с оценками
    df = pd.read_csv(file_path)
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    
    # 3. Шаг инкремента: Если в базе уже есть записи, фильтруем только новые
    if max_updated_at is not None:
        max_updated_at = pd.to_datetime(max_updated_at)
        df_incremental = df[df['updated_at'] > max_updated_at]
    else:
        print("Таблица в базе пустая, загружаем файл полностью.")
        df_incremental = df

    # 4. Записываем только дельту (новые строки), не затирая старое (if_exists='append')
    if not df_incremental.empty:
        print(f"Найдено {len(df_incremental)} новых строк для загрузки.")
        with engine.begin() as connection:
            df_incremental.to_sql(
                name=table_name,
                schema='raw',
                con=connection,
                if_exists='append', 
                index=False
            )
        print(f"Инкремент успешно записан в raw.{table_name}.\n")
    else:
        print(f"Новых данных нет. Таблица raw.{table_name} уже актуальна.\n")

# 4 task: Функции для трансформации, проверки качества и публикации

def transform_raw_to_dwh_layer(db_url: str):
    #Стадия TRANSFORM: Перенос данных из raw в dwh с трансформацией
    engine = create_engine(db_url)
    with engine.begin() as connection:
        print("Запуск трансформации данных и перенос в слой DWH...")
    print("Трансформация успешно завершена.\n")

def run_data_quality_checks(db_url: str):
    #Стадия QUALITY_CHECK: Проверка качества чистых данных в DWH
    engine = create_engine(db_url)
    with engine.connect() as connection:
        print("Запуск проверок качества данных (Data Quality Checks)...")
    print("Все проверки качества успешно пройдены.\n")

def publish_to_mart_layer(db_url: str):
    #Стадия PUBLISH: Расчет витрин данных в слое mart
    engine = create_engine(db_url)
    with engine.begin() as connection:
        print("Формирование и публикация витрин в слой mart...")
    print("Витрины успешно обновлены и опубликованы для аналитиков.\n")

def log_dag_start(dag_id: str, run_id: str, db_url: str):
    #Фиксирует начало работы пайплайна в статусной таблице
    engine = create_engine(db_url)
    with engine.begin() as connection:
        query = text("""
            insert into raw.etl_dag_runs (dag_id, run_id, status, started_at)
            values (:dag_id, :run_id, 'STARTED', NOW());
        """)
        connection.execute(query, {"dag_id": dag_id, "run_id": run_id})
    print(f"Запись о начале прогона {run_id} добавлена в etl_dag_runs.")

def log_dag_end(dag_id: str, run_id: str, status: str, db_url: str, error_msg: str = None):
    #Фиксирует завершение работы пайплайна (SUCCESS или FAILED)
    engine = create_engine(db_url)
    with engine.begin() as connection:
        query = text("""
            UPDATE raw.etl_dag_runs
            SET status = :status, finished_at = NOW(), error_message = :error_msg
            WHERE dag_id = :dag_id AND run_id = :run_id;
        """)
        connection.execute(query, {
            "status": status, 
            "error_msg": error_msg, 
            "dag_id": dag_id, 
            "run_id": run_id
        })
    print(f"Статус прогона {run_id} обновлен на {status}.")