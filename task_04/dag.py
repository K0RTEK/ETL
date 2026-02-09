from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import pendulum


@task()
def extract_and_transform(iot_path: str, load_type: str, days_back: int = 3) -> List[Dict]:
    logger = logging.getLogger("airflow.task")
    df = pd.read_csv(iot_path)

    df_in = df[df['out/in'] == 'In'].copy()
    df_in['noted_date'] = pd.to_datetime(df_in['noted_date'], format='%d-%m-%Y %H:%M').dt.date

    if load_type == "incremental":
        cutoff_date = datetime.now().date() - timedelta(days=days_back)
        df_in = df_in[df_in['noted_date'] >= cutoff_date]
        logger.info(f"Фильтрация для инкрементальной загрузки: дата >= {cutoff_date}")

    if df_in.empty:
        logger.warning("После фильтрации данных не осталось")
        return []

    daily_temp = df_in.groupby('noted_date')['temp'].mean().reset_index()
    daily_temp.rename(columns={'temp': 'avg_temp'}, inplace=True)

    if len(daily_temp) > 20:
        lower_bound = daily_temp['avg_temp'].quantile(0.05)
        upper_bound = daily_temp['avg_temp'].quantile(0.95)
        daily_temp = daily_temp[
            (daily_temp['avg_temp'] >= lower_bound) & (daily_temp['avg_temp'] <= upper_bound)
            ]

    logger.info(f"Подготовлено {len(daily_temp)} записей для загрузки")
    return daily_temp.to_dict(orient="records")


@task()
def load_to_postgres(data: List[Dict], load_type: str):
    if not data:
        logging.info("Нет данных для загрузки")
        return

    hook = PostgresHook(postgres_conn_id="postgres_default")

    hook.run("""
             CREATE TABLE IF NOT EXISTS daily_temperature
             (
                 date
                 DATE
                 PRIMARY
                 KEY,
                 avg_temp
                 NUMERIC
             (
                 5,
                 2
             ),
                 load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                 );
             """)

    if load_type == "incremental":
        dates = [r['noted_date'] for r in data]
        min_d, max_d = min(dates), max(dates)
        hook.run(
            "DELETE FROM daily_temperature WHERE date BETWEEN %s AND %s",
            parameters=(min_d, max_d)
        )

    rows = [(r['noted_date'], round(r['avg_temp'], 2)) for r in data]
    hook.insert_rows(
        table="daily_temperature",
        rows=rows,
        target_fields=["date", "avg_temp"],
        replace=True,
        replace_index=["date"]
    )
    logging.info(f"Загружено {len(rows)} записей. Тип: {load_type}")


@task()
def log_extremes(data: List[Dict]):
    if not data:
        return
    df = pd.DataFrame(data)
    if len(df) < 5:
        return
    hottest = df.nlargest(5, 'avg_temp')
    coldest = df.nsmallest(5, 'avg_temp')
    logger = logging.getLogger("airflow.task")
    logger.info("Топ-5 жарких дней:")
    for _, r in hottest.iterrows():
        logger.info(f"  {r['noted_date']}: {r['avg_temp']:.2f}°C")
    logger.info("Топ-5 холодных дней:")
    for _, r in coldest.iterrows():
        logger.info(f"  {r['noted_date']}: {r['avg_temp']:.2f}°C")


# Полная загрузка
@dag(
    dag_id="iot_full_load",
    schedule_interval="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["iot", "postgres", "full"],
    default_args={"owner": "Kirill Sidorov"}
)
def full_load_dag():
    iot_path = "/opt/airflow/datasets/IOT-temp.csv"
    transformed = extract_and_transform(iot_path, "full")
    loaded = load_to_postgres(transformed, "full")
    log_extremes(transformed)


# Инкрементальная загрузка
@dag(
    dag_id="iot_incremental_load",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["iot", "postgres", "incremental"],
    default_args={"owner": "Kirill Sidorov"}
)
def incremental_load_dag():
    iot_path = "/opt/airflow/datasets/IOT-temp.csv"
    transformed = extract_and_transform(iot_path, "incremental", days_back=3)
    loaded = load_to_postgres(transformed, "incremental")
    log_extremes(transformed)


full_dag_instance = full_load_dag()
incremental_dag_instance = incremental_load_dag()