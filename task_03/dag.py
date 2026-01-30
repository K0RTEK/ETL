from datetime import datetime

from airflow.decorators import dag, task
import pandas as pd
import logging


@task()
def df_transform():
    logger = logging.getLogger("airflow.task")

    df = pd.read_csv("/opt/airflow/datasets/IOT-temp.csv")

    df_in = df[df['out/in'] == 'In'].copy()

    df_in['noted_date'] = pd.to_datetime(df_in['noted_date'], format='%d-%m-%Y %H:%M').dt.date

    daily_temp = df_in.groupby('noted_date')['temp'].mean().reset_index()
    daily_temp.rename(columns={'temp': 'avg_temp'}, inplace=True)

    lower_bound = daily_temp['avg_temp'].quantile(0.05)
    upper_bound = daily_temp['avg_temp'].quantile(0.95)
    daily_temp_cleaned = daily_temp[
        (daily_temp['avg_temp'] >= lower_bound) & (daily_temp['avg_temp'] <= upper_bound)
    ]

    hottest_days = daily_temp_cleaned.nlargest(5, 'avg_temp')
    coldest_days = daily_temp_cleaned.nsmallest(5, 'avg_temp')

    logger.info("Топ-5 самых жарких дней:")
    for _, row in hottest_days.iterrows():
        logger.info(f"  Дата: {row['noted_date']}, Средняя температура: {row['avg_temp']:.2f}°C")

    logger.info("Топ-5 самых холодных дней:")
    for _, row in coldest_days.iterrows():
        logger.info(f"  Дата: {row['noted_date']}, Средняя температура: {row['avg_temp']:.2f}°C")

    return {
        "hottest": hottest_days.to_dict(orient="records"),
        "coldest": coldest_days.to_dict(orient="records")
    }


@dag(
    dag_id="task_1",
    default_args={"owner": "Kirill"},
    schedule_interval=None,
    start_date=datetime(2026, 1, 23),
)
def main():
    task1 = df_transform()

    task1


main()
