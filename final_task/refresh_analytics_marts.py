from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def refresh_analytics_marts():
    conn = psycopg2.connect(
        host="postgres-app",
        port=5432,
        dbname="app_db",
        user="app_user",
        password="app_password",
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS mart_user_activity;

                    CREATE TABLE mart_user_activity AS
                    WITH sessions_agg AS (
                        SELECT
                            user_id,
                            COUNT(session_id) AS sessions_count,
                            AVG(EXTRACT(EPOCH FROM (end_time - start_time))) AS avg_session_duration_sec
                        FROM user_sessions
                        GROUP BY user_id
                    ),
                    pages_agg AS (
                        SELECT
                            us.user_id,
                            COUNT(DISTINCT p.page) AS unique_pages
                        FROM user_sessions us
                        LEFT JOIN LATERAL unnest(us.pages_visited) AS p(page) ON TRUE
                        GROUP BY us.user_id
                    ),
                    actions_agg AS (
                        SELECT
                            us.user_id,
                            COUNT(DISTINCT a.action) AS unique_actions
                        FROM user_sessions us
                        LEFT JOIN LATERAL unnest(us.actions) AS a(action) ON TRUE
                        GROUP BY us.user_id
                    )
                    SELECT
                        s.user_id,
                        s.sessions_count,
                        s.avg_session_duration_sec,
                        COALESCE(p.unique_pages, 0) AS unique_pages,
                        COALESCE(a.unique_actions, 0) AS unique_actions
                    FROM sessions_agg s
                    LEFT JOIN pages_agg p USING(user_id)
                    LEFT JOIN actions_agg a USING(user_id);
                """)

                cur.execute("""
                    DROP TABLE IF EXISTS mart_support_statistics;

                    CREATE TABLE mart_support_statistics AS
                    SELECT
                        issue_type,
                        status,
                        COUNT(*) AS tickets_count,
                        AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) AS avg_resolution_time_sec
                    FROM support_tickets
                    GROUP BY issue_type, status;
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mart_user_activity_user_id
                    ON mart_user_activity(user_id);
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mart_support_statistics_issue_type
                    ON mart_support_statistics(issue_type);
                """)
    finally:
        conn.close()


with DAG(
    dag_id="refresh_analytics_marts",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["analytics", "marts"],
) as dag:
    refresh_task = PythonOperator(
        task_id="refresh_analytics_marts_task",
        python_callable=refresh_analytics_marts,
    )