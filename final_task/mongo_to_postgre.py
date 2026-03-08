import os
from datetime import datetime
from typing import Dict, List, Tuple, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2 import connect
from psycopg2.extras import execute_values, Json
from pymongo import MongoClient

MONGO_URI = "mongodb://kirill:5567@mongodb:27017/admin?authSource=admin"
MONGO_DB = 'etl'

PG_HOST = 'postgres-app'
PG_PORT = 5432
PG_DB = "app_db"
PG_USER = "app_user"
PG_PASSWORD = "app_password"

BATCH_SIZE = 1000

def make_json_serializable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {k: make_json_serializable(v) for k, v in value.items()}

    if isinstance(value, list):
        return [make_json_serializable(v) for v in value]

    return value


def to_datetime(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    return [str(value)]


def normalize_json(value: Any) -> Any:
    if value is None:
        return {}
    return make_json_serializable(value)


def chunked_cursor(cursor, batch_size: int):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def transform_user_sessions(doc: Dict[str, Any]) -> Tuple:
    return (
        doc.get("session_id"),
        doc.get("user_id"),
        to_datetime(doc.get("start_time")),
        to_datetime(doc.get("end_time")),
        normalize_list(doc.get("pages_visited")),
        Json(normalize_json(doc.get("device"))),
        normalize_list(doc.get("actions")),
    )


def transform_event_logs(doc: Dict[str, Any]) -> Tuple:
    return (
        doc.get("event_id"),
        to_datetime(doc.get("timestamp")),
        doc.get("event_type"),
        Json(normalize_json(doc.get("details"))),
    )


def transform_support_tickets(doc: Dict[str, Any]) -> Tuple:
    return (
        doc.get("ticket_id"),
        doc.get("user_id"),
        doc.get("status"),
        doc.get("issue_type"),
        Json(normalize_json(doc.get("messages"))),
        to_datetime(doc.get("created_at")),
        to_datetime(doc.get("updated_at")),
    )


def transform_user_recommendations(doc: Dict[str, Any]) -> Tuple:
    return (
        doc.get("user_id"),
        normalize_list(doc.get("recommended_products")),
        to_datetime(doc.get("last_updated")),
    )


def transform_moderation_queue(doc: Dict[str, Any]) -> Tuple:
    rating = doc.get("rating")
    if rating is not None:
        rating = int(rating)

    return (
        doc.get("review_id"),
        doc.get("user_id"),
        doc.get("product_id"),
        doc.get("review_text"),
        rating,
        doc.get("moderation_status"),
        normalize_list(doc.get("flags")),
        to_datetime(doc.get("submitted_at")),
    )


TABLES = [
    {
        "mongo_collection": "UserSessions",
        "pg_table": "user_sessions",
        "staging_table": "staging_user_sessions",
        "ddl": """
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ NOT NULL,
                pages_visited TEXT[] NOT NULL,
                device JSONB NOT NULL,
                actions TEXT[] NOT NULL
            );
        """,
        "staging_ddl": """
            CREATE TEMP TABLE staging_user_sessions (
                session_id TEXT,
                user_id TEXT,
                start_time TIMESTAMPTZ,
                end_time TIMESTAMPTZ,
                pages_visited TEXT[],
                device JSONB,
                actions TEXT[]
            ) ON COMMIT DROP;
        """,
        "columns": [
            "session_id",
            "user_id",
            "start_time",
            "end_time",
            "pages_visited",
            "device",
            "actions",
        ],
        "transform": transform_user_sessions,
    },
    {
        "mongo_collection": "EventLogs",
        "pg_table": "event_logs",
        "staging_table": "staging_event_logs",
        "ddl": """
            CREATE TABLE IF NOT EXISTS event_logs (
                event_id TEXT PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                event_type TEXT NOT NULL,
                details JSONB NOT NULL
            );
        """,
        "staging_ddl": """
            CREATE TEMP TABLE staging_event_logs (
                event_id TEXT,
                timestamp TIMESTAMPTZ,
                event_type TEXT,
                details JSONB
            ) ON COMMIT DROP;
        """,
        "columns": [
            "event_id",
            "timestamp",
            "event_type",
            "details",
        ],
        "transform": transform_event_logs,
    },
    {
        "mongo_collection": "SupportTickets",
        "pg_table": "support_tickets",
        "staging_table": "staging_support_tickets",
        "ddl": """
            CREATE TABLE IF NOT EXISTS support_tickets (
                ticket_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                issue_type TEXT NOT NULL,
                messages JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
        """,
        "staging_ddl": """
            CREATE TEMP TABLE staging_support_tickets (
                ticket_id TEXT,
                user_id TEXT,
                status TEXT,
                issue_type TEXT,
                messages JSONB,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            ) ON COMMIT DROP;
        """,
        "columns": [
            "ticket_id",
            "user_id",
            "status",
            "issue_type",
            "messages",
            "created_at",
            "updated_at",
        ],
        "transform": transform_support_tickets,
    },
    {
        "mongo_collection": "UserRecommendations",
        "pg_table": "user_recommendations",
        "staging_table": "staging_user_recommendations",
        "ddl": """
            CREATE TABLE IF NOT EXISTS user_recommendations (
                user_id TEXT PRIMARY KEY,
                recommended_products TEXT[] NOT NULL,
                last_updated TIMESTAMPTZ NOT NULL
            );
        """,
        "staging_ddl": """
            CREATE TEMP TABLE staging_user_recommendations (
                user_id TEXT,
                recommended_products TEXT[],
                last_updated TIMESTAMPTZ
            ) ON COMMIT DROP;
        """,
        "columns": [
            "user_id",
            "recommended_products",
            "last_updated",
        ],
        "transform": transform_user_recommendations,
    },
    {
        "mongo_collection": "ModerationQueue",
        "pg_table": "moderation_queue",
        "staging_table": "staging_moderation_queue",
        "ddl": """
            CREATE TABLE IF NOT EXISTS moderation_queue (
                review_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                review_text TEXT NOT NULL,
                rating INTEGER NOT NULL,
                moderation_status TEXT NOT NULL,
                flags TEXT[] NOT NULL,
                submitted_at TIMESTAMPTZ NOT NULL
            );
        """,
        "staging_ddl": """
            CREATE TEMP TABLE staging_moderation_queue (
                review_id TEXT,
                user_id TEXT,
                product_id TEXT,
                review_text TEXT,
                rating INTEGER,
                moderation_status TEXT,
                flags TEXT[],
                submitted_at TIMESTAMPTZ
            ) ON COMMIT DROP;
        """,
        "columns": [
            "review_id",
            "user_id",
            "product_id",
            "review_text",
            "rating",
            "moderation_status",
            "flags",
            "submitted_at",
        ],
        "transform": transform_moderation_queue,
    },
]


def ensure_target_tables(pg_conn) -> None:
    with pg_conn.cursor() as cur:
        for table in TABLES:
            cur.execute(table["ddl"])
    pg_conn.commit()


def load_collection_full_refresh(
    mongo_db,
    pg_conn,
    mongo_collection_name: str,
    pg_table: str,
    staging_table: str,
    staging_ddl: str,
    columns: List[str],
    transform,
) -> None:
    collection = mongo_db[mongo_collection_name]

    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(staging_ddl)

            insert_sql = f"""
                INSERT INTO {staging_table} ({", ".join(columns)})
                VALUES %s
            """

            cursor = collection.find({}, no_cursor_timeout=True).batch_size(BATCH_SIZE)

            total_rows = 0
            try:
                for docs_batch in chunked_cursor(cursor, BATCH_SIZE):
                    rows = [transform(doc) for doc in docs_batch]
                    execute_values(cur, insert_sql, rows, page_size=BATCH_SIZE)
                    total_rows += len(rows)
                    print(f"[{mongo_collection_name}] loaded into staging: {total_rows}")
            finally:
                cursor.close()

            cur.execute(f"TRUNCATE TABLE {pg_table};")
            cur.execute(
                f"""
                INSERT INTO {pg_table} ({", ".join(columns)})
                SELECT {", ".join(columns)}
                FROM {staging_table};
                """
            )

            print(f"[{mongo_collection_name}] full refresh completed: {total_rows} rows")


def main():
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]

    pg_conn = connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    try:
        ensure_target_tables(pg_conn)

        for table in TABLES:
            load_collection_full_refresh(
                mongo_db=mongo_db,
                pg_conn=pg_conn,
                mongo_collection_name=table["mongo_collection"],
                pg_table=table["pg_table"],
                staging_table=table["staging_table"],
                staging_ddl=table["staging_ddl"],
                columns=table["columns"],
                transform=table["transform"],
            )

        print("All tables refreshed successfully.")

    finally:
        pg_conn.close()
        mongo_client.close()

with DAG(
    dag_id="mongo_to_postgres_full_refresh",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    sync_task = PythonOperator(
        task_id="sync_mongo_to_postgres",
        python_callable=main,
    )

if __name__ == "__main__":
    main()