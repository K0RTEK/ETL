import os
import random
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_seed_mongo():
    from pymongo import MongoClient

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://kirill:5567@mongodb:27017/?authSource=admin"
    )
    mongo_db_name = 'etl'

    num_sessions = 200
    num_events = 300
    num_tickets = 80
    num_recommendations = 50
    num_reviews = 120

    batch_size = 1000

    # =========================
    # LOOKUPS
    # =========================
    pages = [
        "/home",
        "/catalog",
        "/products",
        "/cart",
        "/checkout",
        "/profile",
        "/favorites",
        "/orders",
        "/support",
    ]

    event_types = [
        "click",
        "view",
        "login",
        "logout",
        "add_to_cart",
        "purchase",
        "search",
    ]

    actions_pool = [
        "login",
        "view_product",
        "add_to_cart",
        "remove_from_cart",
        "checkout",
        "logout",
        "search",
    ]

    device_types = ["mobile", "desktop", "tablet"]
    operating_systems = ["iOS", "Android", "Windows", "macOS", "Linux"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]

    ticket_statuses = ["open", "in_progress", "closed"]
    issue_types = ["payment", "delivery", "account", "refund", "technical"]

    moderation_statuses = ["pending", "approved", "rejected"]
    flags_pool = ["contains_images", "spam_suspected", "profanity_check", "duplicate_review"]

    user_messages = [
        "Не могу оплатить заказ.",
        "Заказ не отображается в профиле.",
        "Деньги списались дважды.",
        "Не приходит код подтверждения.",
        "Не могу войти в аккаунт.",
        "Когда будет доставка?",
        "Хочу оформить возврат.",
    ]

    support_messages = [
        "Пожалуйста, уточните номер заказа.",
        "Уточните, каким способом вы пытались оплатить.",
        "Проверьте, пожалуйста, папку спам.",
        "Мы передали запрос в технический отдел.",
        "Ваше обращение рассматривается.",
        "Возврат будет обработан в течение 3 рабочих дней.",
    ]

    review_texts = [
        "Отличный товар, работает как нужно!",
        "Качество хорошее, рекомендую.",
        "В целом нормально, но есть мелкие недостатки.",
        "Не оправдал ожиданий.",
        "Очень понравилось, куплю еще.",
        "Доставка быстрая, товар соответствует описанию.",
        "Средний вариант за свои деньги.",
    ]

    # =========================
    # HELPERS
    # =========================
    def random_datetime(start_days_ago=90, end_days_ago=0):
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=start_days_ago)
        end = now - timedelta(days=end_days_ago)
        delta = end - start
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return start + timedelta(seconds=random_seconds)

    def random_user_id():
        return f"user_{random.randint(100, 999)}"

    def random_product_id():
        return f"prod_{random.randint(100, 999)}"

    def random_pages():
        count = random.randint(2, 6)
        result = [random.choice(pages)]
        for _ in range(count - 1):
            if random.random() < 0.4:
                result.append(f"/products/{random.randint(1, 200)}")
            else:
                result.append(random.choice(pages))
        return result

    def random_actions():
        count = random.randint(2, 6)
        return random.sample(actions_pool, k=min(count, len(actions_pool)))

    def random_device():
        return {
            "type": random.choice(device_types),
            "os": random.choice(operating_systems),
            "browser": random.choice(browsers),
        }

    def random_flags():
        if random.random() < 0.4:
            return []
        count = random.randint(1, min(2, len(flags_pool)))
        return random.sample(flags_pool, count)

    def chunked(data, size):
        for i in range(0, len(data), size):
            yield data[i:i + size]

    def insert_in_batches(collection, docs, size):
        total = 0
        for batch in chunked(docs, size):
            if batch:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
        return total

    # =========================
    # GENERATORS
    # =========================
    def generate_user_sessions(n):
        docs = []
        for i in range(1, n + 1):
            start_time = random_datetime()
            end_time = start_time + timedelta(minutes=random.randint(3, 90))

            docs.append({
                "session_id": f"sess_{i:04d}",
                "user_id": random_user_id(),
                "start_time": start_time,
                "end_time": end_time,
                "pages_visited": random_pages(),
                "device": random_device(),
                "actions": random_actions(),
            })
        return docs

    def generate_event_logs(n):
        docs = []
        for i in range(1, n + 1):
            event_type = random.choice(event_types)

            if event_type in ["click", "view"]:
                details = {
                    "page": random.choice(pages + [f"/products/{random.randint(1, 200)}"]),
                    "element": random.choice(["button", "link", "image", "card", "menu_item"]),
                }
            elif event_type == "search":
                details = {
                    "query": random.choice(["phone", "laptop", "headphones", "mouse", "keyboard"]),
                    "results_count": random.randint(0, 120),
                }
            elif event_type == "add_to_cart":
                details = {
                    "product_id": random_product_id(),
                    "quantity": random.randint(1, 3),
                }
            elif event_type == "purchase":
                details = {
                    "order_id": f"order_{random.randint(1000, 9999)}",
                    "amount": round(random.uniform(10, 500), 2),
                    "currency": "RUB",
                }
            else:
                details = {
                    "info": f"{event_type} event recorded",
                }

            docs.append({
                "event_id": f"evt_{i:05d}",
                "timestamp": random_datetime(),
                "event_type": event_type,
                "details": details,
            })
        return docs

    def generate_support_tickets(n):
        docs = []
        for i in range(1, n + 1):
            created_at = random_datetime()
            msg_count = random.randint(1, 4)

            messages = [{
                "sender": "user",
                "message": random.choice(user_messages),
                "timestamp": created_at,
            }]

            current_time = created_at
            for j in range(1, msg_count):
                current_time += timedelta(minutes=random.randint(10, 180))
                sender = "support" if j % 2 == 1 else "user"
                text = random.choice(support_messages if sender == "support" else user_messages)
                messages.append({
                    "sender": sender,
                    "message": text,
                    "timestamp": current_time,
                })

            docs.append({
                "ticket_id": f"ticket_{i:04d}",
                "user_id": random_user_id(),
                "status": random.choice(ticket_statuses),
                "issue_type": random.choice(issue_types),
                "messages": messages,
                "created_at": created_at,
                "updated_at": messages[-1]["timestamp"],
            })
        return docs

    def generate_user_recommendations(n):
        docs = []
        used_users = set()

        while len(docs) < n:
            user_id = random_user_id()
            if user_id in used_users:
                continue
            used_users.add(user_id)

            docs.append({
                "user_id": user_id,
                "recommended_products": random.sample(
                    [f"prod_{i}" for i in range(100, 500)],
                    k=random.randint(3, 6),
                ),
                "last_updated": random_datetime(),
            })
        return docs

    def generate_moderation_queue(n):
        docs = []
        for i in range(1, n + 1):
            docs.append({
                "review_id": f"rev_{i:04d}",
                "user_id": random_user_id(),
                "product_id": random_product_id(),
                "review_text": random.choice(review_texts),
                "rating": random.randint(1, 5),
                "moderation_status": random.choice(moderation_statuses),
                "flags": random_flags(),
                "submitted_at": random_datetime(),
            })
        return docs

    # =========================
    # MONGO WRITE
    # =========================
    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]

    datasets = {
        "UserSessions": generate_user_sessions(num_sessions),
        "EventLogs": generate_event_logs(num_events),
        "SupportTickets": generate_support_tickets(num_tickets),
        "UserRecommendations": generate_user_recommendations(num_recommendations),
        "ModerationQueue": generate_moderation_queue(num_reviews),
    }

    try:
        for collection_name, docs in datasets.items():
            collection = db[collection_name]

            # full refresh коллекции
            collection.delete_many({})

            inserted = insert_in_batches(collection, docs, batch_size)
            print(f"[{collection_name}] inserted {inserted} documents")

        # индексы
        db["UserSessions"].create_index("session_id", unique=True)
        db["EventLogs"].create_index("event_id", unique=True)
        db["SupportTickets"].create_index("ticket_id", unique=True)
        db["UserRecommendations"].create_index("user_id", unique=True)
        db["ModerationQueue"].create_index("review_id", unique=True)

        print("Mongo seed completed successfully")

    finally:
        client.close()


with DAG(
    dag_id="mongo_seed_data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mongo", "seed", "test-data"],
) as dag:

    seed_mongo_task = PythonOperator(
        task_id="seed_mongo_data",
        python_callable=run_seed_mongo,
    )