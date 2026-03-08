# ETL Final Task

Итоговое задание по модулю **ETL-процессы**.

Реализован ETL pipeline:

**MongoDB → PostgreSQL → аналитические витрины**

Все сервисы запускались локально в Docker

---

# Структура проекта

```
ETL
└── final_task
    ├── schemas
    │   ├── eventlogs.json
    │   ├── moderationqueue.json
    │   ├── supporttickets.json
    │   ├── userrecommendations.json
    │   └── usersessions.json
    │
    ├── mongo_data_generator.py
    ├── mongo_to_postgre.py
    ├── refresh_analytics_marts.py
    └── README.md
```

---

# Источник данных (MongoDB)

В MongoDB генерируются следующие коллекции:

| Коллекция           | Описание                  |
| ------------------- | ------------------------- |
| UserSessions        | пользовательские сессии   |
| EventLogs           | события пользователей     |
| SupportTickets      | обращения в поддержку     |
| UserRecommendations | рекомендации товаров      |
| ModerationQueue     | очередь модерации отзывов |

JSON-схемы документов находятся в папке:

```
schemas/
```

---

# DAG 1 — Генерация данных

Файл:

```
mongo_data_generator.py
```

DAG **mongo_seed_data** генерирует тестовые данные и записывает их в MongoDB

Пример документа:

```json
{
  "session_id": "sess_001",
  "user_id": "user_123",
  "start_time": "2024-01-10T09:00:00Z",
  "end_time": "2024-01-10T09:30:00Z",
  "pages_visited": ["/home", "/products", "/cart"],
  "device": {"type": "mobile"},
  "actions": ["login", "view_product", "add_to_cart"]
}
```

---

# DAG 2 — Репликация данных

Файл:

```
mongo_to_postgre.py
```

DAG **mongo_to_postgres_full_refresh** выполняет загрузку данных из MongoDB в PostgreSQL

Этапы:

1. чтение данных из MongoDB
2. преобразование структуры данных
3. батчевая запись в PostgreSQL

Создаются таблицы:

```
user_sessions
event_logs
support_tickets
user_recommendations
moderation_queue
```

Данные приводятся к структуре, удобной для дальнейшей аналитики

---

# DAG 3 — Построение аналитических витрин

Файл:

```
refresh_analytics_marts.py
```

DAG **refresh_analytics_marts** пересчитывает аналитические витрины в PostgreSQL

---

# Аналитические витрины

## mart_user_activity

Витрина активности пользователей.

Метрики:

* количество сессий
* средняя длительность сессии
* количество уникальных страниц
* количество действий пользователя

---

## mart_support_statistics

Витрина эффективности работы поддержки.

Метрики:

* количество обращений
* статус тикетов
* тип проблемы
* среднее время решения

---

# Pipeline

Общий pipeline выглядит следующим образом:

```
mongo_seed_data
        ↓
mongo_to_postgres_full_refresh
        ↓
refresh_analytics_marts
```

---

# Примеры аналитических запросов

Активность пользователей:

```sql
SELECT *
FROM mart_user_activity
ORDER BY sessions_count DESC;
```

Статистика поддержки:

```sql
SELECT *
FROM mart_support_statistics;
```
