CREATE TABLE mart_user_activity AS
SELECT
    us.user_id,
    COUNT(us.session_id) AS sessions_count,
    AVG(EXTRACT(EPOCH FROM (us.end_time - us.start_time))) AS avg_session_duration_sec,

    COUNT(DISTINCT pv.page) AS unique_pages_visited,
    COUNT(DISTINCT act.action) AS unique_actions

FROM user_sessions us
LEFT JOIN LATERAL unnest(us.pages_visited) AS pv(page) ON TRUE
LEFT JOIN LATERAL unnest(us.actions) AS act(action) ON TRUE
GROUP BY us.user_id;



CREATE TABLE mart_open_tickets AS
SELECT
    ticket_id,
    user_id,
    issue_type,
    created_at,
    NOW() - created_at AS open_duration
FROM support_tickets
WHERE status != 'closed';