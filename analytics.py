import duckdb
import pandas as pd

con = duckdb.connect("wiki.db")

avg_edit_size = con.execute("""
SELECT
    wiki,
    AVG(ABS(change_size)) AS avg_edit_size
FROM wiki_events
GROUP BY wiki
ORDER BY avg_edit_size DESC
""").df()
print(avg_edit_size.head(10))

active_wikis = con.execute("""
SELECT
    wiki,
    COUNT(*) AS edit_count
FROM wiki_events
GROUP BY wiki
ORDER BY edit_count DESC
""").df()
print(active_wikis.head(10))


char_velocity = con.execute("""
WITH per_minute AS (
    SELECT
        DATE_TRUNC('minute', event_time) AS minute,
        SUM(ABS(change_size)) AS chars_changed
    FROM wiki_events
    GROUP BY minute
)
SELECT
    minute,
    chars_changed,
    AVG(chars_changed) OVER (
        ORDER BY minute
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_min_avg
FROM per_minute
ORDER BY minute DESC
LIMIT 10
""").df()
print(char_velocity)
