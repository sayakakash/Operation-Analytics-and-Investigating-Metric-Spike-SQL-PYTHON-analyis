CREATE TABLE job_data (
  ds date,
  job_id int,
  actor_id int,
  event varchar(10),
  language varchar(10),
  time_spent int,
  org varchar(1)
);

INSERT INTO job_data VALUES 
('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');


SELECT*FROM job_data

--Number of jobs reviewed: Amount of jobs reviewed over time .
--task: Calculate the number of jobs reviewed per hour per day for November 2020?

SELECT CAST(ds AS date) AS date, DATEPART(hour, CAST(ds AS datetime)) AS hour, COUNT(*) AS jobs_reviewed, SUM(time_spent)/3600.0 AS total_hours
FROM job_data
WHERE ds >= '2020-11-01' AND ds < '2020-12-01'
GROUP BY CAST(ds AS date), DATEPART(hour, CAST(ds AS datetime))
ORDER BY CAST(ds AS date), DATEPART(hour, CAST(ds AS datetime))


--Throughput: It is the no. of events happening per second.
--task: Calculate 7 day rolling average of throughput? For throughput, do you prefer daily metric or 7-day rolling and why?

--7 day rolling average of throughput:method 1

SELECT 
  ds, 
  AVG(SUM(time_spent)) OVER(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_throughput 
FROM 
  job_data 
GROUP BY 
  ds 
ORDER BY 
  ds ASC;



--Throughput: It is the no. of events happening per second.
--task: Calculate 7 day rolling average of throughput? 
--For throughput, do you prefer daily metric or 7-day rolling and why?

--7 day rolling average of throughput:method 2
  SELECT 
  ds, 
  (
    SELECT AVG(time_spent) 
    FROM job_data 
    WHERE ds BETWEEN DATEADD(day, -6, t.ds) AND t.ds
  ) AS rolling_avg_throughput 
FROM 
  job_data t 
GROUP BY 
  ds 
ORDER BY 
  ds ASC;



 --7 day rolling average of throughput:method 3

  WITH cte_job_data AS (
  SELECT 
    ds, 
    SUM(time_spent) AS total_time_spent 
  FROM 
    job_data 
  GROUP BY 
    ds 
)
SELECT 
  ds, 
  (
    SELECT AVG(total_time_spent) 
    FROM cte_job_data 
    WHERE ds BETWEEN DATEADD(day, -6, t.ds) AND t.ds
  ) AS rolling_avg_throughput 
FROM 
  cte_job_data t 
ORDER BY 
  ds ASC;

  -- the daily throughput
  SELECT 
  ds,
  SUM(time_spent) AS daily_throughput
FROM 
  job_data 
GROUP BY 
  ds 
ORDER BY 
  ds ASC;

  --Percentage share of each language: Share of each language for different contents.
  SELECT 
  event,
  language,
  COUNT(*) AS count,
  100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY event) AS percentage
FROM job_data
GROUP BY event, language;

--Duplicate rows: Rows that have the same value present in them. To display the duplicate rows in the table,
SELECT ds, job_id, actor_id, event, language, time_spent, org, COUNT(*) as duplicates
FROM job_data
GROUP BY ds, job_id, actor_id, event, language, time_spent, org
HAVING COUNT(*) > 1;







--(Investigating metric spike)

--the weekly user engagement(in hour)
  SELECT 
    DATEADD(week, DATEDIFF(week, 0, e.occurred_at), 0) AS week_start,
    COUNT(DISTINCT e.user_id) AS weekly_engaged_users
FROM  [Table-2 events] e
INNER JOIN [Table-1 users] u ON e.user_id = u.user_id
WHERE e.event_type = 'engagement'
GROUP BY DATEADD(week, DATEDIFF(week, 0, e.occurred_at), 0)
ORDER BY week_start ASC;



 --user growth for product
SELECT
  YEAR(created_at) AS year,
  COUNT(DISTINCT ue.user_id) AS new_users,
  COUNT(DISTINCT CASE WHEN ua.user_id IS NOT NULL THEN ue.user_id END) AS activated_users,
  (COUNT(DISTINCT CASE WHEN ua.user_id IS NOT NULL THEN ue.user_id END) * 100.0) / COUNT(DISTINCT ue.user_id) AS user_growth_percentage
FROM [master].[dbo].[Table-1 users] AS ue
LEFT JOIN [master].[dbo].[Table-2 events] AS ua
  ON ue.user_id = ua.user_id
  AND ua.event_name = 'home_page'
  AND YEAR(ua.occurred_at) BETWEEN 2013 AND 2014
WHERE YEAR(ue.created_at) BETWEEN 2013 AND 2014
GROUP BY YEAR(created_at)
ORDER BY YEAR(created_at);


--weekly retention of users-sign up cohort
SELECT 
  DATEADD(week, DATEDIFF(week, 0, u.created_at), 0) AS signup_week, 
  DATEADD(week, DATEDIFF(week, 0, e.occurred_at), 0) AS engagement_week, 
  COUNT(DISTINCT e.user_id) * 100.0 / COUNT(DISTINCT CASE WHEN e.event_type = 'engagement' THEN e.user_id END) AS weekly_retention
FROM [master].[dbo].[Table-1 users] u
JOIN [master].[dbo].[Table-2 events] e ON u.user_id = e.user_id
WHERE u.state = 'active' 
GROUP BY DATEADD(week, DATEDIFF(week, 0, u.created_at), 0), DATEADD(week, DATEDIFF(week, 0, e.occurred_at), 0)
ORDER BY signup_week, engagement_week;


--the weekly engagement per device
WITH temp_table_1 AS (
  SELECT user_id, 
         activated_at, 
         state,
         DATEPART(week, activated_at) AS week_num
  FROM [master].[dbo].[Table-1 users] 
),
temp_table_2 AS (
  SELECT user_id, 
         occurred_at, 
         event_type, 
         device,
         DATEPART(week, occurred_at) AS week_num
  FROM [master].[dbo].[Table-2 events]
)
SELECT COUNT(DISTINCT t1.user_id) AS weekly_engagement,t2.device
FROM  temp_table_1 t1
JOIN temp_table_2 t2 ON t1.user_id = t2.user_id AND t1.week_num = t2.week_num
GROUP BY t1.week_num, t2.device
ORDER BY t1.week_num;

--the email engagement metrics
SELECT 
    COUNT(DISTINCT user_id) AS engaged_users, 
    COUNT(*) AS total_emails_sent, 
    CASE WHEN COUNT(*) > 0 
         THEN ROUND(COUNT(CASE WHEN action = 'email_open' THEN 1 END) * 100.0 / COUNT(*), 2) 
         ELSE 0 
    END AS email_open_rate
FROM [master].[dbo].[Table-3 email_events]
WHERE action IN ('sent_weekly_digest', 'email_open')
 






