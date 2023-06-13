SELECT *, EXTRACT(month from activated_at) as monthnnum from op_an.table_users order by monthnnum desc; 
Select user_id ,(Select EXTRACT(YEAR from activated_at) as year_at, extract(month from activated_at) as month_at) a from op_an.table_users where month_at = 11 and year_at = 2020;
Select Year(activated_at)as Year , monthname(activated_at) as Month from op_an.table_users;

select * from op_an.table_users where monthname(activated_at) = 'November' and year(activated_at) = 2013; 
select *, extract(year from ds )as year, extract(month from ds) as month from op_an.sql_project_table;
select * from op_an.sql_project_table;
select * from op_an.table_events;
select extract(year from ds) as year, extract(month from ds) as month from op_an.sql_project_table;
select * from op_an.sql_project_table where monthname(ds) = 'November' and year(ds) = 2020;
select language, (count(language) * 100 / (select count(*) from op_an.sql_project_table) )as per from op_an.sql_project_table group by language ;
select job_id, count(job_id) from op_an.sql_project_table group by job_id having count(job_id) > 1;

select event_type, count( event_type) from op_an.table_events group by event_type;
select * from op_an.table_events;
select count(distinct user_id) from op_an.table_events;

 select * from op_an.table_events where event_type = 'signup_flow';
 select distinct event_name from op_an.table_events where event_type = 'engagement';
 select * from op_an.table_email_events;
 select distinct action from op_an.table_email_events;
 select * from op_an.table_email_events where action = 'email_clickthrough';
 
 



 select count(distinct user_id) from op_an.table_email_events where action not in ('sent_reengagement_email');
 
select 
	user_id ,
    count(user_id) as WAU 
from op_an.table_events
	where occurred_at < NOW() 
    and event_type = 'engagement' 
    and event_name not in ('login')
group by user_id ;


select extract(week from occurred_at) as weeknumber, count(distinct user_id) from op_an.table_events group by weeknumber;

select weeknumber, active_users, sum(active_users) over(order by weeknumber rows between unbounded preceding and current row) as cum_sum 
from( select extract(week from a.activated_at )as weeknumber,count(distinct user_id) as active_users from op_an.table_users a where state='active' 
group by weeknumber) a;



select extract(year from occurred_at ) as year, extract(week from occurred_at) as week, device, count(distinct user_id) from
op_an.table_events where event_type = 'engagement'group by 1,2,3 order by 1,2,3;




select * from op_an.table_email_events;


select action, count(*) as num_email from op_an.table_email_events group by action;
select 100 * sum(case when email_cat = 'email_open' then 1 else 0 end)/sum(case when email_cat = 'email_sent' then 1 else 0 end) as email_open_rate,
	100 * sum(case when email_cat = 'email_clicked' then 1 else 0 end)/sum(case when email_cat = 'email_sent' then 1 else 0 end) as  email_clicked_from
from
(
select *, case when action IN ('sent_weekly_digest','sent_reengagement_email') 
	THEN 'email_sent'
	when action IN ('email_open')
    then 'email_open'
    when action in ('email_clickthrough')
    then 'email_clicked'
    end as email_cat
from op_an.table_email_events
    ) a;
 
 select count(user_id), sum(case when retention_week =1 then 1 else 0 end) as week_1 from(
 select a.user_id,a.signup_week, b.engagement_week, b.engagement_week - a.signup_week as retention_week from(
(select distinct user_id, extract(week from occurred_at) as signup_week   
from op_an.table_events where event_type = 'signup_flow' 
and event_name = 'complete_signup' 
and extract(week from occurred_at) = 18) a
left join 
(
select user_id, 
extract(week from occurred_at) as engagement_week 
from op_an.table_events where event_type = 'engagement' 
)b 
on a.user_id = b.user_id)
order by a.user_id)
group by a.user_id;    

select * from op_an.sql_project_table;


SELECT ds, COUNT(job_id) AS jobs_per_day, SUM(time_spent)/3600 AS hours_spent
FROM  op_an.sql
WHERE ds >= '01-11-2020' AND ds <= '30-11-2020'
GROUP BY ds;



select * from op_an.sql;




-- ITH CTE AS (
--     SELECT ds, COUNT(job_id) AS jobs, SUM(time_spent) AS times
--     FROM op_an.sql
--     GROUP BY ds
-- )
-- SELECT ds, SUM(jobs) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / SUM(total_time) OVER (ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS throughput_7d_rolling_avg
-- FROM CTE;


select *,
       avg(dailyusage) over(partition by productid order by productid, date rows between 6 preceding and current row) as rolling_avg
  from (select job_id, ds, sum(timpe_spent) as dailyusage
          from tbl
         group by productid, date) x;

select t1.ds, AVG(t2.`event`) as MV_AVG
from op_an.sql t1
left outer join op_an.sql t2 
    on t2.ds between DATE_ADD(t1.ds, INTERVAL -6 DAY) 
        and t1.ds
group by t1.ds;


SELECT ds as date_of_review, jobs_reviewed, 
AVG(jobs_reviewed) 
OVER(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS throughput_7_rolling_average
FROM ( SELECT ds, COUNT( DISTINCT job_id) AS jobs_reviewed FROM op_an.sql GROUP BY ds ORDER BY ds ) a;

SELECT * FROM 
(SELECT *, ROW_NUMBER()OVER(PARTITION BY job_id) AS row_num
FROM op_an.sql) a 
WHERE row_num>1;


select op_an.sql.job_id, op_an.sql.language,count(op_an.sql.language) as total_of_each_language,((count(op_an.sql.language)/(select count(*) from op_an.sql))*100) as percentage_share_of_each_distinct_language 
from op_an.sql
order by op_an.sql.language;


SELECT 
  extract(week from occurred_at) as week_num_user,
  count(distinct user_id)
FROM 
  op_an.table_email_events
group by 
  week_num_user;
  
  
  
  
  select 
  year_num,
  week_num,
  num_active_users,
  SUM(num_active_users)OVER(ORDER BY year_num, week_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_active_users
from
(
select 
  extract(year from a.activated_at) as year_num,
  extract(week from a.activated_at) as week_num,
  count(distinct user_id) as num_active_users
from 
  op_an.table_users a 
WHERE
  state = 'active'
group by year_num,week_num
order by year_num,week_num
) a;


SELECT
distinct user_id,
COUNT(user_id),
SUM(CASE WHEN retention_week = 1 Then 1 Else 0 END) as per_week_retention
FROM 
(
SELECT
a.user_id,
a.signup_week,
b.engagement_week,
b.engagement_week - a.signup_week as retention_week
FROM 
(
(SELECT distinct user_id, extract(week from occurred_at) as signup_week from op_an.table_email_events
WHERE event_type = 'signup_flow'
and event_name = 'complete_signup'
-- --and extract(week from occurred_at) = 18
)a 
LEFT JOIN
(SELECT distinct user_id, extract(week from occurred_at) as engagement_week FROM op_an.table_email_events
where event_type = 'engagement'
)b 
on a.user_id = b.user_id
)
)d 
group by user_id
order by user_id
;

SELECT
distinct user_id,
COUNT(user_id),
SUM(CASE WHEN retention_week = 1 Then 1 Else 0 END) as per_week_retention
FROM 
(
SELECT
a.user_id,
a.signup_week,
b.engagement_week,
b.engagement_week - a.signup_week as retention_week
FROM 
(
(SELECT distinct user_id, extract(week from occurred_at) as signup_week from op_an.table_events
WHERE event_type = 'signup_flow'
and event_name = 'complete_signup'
and extract(week from occurred_at) = 18
)a 

LEFT JOIN

(SELECT distinct user_id, extract(week from occurred_at) as engagement_week FROM op_an.table_events
where event_type = 'engagement'
)b 
on a.user_id = b.user_id
)
)d 
group by user_id
order by user_id
;

select device,
  extract(year from occurred_at) as year_num,
  extract(week from occurred_at) as week_num,
  COUNT(distinct user_id) as no_of_users
FROM 
 op_an.table_events
where event_type = 'engagement'
GROUP by 1,2,3
order by 1,2,3;



SELECT
  100.0*SUM(CASE when email_cat = 'email_opened' then 1 else 0 end)/SUM(CASE when email_cat = 'email_sent' then 1 else 0 end) as email_opening_rate,
  100.0*SUM(CASE when email_cat = 'email_clicked' then 1 else 0 end)/SUM(CASE when email_cat = 'email_sent' then 1 else 0 end) as email_clicking_rate
FROM 
(
SELECT 
  *,
  CASE 
    WHEN action in ('sent_weekly_digest','sent_reengagement_email')
      then 'email_sent'
    WHEN action in ('email_open')
      then 'email_opened'
    WHEN action in ('email_clickthrough')
      then 'email_clicked'
  end as email_cat
from op_an.table_email_events
) a;