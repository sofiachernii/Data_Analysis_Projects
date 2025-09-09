# Расчет DAU (Daily Active Users) для дашборда
select entry_at::date as "Дата", count(distinct user_id) as "Кол-во пользователей"
from userentry
where true 
    and {{entry_at}}
group by entry_at::date

# Расчет MAU (Monthly Active Users) для дашборда
with for_mau as (
    select to_char(entry_at, 'YYYY-MM') as ym, count(distinct user_id) as cnt
    from userentry
        where true 
            and {{entry_at}}
    group by ym
)
select 
    percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt) as median_mau,
    avg(cnt) as avg_mau
from for_mau

# Расчет Rolling Retention (удержание пользователей) для дашборда
with users_filt as (
	select *
	from users
where true
    and {{date_joined}}
),
a as (
	select 
		u.user_id, 
		to_char(u2.date_joined, 'YYYY-MM') as cohort,
		extract(day from u.entry_at - u2.date_joined)  as diff 
	from userentry u
	left join users_filt u2 
	on u.user_id = u2.id
	where u.entry_at::date - u2.date_joined::date >= 0
	order by user_id, diff
)
select
	cohort,
	round(count(distinct case when diff >= 0 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 0",
	round(count(distinct case when diff >= 1 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 1",
	round(count(distinct case when diff >= 3 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 3",
	round(count(distinct case when diff >= 7 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 7",
	round(count(distinct case when diff >= 14 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 14",
	round(count(distinct case when diff >= 30 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 30",
	round(count(distinct case when diff >= 60 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 60",
	round(count(distinct case when diff >= 90 then user_id end) * 100.0/count(distinct case when diff >= 0 then user_id end), 1) as "Day 90"
from a
group by cohort
order by cohort

# Расчет распределения активации аккаунтов для дашборда
select
    case when is_active = 1 then 'Активированный аккаунт' else 'Неактивированный аккаунт' end, count(*)
from users
where true
    and {{date_joined}}
group by is_active

# Расчет распределения первых и повторных покупок для дашборда
with a as (
	select 
		user_id, 
		count(*), 
		case 
			when count(*) = 1 then 'Купили один раз' else 'Купили больше одного раза'
		end user_type
	from "transaction" 
	left join transactiontype
	on "transaction".type_id = transactiontype."type" 
	where true 
	    and transactiontype."type" = 1 or transactiontype."type" between 23 and 28
	    and {{date_range}}
	group by user_id
)
select user_type, count(*) 
from a
group by user_type

# Расчет воронки пользователей за все время для дашборда
with codesubmit_filt as (
    select *
    from codesubmit
    where true 
    [[and codesubmit.created_at between {{start_date}} and {{end_date}}]]
),
coderun_filt as (
    select *
    from coderun
    where true 
    [[and coderun.created_at between {{start_date}} and {{end_date}}]]
),
transaction_filt as (
    select *
    from transaction
    where true 
    [[and transaction.created_at between {{start_date}} and {{end_date}}]]
),
attempts as (
	select user_id
	from codesubmit_filt
	union all
	select user_id
	from coderun_filt
),
results as (
	select count(distinct user_id) as "Количество", '1. Попытались решить задачу' as description
	from attempts
	union
	select count(distinct user_id) as "Количество", '2. Решили задачу' as description
	from codesubmit_filt
	where is_false = 0
	union
	select count(distinct user_id) as "Количество", '3. Пополнили кошелек' as description
	from transaction_filt
	where type_id = 2
)
select * 
from results
order by description

# Расчет распределения трат CodeCoins (внутренней валюты) для дашборда
with transaction_filt as (
    select *
    from transaction
    where true
    and {{date_range}}
)
select t2.description as "Тип транзакции", count(t.id) as "Количество транзакций"
from transaction_filt t
left join transactiontype t2 
on t.type_id = t2."type" 
where t2."type" = 1 or t2."type" between 23 and 28
group by t2.description
order by count(t.id) desc

# Расчет Churn Rate (отток пользователей) для дашборда
with users_filt as (
	select *
	from users
	where true
    and {{date_joined}}
),
user_activity as (
	select 
		u.user_id, 
		to_char(u2.date_joined, 'YYYY-MM') as cohort,
		extract(day from u.entry_at - u2.date_joined)  as days_since_join,
		MAX(extract(day from u.entry_at - u2.date_joined)) over (partition by u.user_id) as max_active_day
	from userentry u
	left join users_filt u2 
	on u.user_id = u2.id
	where u.entry_at::date - u2.date_joined::date >= 0
),
cohort_stats as (
select
	cohort,
	round(count(distinct case when days_since_join >= 0 then user_id end) * 100.0/count(distinct case when days_since_join >= 0 then user_id end), 1) as "Day 0",
	round(count(distinct case when days_since_join >= 1 then user_id end) * 100.0/count(distinct case when days_since_join >= 0 then user_id end), 1) as "Day 1",
	100 - ROUND(COUNT(distinct case when days_since_join >= 1 then user_id end) * 100.0 / 
              COUNT(distinct case when days_since_join >= 0 then user_id end), 1) as "Churn_Day1",
	100 - ROUND(COUNT(distinct case when days_since_join >= 7 then user_id end) * 100.0 / 
              COUNT(distinct case when days_since_join >= 0 then user_id end), 1) as "Churn_Day7",
	100 - ROUND(COUNT(distinct case when days_since_join >= 30 then user_id end) * 100.0 / 
              COUNT(distinct case when days_since_join >= 0 then user_id end), 1) as "Churn_Day30",
	100 - ROUND(COUNT(distinct case when days_since_join >= 90 then user_id end) * 100.0 / 
              COUNT(distinct case when days_since_join >= 0 then user_id end), 1) as "Churn_Day90"
from
	user_activity
group by
	cohort
)
select
	cohort,
	"Day 0",
	"Day 1",
	"Churn_Day1",
	"Churn_Day7",
	"Churn_Day30",
	"Churn_Day90"
from
	cohort_stats
order by
	cohort