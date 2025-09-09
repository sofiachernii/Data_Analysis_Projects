# Часть 1. Расчет Retention Rate (удержание)

## Расчет Rolling Retention – доля пользователей, оставшихся активными через N дней.

```sql
with users_filt as (
select
	*
from
	users
),
a as (
select 
		u.user_id, 
		to_char(u2.date_joined, 'YYYY-MM') as cohort,
		extract(day from u.entry_at - u2.date_joined) as diff
from
	userentry u
left join users_filt u2 
	on
	u.user_id = u2.id
where
	u.entry_at::date - u2.date_joined::date >= 0
order by
	user_id,
	diff
)
select
	cohort,
	round(count(distinct case when diff >= 0 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 0",
	round(count(distinct case when diff >= 1 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 1",
	round(count(distinct case when diff >= 3 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 3",
	round(count(distinct case when diff >= 7 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 7",
	round(count(distinct case when diff >= 14 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 14",
	round(count(distinct case when diff >= 30 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 30",
	round(count(distinct case when diff >= 60 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 60",
	round(count(distinct case when diff >= 90 then user_id end) * 100.0 / count(distinct case when diff >= 0 then user_id end), 1) as "Day 90"
from
	a
group by
	cohort
order by
	cohort
```

# Часть 2. Расчет поведенческих метрик
## Частота использования сервиса (DAU, MAU).
### Расчет DAU
```sql
select
	entry_at::date as "Дата",
	count(distinct user_id) as "Кол-во пользователей"
from
	userentry
group by
	entry_at::date
```

### Расчет MAU
```sql
with for_mau as (
    select to_char(entry_at, 'YYYY-MM') as ym, count(distinct user_id) as cnt
    from userentry
    group by ym
)
select 
    percentile_cont(0.5) WITHIN GROUP (ORDER BY cnt) as median_mau,
    avg(cnt) as avg_mau
from for_mau
```

## Распределение активации аккаунтов (подтвердили/не подтвердили аккаунт).
```sql
select
	case
		when is_active = 1 then 'Активированный аккаунт'
		else 'Неактивированный аккаунт'
	end,
	count(*)
from
	users
group by
	is_active
```

## Воронка «Сделал попытку решить задачу – «Решил задачу успешно – Пополнил кошелек».
```sql
with codesubmit_filt as (
    select *
    from codesubmit
),
coderun_filt as (
    select *
    from coderun
),
transaction_filt as (
    select *
    from transaction
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
	select count(distinct user_id) as "Количество", '3. Пополнил кошелек' as description
	from transaction_filt
	where type_id = 2
)
select * 
from results
order by description
```

## Распределение, показывающее, на что пользователи тратят CodeCoins: покупают задачи, подсказки, решения, тесты.
```sql
with transaction_filt as (
select
	*
from
	transaction
)
select
	t2.description as "Тип транзакции",
	count(t.id) as "Количество транзакций"
from
	transaction_filt t
left join transactiontype t2 
on
	t.type_id = t2."type"
where
	t2."type" = 1
	or t2."type" between 23 and 28
group by
	t2.description
order by
	count(t.id) desc
```

## Распределение первых и повторных покупок.
```sql
with a as (
select 
		user_id, 
		count(*), 
		case 
			when count(*) = 1 then 'Купили один раз'
		else 'Купили больше одного раза'
	end user_type
from
	"transaction"
left join transactiontype
	on
	"transaction".type_id = transactiontype."type"
where
	transactiontype."type" = 1
	or transactiontype."type" between 23 and 28
group by
	user_id
)
select
	user_type,
	count(*)
from
	a
group by
	user_type
```

# Часть 3. Расчет финансовых метрик
## Churn Rate (отток) – процент отписавшихся за период.
```sql
with users_filt as (
select
	*
from
	users
),
user_activity as (
select
	u.user_id,
	to_char(u2.date_joined, 'YYYY-MM') as cohort,
	extract(day from u.entry_at - u2.date_joined) as days_since_join,
	MAX(extract(day from u.entry_at - u2.date_joined)) over (partition by u.user_id) as max_active_day
from
	userentry u
left join users_filt u2 
        on
	u.user_id = u2.id
where
	u.entry_at::date - u2.date_joined::date >= 0
),
cohort_stats as (
select
	cohort,
	COUNT(distinct user_id) as cohort_size,
	--Расчет Rolling Retention
	ROUND(COUNT(distinct case when days_since_join >= 0 then user_id end) * 100.0 / 
              COUNT(distinct case when days_since_join >= 0 then user_id end), 1) as "Day 0",
	ROUND(COUNT(distinct case when days_since_join >= 1 then user_id end) * 100.0 / 
              COUNT(distinct case when days_since_join >= 0 then user_id end), 1) as "Day 1",
	--Расчет Churn Rate (100% - Retention)
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
	cohort_size,
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
```