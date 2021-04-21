
-- items bought from 2021-04-01 to 2021-04-14 for users' first checkout and the products viewed in the prior month
select * from wandajuan.mpcon_items_bought_viewed_distinct;
select min(order_date), max(order_date), count(distinct order_id), count(distinct user_id) from wandajuan.mpcon_items_bought_viewed_distinct;

-- 22,973 unique order_id||item_id
select count(*), count(distinct cast(order_id as varchar)||cast(item_id as varchar)) from wandajuan.mpcon_items_bought_viewed_distinct; 


-- 188443	22973	8
-- On average 8 products were viewed before an item was purchased
with t as (
	select count(distinct session_id||cast(pv_house_id as varchar)) as nunique_pv, 
			count(distinct cast(order_id as varchar)||cast(item_id as varchar)) as nunique_order_itm
	from wandajuan.mpcon_items_bought_viewed_distinct
) select nunique_pv, nunique_order_itm, nunique_pv / nunique_order_itm as avg_num_pv_per_order_item from t;


select avg(sess_cnt), avg(sess_dt_cnt), avg(itm_cnt), avg(pv_cnt) from (
	select order_id, count(distinct session_id) as sess_cnt, count(distinct session_dt) as sess_dt_cnt, count(distinct house_id) as itm_cnt, count(distinct pv_house_id) as pv_cnt from wandajuan.mpcon_items_bought_viewed_distinct
	group by 1
	order by 2 desc
);



-- l1 categories between items bought vs. viewed
select itm_l1_category, pv_l1_category, count(distinct order_id) from wandajuan.mpcon_items_bought_viewed_distinct
group by 1, 2
having count(distinct order_id) > 2
order by 3 desc;


-- house_id between items bought vs. viewed  --> as expected, same products bought and viewed
select house_id, pv_house_id, count(distinct order_id) from wandajuan.mpcon_items_bought_viewed_distinct
where house_id <> pv_house_id
group by 1, 2
order by 3 desc;
--having count(distinct order_id) > 2



-- days between first product view to purchase (histogram)
-- 76% happened on the same day
select max_days_pv2pur, count(*) from (
	select order_id, max(date_diff('day', cast(session_dt as date), cast(order_date as date))) as max_days_pv2pur from wandajuan.mpcon_items_bought_viewed_distinct
	where house_id = pv_house_id
	group by 1
) t
group by 1
order by 2 desc;

-- for those same-day orders, what categories do they fall into? are they small, less expensive items?
with t as (
	select max(date_diff('day', cast(session_dt as date), cast(order_date as date))) over (partition by order_id) as max_days_pv2pur, * from wandajuan.mpcon_items_bought_viewed_distinct
	where house_id = pv_house_id
	)
select itm_l1_category, itm_l2_category, itm_l3_category, max_days_pv2pur, count(distinct order_id), count(*) from t
group by 1, 2, 3, 4
order by 4 asc, 5 desc;










-- products viewed before order_date
with t as (
	select dense_rank() over (partition by order_id order by session_dt desc) as rnk, * from wandajuan.mpcon_items_bought_viewed_distinct
	) 
select house_id, pv_house_id, count(distinct order_id) from t
where rnk > 1 
group by 1, 2
order by 3 desc;


-- products viewed on the 2nd to the last session
with t as (
	select dense_rank() over (partition by order_id order by session_dt desc) as rnk, * from wandajuan.mpcon_items_bought_viewed_distinct
	) 
select house_id, pv_house_id, count(distinct order_id) from t
where rnk = 2 
group by 1, 2
order by 3 desc;




-- add a dense_rank to count session_dt order from the latest to the farest
create table wandajuan.mpcon_items_bought_viewed_distinct_rnk as (
	select *, dense_rank() over (partition by order_id order by session_dt desc) as rnk from wandajuan.mpcon_items_bought_viewed_distinct
);


-- 180,575 rows
-- filter only the 2nd to the last session - previous session
drop table wandajuan.mpcon_items_bought_viewed_distinct_prev_sess;
create table wandajuan.mpcon_items_bought_viewed_distinct_prev_sess as (
	with t as (
		select *, dense_rank() over (partition by order_id order by session_dt desc) as rnk,
				date_diff('day', cast(session_dt as date), cast(order_date as date)) as days_pv2pur
		from wandajuan.mpcon_items_bought_viewed_distinct
	)
	select * from t
	where rnk = 2
);


-- focus on the prev session
-- l1 categories between items bought vs. viewed in the previous session
select itm_l1_category, pv_l1_category, count(distinct order_id) from wandajuan.mpcon_items_bought_viewed_distinct_prev_sess
group by 1, 2
having count(distinct order_id) > 2
order by 3 desc;

-- house_id bought vs viewed in prev session
select house_id, pv_house_id, count(distinct order_id) from wandajuan.mpcon_items_bought_viewed_distinct_prev_sess
group by 1, 2
having count(distinct order_id) > 1
order by 3 desc;