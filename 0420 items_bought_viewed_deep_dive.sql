
-- items bought from 2021-04-01 to 2021-04-16 for users' first checkout and the products viewed in the prior month
select * from wandajuan.mpcon_items_bought_viewed_distinct;


-- 22,973 unique order_id||item_id
select count(*), count(distinct cast(order_id as varchar)||cast(item_id as varchar)) from wandajuan.mpcon_items_bought_viewed_distinct; 


-- 188443	22973	8
-- On average 8 products were viewed before an item was purchased
with t as (
	select count(distinct session_id||cast(pv_house_id as varchar)) as nunique_pv, 
			count(distinct cast(order_id as varchar)||cast(item_id as varchar)) as nunique_order_itm
	from wandajuan.mpcon_items_bought_viewed_distinct
) select nunique_pv, nunique_order_itm, nunique_pv / nunique_order_itm as avg_num_pv_per_order_item from t;



-- l1 categories between items bought vs. viewed
select itm_l1_category, pv_l1_category, count(distinct order_id) from wandajuan.mpcon_items_bought_viewed_distinct
group by 1, 2
having count(distinct order_id) > 2
order by 3 desc;

-- house_id between items bought vs. viewed  --> as expected, same products bought and viewed
select house_id, pv_house_id, count(distinct order_id) from wandajuan.mpcon_items_bought_viewed_distinct
group by 1, 2
having count(distinct order_id) > 2
order by 3 desc;


-- days between first product view to purchase (histogram)
select max_days_pv2pur, count(*) from (
	select order_id, max(date_diff('day', cast(session_dt as date), cast(order_date as date))) as max_days_pv2pur from wandajuan.mpcon_items_bought_viewed_distinct
	where house_id = pv_house_id
	group by 1
) t
group by 1
order by 2 desc;




