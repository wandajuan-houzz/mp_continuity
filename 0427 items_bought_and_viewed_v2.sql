create table wandajuan.mpcon_items_bought_viewed as (
with t as (

select 
--	vl.l1_category_name, * 
	i.order_id, i.order_date, i.user_id, i.order_item_id, i.item_id, i.house_id, i.sku_name, 
	vl.title as itm_title,
	vl.category_name as itm_category_name,
	vl.l1_category_name as itm_l1_category, 
	vl.l2_category_name as itm_l2_category,
	vl.l3_category_name as itm_l3_category,
	vl.l4_category_name as itm_l4_category, 
	
--	order_sess.order_id, order_sess.order_date, order_sess.user_id, order_sess.session_id, order_sess.session_dt, order_sess.device_cat, order_sess.session_type 
	order_sess.*
from mp.order_item_margins_with_replacement i -- items bought
left join shop.vl_pupil vl
on vl.vendor_listing_id = i.item_id
left join (
	select 
		o.order_id as order_id_dup, --o.order_date, o.user_id, 
		o.session_id, o.session_dt, o.device_cat, o.session_type, 
		coalesce(web_pv.house_id, app_pv.house_id) as pv_house_id,
		coalesce(web_pv.visitor_id, app_pv.device_id) as visitor_id,
		coalesce(web_pv.source, app_pv.source) as pv_source,
		coalesce(web_pv.dt, app_pv.dt) as pv_dt
	--	web_pv.house_id as web_house_id, web_pv.visitor_id, web_pv.dt as web_dt,
	--	app_pv.house_id as app_house_id, app_pv.device_id, app_pv.dt as app_dt
	from dm.order_sess o  -- prior sessions associated to this order
	left join (
		select 
			page_behavior,
			if(page_id is null, cast(regexp_extract(url, '(.*pv~)(\d+)(.*)', 2) as bigint), page_id) as house_id,
			session_id,
			visitor_id,
			'web' as source,
			dt
		from l2.page_views_daily -- page views via web sess
		where page_behavior in ('VIEW_PRODUCT', 'pvp')
		and dt >= '2021-03-01'
	) web_pv
	on o.session_id = web_pv.session_id
	and o.status in (0, 1, 2, 3, 4, 5, 20, 99)
	and o.order_date >= '2021-04-01'
	left join (
		select 
			cast(object_id as bigint) as house_id, 
			session_id,
			device_id,
			'app' as source,
			dt
		from l2.mobile_client_event -- page views via app sess 
		where event_type = 'View'
		and object_id is not null and entity_type = 'Product'
		and (context <> 'Back' or context is null)
		and dt >= '2021-03-01'
	) app_pv
	on o.session_id = app_pv.session_id
	where o.status in (0, 1, 2, 3, 4, 5, 20, 99)
	and o.order_date >= '2021-04-01'
) order_sess
on cast(i.order_id as bigint) = order_sess.order_id_dup
where 
i.status in (0, 1, 2, 3, 4, 5, 20, 99)
and i.is_replacement_order = 0
and i.order_id not in (select order_id from logs.marketplace_gift_cards_purchased)
and i.checkout_rank = 1
and i.order_date >= '2021-04-01'

)
select t.*, 
	vl.title as pv_title,
	vl.category_name as pv_category_name,
	vl.l1_category_name as pv_l1_category, 
	vl.l2_category_name as pv_l2_category,
	vl.l3_category_name as pv_l3_category,
	vl.l4_category_name as pv_l4_category 
from t 
left join shop.vl_pupil vl
on t.pv_house_id = vl.house_id
where pv_house_id is not null
);