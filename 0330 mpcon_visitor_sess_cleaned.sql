-- 1. all visitors who have session between '2021-01-01' and date '2021-01-14'
create table wandajuan.mpcon_allvisitors as
select distinct user_id, visitor_id from l2.session_analytics
where cast(dt as date) between date '2021-01-01' and date '2021-01-14';


-- 2. new visitors only (remove visitors who visited in 2020) - aka whose first visit is between '2021-01-01' and date '2021-01-14'
create table wandajuan.mpcon_newvisitors as (
select * from wandajuan.mpcon_allvisitors where
    visitor_id not in (
        select distinct visitor_id from l2.session_analytics
        where cast(dt as date) between date '2020-01-01' and date '2020-12-31'));


-- 3. first visitors between 2021-01-01 and 2021-01-14 and their sessions between date '2021-01-01' and date '2021-03-01' 
create table wandajuan.mpcon_newvisitor_sess as (
    select * from l2.session_analytics
    where visitor_id in (select visitor_id from wandajuan.mpcon_newvisitors)
    and cast(dt as date) between date '2021-01-01' and date '2021-03-01'
    );



-- 4a one-time visitors only - who have only one session 
select visitor_type, platform, medium, 
        count(distinct session_id) as hz_sessions,
        sum(if_mpl_session) as mp_sessions,
        sum(if_view_product) as view_products,
        sum(if_cart_add) as cart_adds,
        sum(if_xo_confirm) as xo_confirms
from (
    select *, if(device_cat='Personal computer', 'dWeb', 'mWeb') as platform, 
            if(signin_status!='SIGNED_OUT', 'Signed-in User', 'Visitor') as visitor_type,
            if(
                browse_products + view_product + search_products + add_product_to_ideabook +
                cart_add + cart_remove + view_cart + --cart_update + 
                checkout_launch + checkout_order_review + checkout_shipping +
                checkout_billing_and_payment + checkout_order_confirmation >= 1,
                1, 0) as if_mpl_session,
            if(view_product>0, 1, 0) as if_view_product,
            if(cart_add>0, 1, 0) as if_cart_add,
            if(checkout_order_confirmation>0, 1, 0) as if_xo_confirm 
    from (
        select s.* from wandajuan.mpcon_newvisitor_sess s
        inner join (select visitor_id, count(*) as usr_sess_cnt from wandajuan.mpcon_newvisitor_sess group by 1) v
        on s.visitor_id = v.visitor_id
        and v.usr_sess_cnt = 1  -- one-time visitor only
        ) m
    ) m
group by 1, 2, 3
order by 1, 2, 3;


--4b repeating visitors' sessions in 30days from the first visit
create table wandajuan.mpcon_repeatvisitor_sess as (
select * from (
    select s.*, first_value(s.dt) over (partition by s.visitor_id order by s.dt asc) as first_dt
    -- ,
    --             first_value(s.device_cat) over (partition by s.visitor_id order by s.dt asc) as first_device,
    --             if(s.signin_status!='SIGNED_OUT', 1, 0) as if_signed_in
    from wandajuan.mpcon_newvisitor_sess s
    inner join (select visitor_id, count(*) as usr_sess_cnt from wandajuan.mpcon_newvisitor_sess group by 1) v
    on s.visitor_id = v.visitor_id
    and v.usr_sess_cnt > 1 -- visitor WITH MORE than 1 visit
    ) m 
where cast(dt as date) between cast(first_dt as date) and date_add('day', 30, cast(first_dt as date))
);



select visitor_type, first_platform, first_medium, repeat_times, 
                                    count(distinct visitor_id) as visitor_cnt, 
                                    count(distinct session_id) as hz_sessions, 
                                    sum(if_mpl_session) as mp_sessions, 
                                    sum(if_view_product) as view_products, 
                                    sum(if_cart_add) as cart_adds, 
                                    sum(if_xo_confirm) as xo_confirms from (

    select s.session_id, s.visitor_id, s.dt, s.device_cat, s.first_dt, --s.signin_status,
         --   f.vis_sess_cnt,
            case when f.vis_sess_cnt = 2 then '1' 
                 when f.vis_sess_cnt = 3 then '2'
                 when f.vis_sess_cnt > 3 then '3+'
                 else 'No repeat' end as repeat_times,

            if(f.device_cat='Personal computer', 'dWeb', 'mWeb') as first_platform,
            f.medium as first_medium,


            -- if a user has at least one signed-in session, then 'Signed-In User'. Otherwise, 'Visitor', who does not have a Houzz account or has one but never signed in with this device/browser 
            if(sum(if(s.signin_status!='SIGNED_OUT', 1, 0)) over (partition by s.visitor_id)>1, 'Signed-in User', 'Visitor') as visitor_type, 

            if(
                s.browse_products + s.view_product + s.search_products + s.add_product_to_ideabook +
                s.cart_add + s.cart_remove + s.view_cart + --cart_update + 
                s.checkout_launch + s.checkout_order_review + s.checkout_shipping +
                s.checkout_billing_and_payment + s.checkout_order_confirmation >= 1,
                1, 0) as if_mpl_session,
            if(s.view_product>0, 1, 0) as if_view_product,
            if(s.cart_add>0, 1, 0) as if_cart_add,
            if(s.checkout_order_confirmation>0, 1, 0) as if_xo_confirm


            -- f.dt, f.device_cat, f.signin_status
    from wandajuan.mpcon_repeatvisitor_sess s
    join (select *, row_number() over (partition by visitor_id order by dt asc) as rnum, 
                    count(*) over (partition by visitor_id) as vis_sess_cnt  from wandajuan.mpcon_repeatvisitor_sess) f
    on s.visitor_id = f.visitor_id
    and f.rnum = 1 
    -- and s.visitor_id in 
    -- ('fa7adaaa-c379-43c7-a2ad-cce5b3c6ca0a',
    -- 'fbfdc1ff-0083-4e9c-9ba9-a857116040fe',
    -- 'ff6d5cab-9d7b-4de7-8595-92e58f9b7883',
    -- 'ff8d96b4-952e-4477-ae0c-fc3d0c10c09b',
    -- 'f56ded92-eccb-4269-a7a0-cbc5e39717e6',
    -- 'f3ff1e1d-5e3a-460f-b28a-c91ac8ccde37',
    -- 'f2cbe3af-4daa-43f9-9d81-a109331cf794',
    -- 'f2bb5b39-3913-4a28-8e9f-93c197f5c6e3',
    -- 'eef59e0f-e101-4824-8eb5-0d4c966c33ec',
    -- 'e7a58428-5567-4f5d-b90d-5bb1a4238a4b',
    -- 'db872a4e-0c06-453a-afcf-c023ed0832d8',
    -- 'f52826de-2995-483c-adce-07fa1bb83a4a')
    -- order by 2
    -- left join (select session_id, sum(if(order_id is null, 0, 1)) as order_cnt from dm.order_sess group by 1) dm
    -- on s.session_id = dm.session_id
    -- where dm.order_cnt is not null
    ) m
group by 1, 2, 3, 4
order by 1, 2, 3, 4;



-- 5 union 4a and 4b to create source table for tableau analysis

-- -- Updated Rows	14,262,331
create table wandajuan.mpcon_sess_raw_v2 as ( 
select session_id, visitor_id, dt, visitor_type, first_platform, first_channel, medium as first_medium, 'No repeat' as repeat_times,
        if_mpl_session, if_view_product, if_cart_add, if_xo_confirm,
        signin_status, device_cat, medium, landing_page_class, usr_sess_cnt, 1 as rnum
from (
    select *,
                if(device_cat='Personal computer', 'dWeb', 'mWeb') as first_platform, 

                case when medium = 'SEARCH' and landing_page_class in ('BROWSE_PRODUCTS', 'VIEW_PRODUCT', 'VIEW_CART', 'pvp', 'CHECKOUT_ACTION') then 'SEO - MP'
                    when medium = 'SEARCH' then 'SEO - Other'
                    else medium end as first_channel, 


                if(signin_status!='SIGNED_OUT', 'Signed-in User', 'Visitor') as visitor_type,
                if(
                    browse_products + view_product + search_products + add_product_to_ideabook +
                    cart_add + cart_remove + view_cart + --cart_update + 
                    checkout_launch + checkout_order_review + checkout_shipping +
                    checkout_billing_and_payment + checkout_order_confirmation >= 1,
                    1, 0) as if_mpl_session,
                if(view_product>0, 1, 0) as if_view_product,
                if(cart_add>0, 1, 0) as if_cart_add,
                if(checkout_order_confirmation>0, 1, 0) as if_xo_confirm 
        from (
            select s.*, v.usr_sess_cnt from wandajuan.mpcon_newvisitor_sess s
            inner join (select visitor_id, count(*) as usr_sess_cnt from wandajuan.mpcon_newvisitor_sess group by 1) v
            on s.visitor_id = v.visitor_id
            and v.usr_sess_cnt = 1
            ) m
    ) 

union all

select session_id, visitor_id, dt, visitor_type, first_platform, first_channel, first_medium, repeat_times,
        if_mpl_session, if_view_product, if_cart_add, if_xo_confirm,
        signin_status, device_cat, medium, landing_page_class, usr_sess_cnt, rnum 
from (
    select s.*, --s.session_id, s.visitor_id, s.dt, s.device_cat, --s.first_dt, --s.signin_status,
         --   f.vis_sess_cnt,
            f.vis_sess_cnt as usr_sess_cnt,
            case when f.vis_sess_cnt = 2 then '1' 
                 when f.vis_sess_cnt = 3 then '2'
                 when f.vis_sess_cnt > 3 then '3+'
                 else 'No repeat' end as repeat_times,

            if(f.device_cat='Personal computer', 'dWeb', 'mWeb') as first_platform,
            f.medium as first_medium,

            case when f.medium = 'SEARCH' and f.landing_page_class in ('BROWSE_PRODUCTS', 'VIEW_PRODUCT', 'VIEW_CART', 'pvp', 'CHECKOUT_ACTION') then 'SEO - MP'
                when f.medium = 'SEARCH' then 'SEO - Other'
                else f.medium end as first_channel, 


            if(sum(if(s.signin_status!='SIGNED_OUT', 1, 0)) over (partition by s.visitor_id)>1, 'Signed-in User', 'Visitor') as visitor_type,

            if(
                s.browse_products + s.view_product + s.search_products + s.add_product_to_ideabook +
                s.cart_add + s.cart_remove + s.view_cart + --cart_update + 
                s.checkout_launch + s.checkout_order_review + s.checkout_shipping +
                s.checkout_billing_and_payment + s.checkout_order_confirmation >= 1,
                1, 0) as if_mpl_session,
            if(s.view_product>0, 1, 0) as if_view_product,
            if(s.cart_add>0, 1, 0) as if_cart_add,
            if(s.checkout_order_confirmation>0, 1, 0) as if_xo_confirm,

            row_number() over (partition by s.visitor_id order by s.dt asc) as rnum


            -- f.dt, f.device_cat, f.signin_status
	  from wandajuan.mpcon_repeatvisitor_sess s
	  join (select *, row_number() over (partition by visitor_id order by dt asc) as rnum, 
	                    count(*) over (partition by visitor_id) as vis_sess_cnt  from wandajuan.mpcon_repeatvisitor_sess) f
	  on s.visitor_id = f.visitor_id
	  and f.rnum = 1 
    )
);
    
   
commit;   
   
   
select count(*), count(distinct session_id), count(distinct visitor_id), sum(if_mpl_session), sum(if_view_product), sum(if_cart_add), sum(if_xo_confirm) from wandajuan.mpcon_sess_raw;
-- 14262331	14262331	10227991	2988729	1534954	75043	21870


-- Unique Users to Checkout Conversion Rate

select visitor_type, first_platform, first_channel, first_medium, repeat_times, 
	sum(if_hz_session), sum(if_mpl_session), sum(if_view_product), sum(if_cart_add), sum(if_xo_confirm) 
from (
	select visitor_type, first_platform, first_channel, first_medium, repeat_times,
		visitor_id, -- aggregate to visitor level
		if(count(distinct session_id)>0, 1, 0) as if_hz_session,
		if(sum(if_mpl_session)>0, 1, 0) as if_mpl_session,
		if(sum(if_view_product)>0, 1, 0) as if_view_product,
		if(sum(if_cart_add)>0, 1, 0) as if_cart_add,
		if(sum(if_xo_confirm)>0, 1, 0) as if_xo_confirm
	from wandajuan.mpcon_sess_raw
	group by 1, 2, 3, 4, 5, 6
	)
group by 1, 2, 3, 4, 5
;