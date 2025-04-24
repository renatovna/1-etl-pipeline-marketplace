with dwh_delta as (
	select 	
			dcs.customer_id,
			dcs.customer_name,
			dcs.customer_address,
			dcs.customer_birthday,
			dcs.customer_email,
			dp.product_price,
			fo.order_completion_date - fo.order_created_date as diff_order_date, 
			fo.order_id,	
			dp.product_id,
			dp.product_type,
			dc.craftsman_id,
			fo.order_status,
			to_char(fo.order_created_date, 'yyyy-mm') as report_period,
			-- две строки ниже помогут определить самый популярный товар и мастера у заказчика
			count(dp.product_id) over (partition by dcs.customer_id, dp.product_type) as count_product_for_type,
			count(dp.product_id) over (partition by dcs.customer_id, dc.craftsman_id) as count_product_for_craftsman,
			crd.customer_id as exist_customer_id,
			dc.load_dttm as craftsman_load_dttm,
			dcs.load_dttm as customers_load_dttm,
			dp.load_dttm as products_load_dttm
			from dwh.f_order fo 
				inner join dwh.d_craftsman dc on dc.craftsman_id = fo.craftsman_id
				inner join dwh.d_customer dcs on dcs.customer_id = fo.customer_id
				inner join dwh.d_product dp on dp.product_id = fo.product_id
				left join dwh.customer_report_datamart crd on crd.customer_id = dcs.customer_id
					where fo.load_dttm > (select coalesce(max(load_dttm),'1900-01-01') from dwh.load_dates_customers_report_datamart) 
                    or dc.load_dttm > (select coalesce(max(load_dttm),'1900-01-01') from dwh.load_dates_customers_report_datamart) 
					or dcs.load_dttm > (select coalesce(max(load_dttm),'1900-01-01') from dwh.load_dates_customers_report_datamart) 
					or dp.load_dttm > (select coalesce(max(load_dttm),'1900-01-01') from dwh.load_dates_customers_report_datamart)
),
dwh_update_delta as  (
	select 	
			distinct customer_id
			from dwh_delta dd 
			where exist_customer_id is not null
),
dwh_delta_insert_result as (
	select  
			t4.customer_id,
			t4.customer_name,
			t4.customer_address,
			t4.customer_birthday,
			t4.customer_email,
			t4.customer_money,
			t4.platform_money,
			t4.count_order,
			t4.avg_price_order,
			t4.median_time_order_completed,
			t4.top_product_category,
			t4.top_craftsman,
			t4.count_order_created as count_order_created,
			t4.count_order_in_progress as count_order_in_progress,
			t4.count_order_delivery as count_order_delivery,
			t4.count_order_done as count_order_done,
			t4.count_order_not_done as count_order_not_done,
			t4.report_period as report_period 
			from (
				select 	-- в этой выборке объединяем две внутренние выборки
						distinct t2.*,
						t3.top_product_category,
						t3.top_craftsman
						from ( 
							select 
								t1.customer_id, 
								t1.customer_name, 
								t1.customer_address, 
								t1.customer_birthday, 
								t1.customer_email, 
								t1.report_period,
								sum(product_price) as customer_money,
								sum(product_price)*0.1 as platform_money,
								count(distinct order_id) count_order,
								avg(product_price) as avg_price_order,
								percentile_cont(0.5) within group(order by diff_order_date) median_time_order_completed,
								sum(case when order_status = 'created' then 1 else 0 end) count_order_created,
								sum(case when order_status = 'in progress' then 1 else 0 end) count_order_in_progress,
								sum(case when order_status = 'delivery' then 1 else 0 end) count_order_delivery,
								sum(case when order_status = 'done' then 1 else 0 end) count_order_done,
								sum(case when order_status != 'done' then 1 else 0 end) count_order_not_done
								from dwh_delta as t1
									where t1.exist_customer_id is null
										group by t1.customer_id, 
											t1.customer_name, 
											t1.customer_address, 
											t1.customer_birthday, 
											t1.customer_email, 
											t1.report_period
							) as t2 
								inner join ( --опредеяем популярную категорию и мастера по заказчику
									select distinct	customer_id, 
									first_value(product_type) over(partition by customer_id order by count_product_for_type desc) as top_product_category,
									first_value(craftsman_id) over(partition by customer_id order by count_product_for_craftsman desc) as top_craftsman
									from dwh_delta) as t3 
									on t2.customer_id = t3.customer_id
				) as t4 order by report_period
),
--выборка для обновления существующих данных по заказчикам
dwh_delta_update_result as (
	select 
			t4.customer_id,
			t4.customer_name,
			t4.customer_address,
			t4.customer_birthday,
			t4.customer_email,
			t4.customer_money,
			t4.platform_money,
			t4.count_order,
			t4.avg_price_order,
			t4.median_time_order_completed,
			t4.top_product_category,
			t4.top_craftsman,
			t4.count_order_created as count_order_created,
			t4.count_order_in_progress as count_order_in_progress,
			t4.count_order_delivery as count_order_delivery,
			t4.count_order_done as count_order_done,
			t4.count_order_not_done as count_order_not_done,
			t4.report_period as report_period 
			from (
				select 	-- в этой выборке объединяем две внутренние выборки
						distinct t2.*,
						t3.top_product_category,
						t3.top_craftsman
						from (
							select 
								t1.customer_id,
								t1.report_period,
								t1.customer_name,
								t1.customer_address,
								t1.customer_birthday,
								t1.customer_email,
								sum(product_price) as customer_money,
								sum(product_price)*0.1 as platform_money,
								count(distinct order_id) count_order,
								avg(product_price) as avg_price_order,
								percentile_cont(0.5) within group(order by diff_order_date) median_time_order_completed,
								sum(case when order_status = 'created' then 1 else 0 end) count_order_created,
								sum(case when order_status = 'in progress' then 1 else 0 end) count_order_in_progress,
								sum(case when order_status = 'delivery' then 1 else 0 end) count_order_delivery,
								sum(case when order_status = 'done' then 1 else 0 end) count_order_done,
								sum(case when order_status != 'done' then 1 else 0 end) count_order_not_done
								from (
									select 	-- в этой выборке достаём из dwh обновлённые или новые данные по заказчикам
											dcs.customer_id,
											dcs.customer_name,
											dcs.customer_address,
											dcs.customer_birthday,
											dcs.customer_email,
											fo.order_id as order_id,
											dp.product_id as product_id,
											dp.product_price as product_price,
											dp.product_type as product_type,
											fo.order_completion_date - fo.order_created_date as diff_order_date,
											fo.order_status as order_status, 
											to_char(order_created_date, 'yyyy-mm') as report_period
											from dwh.f_order fo 
												inner join dwh.d_craftsman dc on fo.craftsman_id = dc.craftsman_id 
												inner join dwh.d_customer dcs on fo.customer_id = dcs.customer_id 
												inner join dwh.d_product dp on fo.product_id = dp.product_id
												inner join dwh_update_delta ud on fo.customer_id = ud.customer_id
								) as t1
									group by t1.customer_id,
									t1.customer_name,
									t1.customer_address,
									t1.customer_birthday,
									t1.customer_email, t1.report_period
							) as t2 
								inner join ( --опредеяем популярную категорию и мастера по заказчику
									select distinct	customer_id, 
									first_value(product_type) over(partition by customer_id order by count_product_for_type desc) as top_product_category,
									first_value(craftsman_id) over(partition by customer_id order by count_product_for_craftsman desc) as top_craftsman
									from dwh_delta) as t3 
									on t2.customer_id = t3.customer_id
				) as t4 order by report_period
),				
insert_delta as (
insert into dwh.customer_report_datamart 
(customer_id, customer_name, customer_address, customer_birthday, customer_email, customer_money, platform_money, count_order, avg_price_order, top_product_category, top_craftsman, median_time_order_completed, count_order_created, count_order_in_progress, count_order_delivery, count_order_done, count_order_not_done, report_period)
select customer_id, customer_name, customer_address, customer_birthday, customer_email, customer_money, platform_money::bigint, count_order, avg_price_order, top_product_category, top_craftsman, median_time_order_completed, count_order_created, count_order_in_progress, count_order_delivery, count_order_done, count_order_not_done, report_period
from dwh_delta_insert_result
),
update_delta AS (
	UPDATE dwh.customer_report_datamart tableA SET
		customer_name = tableB.customer_name, 
		customer_address = tableB.customer_address, 
		customer_birthday = tableB.customer_birthday,
        customer_email = tableB.customer_email,
        customer_money = tableB.customer_money,
        platform_money = tableB.platform_money,
        count_order = tableB.count_order,
        avg_price_order = tableB.avg_price_order,
        median_time_order_completed = tableB.median_time_order_completed,
        top_product_category = tableB.top_product_category,
        top_craftsman = tableB.top_craftsman,
        count_order_created = tableB.count_order_created,
        count_order_in_progress = tableB.count_order_in_progress,
        count_order_delivery = tableB.count_order_delivery,
        count_order_done = tableB.count_order_done,
        count_order_not_done = tableB.count_order_not_done,
        report_period = tableB.report_period
	FROM (
		SELECT 
            customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
            customer_money,  platform_money, count_order,
            avg_price_order, top_craftsman, median_time_order_completed,
            top_product_category, count_order_created,
            count_order_in_progress, count_order_delivery, count_order_done
            ,count_order_not_done,report_period
        from dwh_delta_update_result) AS tableB
	WHERE tableA.customer_id = tableB.customer_id
),
insert_load_date AS (
	INSERT INTO dwh.load_dates_customers_report_datamart (
		load_dttm
	)
	    select coalesce(greatest(max(craftsman_load_dttm), max(customers_load_dttm), max(products_load_dttm)), now())
		FROM dwh_delta
)
SELECT 'increment datamart';
