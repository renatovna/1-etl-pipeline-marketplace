drop table if exists dwh.customer_report_datamart;
create table if not exists dwh.customer_report_datamart (
	id bigint generated always as identity primary key,
	customer_id bigint not null,
    customer_name text not null,
    customer_address text not null,
    customer_birthday date  not null,
    customer_email text not null,
    customer_money numeric(15,2) not null, 	--сумма, которую потратил заказчик;
    platform_money bigint not null, 		--сумма, которую заработала платформа от покупок заказчика 
    count_order bigint not null, 			--количество заказов у заказчика за месяц;
	avg_price_order numeric(10,2) not null, --средняя стоимость одного заказа у заказчика за месяц;
	median_time_order_completed numeric(10,1), --медианное время в днях от момента создания заказа до его завершения за месяц;
	top_product_category varchar not null, --самая популярная категория товаров у этого заказчика за месяц;
	top_craftsman varchar not null, --идентификатор самого популярного мастера ручной работы у заказчика
	count_order_created BIGINT NOT NULL, --количество созданных заказов за месяц;
	count_order_in_progress BIGINT NOT NULL, --количество заказов в процессе изготовки за месяц;
	count_order_delivery BIGINT NOT NULL, --количество заказов в доставке за месяц;
	count_order_done BIGINT NOT NULL, --количество завершённых заказов за месяц;
	count_order_not_done BIGINT NOT NULL, --количество незавершённых заказов за месяц;
	report_period VARCHAR NOT NULL -- отчётный период год и месяц
)

