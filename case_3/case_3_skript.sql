/*Исходная таблица данных*/
drop table if exists oltp_src_system.account_data;
CREATE TABLE oltp_src_system.account_data (
	id int4 NULL,
	status_nm text NULL,
	create_dttm timestamp NULL,
	update_dttm timestamp NULL
);

   
/*Функция создания данных*/      
CREATE OR REPLACE FUNCTION oltp_src_system.create_tasks()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN
insert into oltp_src_system.account_data (id, status_nm, create_dttm, update_dttm)
select id
     , 'opened' status_nm
     , now() create_dttm
     , now() update_dttm
  from (select n id
          from generate_series((select coalesce((select max(id) 
                                  from oltp_src_system.account_data) + 1, 1)
                                )
                              , (select coalesce((select max(id) 
                                                    from oltp_src_system.account_data) + 1, 1)
                              ) + round(random()*3)::int
                    ) n) nn;

    get diagnostics v_rc = row_count;           
    raise notice '% rows inserted into account_data',v_rc;
    return true;

end $function$
;

/*Функция удаления данных*/     
CREATE OR REPLACE FUNCTION oltp_src_system.deleted_existed_task()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN                   
delete from oltp_src_system.account_data
where id in (
             select id
               from (select id 
                          , round(random()*100) rnd
                       from oltp_src_system.account_data
                    ) rnd_tbl
              where (rnd - floor(rnd/100)) = 1
            );

    get diagnostics v_rc = row_count;           
    raise notice '% rows deleted into account_data',v_rc;
    return true;

end $function$
;

/*Функция обновления данных*/     
CREATE OR REPLACE FUNCTION oltp_src_system.update_existed_task()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
BEGIN                   
update oltp_src_system.account_data
set status_nm = case (floor(random() * (4 - 1 + 1) + 1)::int)
                     when 1 then 'blocked'
                     when 2 then 'aressted'
                     when 3 then 'reserved'
                     when 4 then 'closed'
                     else null
                 end 
    , update_dttm = now()
where id in (select id
               from (select id 
                          , round(random()*10) rnd
                       from oltp_src_system.account_data
                      where status_nm not in ('closed')
                    ) rnd_tbl
              where (rnd - floor(rnd/10)) = 1);

    get diagnostics v_rc = row_count;           
    raise notice '% rows updated into account_data' ,v_rc;
    return true;

end $function$
;

/*Лог изменений*/     
drop table if exists oltp_cdc_src_system.account_data_changes;
CREATE TABLE oltp_cdc_src_system.account_data_changes (
	id int4 NOT NULL,
	status_nm text NULL,
	create_dttm timestamptz NULL,
	update_dttm timestamptz NULL,
	operation_type bpchar(1) NULL,
	updated_dttm timestamptz NULL
);

/*Запись в лог изменений*/  
CREATE OR REPLACE FUNCTION oltp_cdc_src_system.account_data_changes()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    begin
	    if (tg_op = 'DELETE') then
            insert into oltp_cdc_src_system.account_data_changes select old.*, 'D', now();
            return old;
        elsif (tg_op = 'UPDATE') then
            insert into oltp_cdc_src_system.account_data_changes select new.*, 'U', now();
            return new;
        elsif (tg_op = 'INSERT') then
            insert into oltp_cdc_src_system.account_data_changes select new.*, 'I', now();
            return new;
        end if;
        return null;
    end;
$function$
;

-- Table Triggers
create trigger account_data_changes after
insert
    or
delete
    or
update
    on
    oltp_src_system.account_data for each row execute function oltp_cdc_src_system.account_data_changes();

/*первый слой прототипа хранилища данных*/  
drop table if exists dwh_stage.account_data_src;
create table dwh_stage.account_data_src(
	id int4 not null,
	status_nm text null,
	create_dttm timestamptz null,
	update_dttm timestamptz null,
	operation_type bpchar(1) null,
	updated_dttm  timestamptz null,
	hash bytea NULL GENERATED ALWAYS AS 
	(digest((((
		COALESCE(status_nm, '#$%^&'::text)
		||
		date_part('epoch'::text, COALESCE(timezone('UTC'::text, create_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text)
		||
		date_part('epoch'::text, COALESCE(timezone('UTC'::text, update_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text)
		||
		COALESCE(operation_type, '#$%^&'::text)) 
		||
		date_part('epoch'::text, COALESCE(timezone('UTC'::text, updated_dttm), '1990-01-01 00:00:00'::timestamp without time zone))::text
		, 'sha256'::text))
	stored
);

/*Функция загрузки в Stage слой*/  
CREATE OR REPLACE FUNCTION dwh_stage.load_stage_from_oltp_cdc_src_system()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_rc int;
begin
	truncate dwh_stage.account_data_src;
insert into dwh_stage.account_data_src (id, status_nm, create_dttm, update_dttm, operation_type, updated_dttm)
select id
     , status_nm
     , create_dttm
     , update_dttm
     , operation_type
     , updated_dttm 
	from oltp_cdc_src_system.account_data_changes src;

    get diagnostics v_rc = row_count;           
    raise notice '% rows inserted into account_data_src',v_rc;
    return true;

end $function$
;

/*Слой поиска изменений в логе*/ 
drop table if exists cdc_from_stage_to_ods.stage_account_data_cdc;
CREATE TABLE cdc_from_stage_to_ods.stage_account_data_cdc (
	id int4 not null,
	status_nm text null,
	create_dttm timestamptz null,
	update_dttm timestamptz null,
	operation_type bpchar(1) null,
	updated_dttm  timestamptz null,
	hash bytea NULL,
	load_dttm timestamp NOT null
);

/*Функция загрузки в слой поиска изменений*/  
CREATE OR REPLACE FUNCTION cdc_from_stage_to_ods.load_stage_account_data_cdc()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
    declare
        v_load_dttm timestamp = now();
    begin

        truncate table cdc_from_stage_to_ods.stage_account_data_cdc;
       
        insert into cdc_from_stage_to_ods.stage_account_data_cdc( 
          id 
          , status_nm
          , create_dttm 
          , update_dttm 
          , operation_type 
          , updated_dttm  
          , hash
          , load_dttm
        )
        select id 
          , status_nm
          , create_dttm 
          , update_dttm 
          , operation_type 
          , updated_dttm  
          , hash
          , v_load_dttm load_dttm
        FROM dwh_stage.account_data_src src
		WHERE NOT EXISTS (SELECT NULL 
		                  FROM dwh_ods.account_data_hist dst
		                  WHERE dst.id = src.id
						  AND src.hash IN (SELECT hash 
						                       FROM dwh_ods.account_data_hist dst));
						                      
        return true;     
    
    end
$function$
;

/*Детальный слой*/
drop table if exists dwh_ods.account_data_hist;
create table dwh_ods.account_data_hist(
	id int4 not null,
	status_nm text null,
	create_dttm timestamptz null,
	update_dttm timestamptz null,
	operation_type bpchar(1) null,
	updated_dttm  timestamptz null,
	hash bytea NULL,
	load_dttm timestamp null,
	valid_from_dttm timestamptz NULL,
	valid_to_dttm timestamptz NULL,
	deleted_flg bpchar(1) null,
	deleted_dttm timestamptz null
);

/*Функция загрузки в детальный слой*/  
CREATE OR REPLACE FUNCTION dwh_ods.load_to_dwh_stage_account_data_hist()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

    declare
        v_load_dttm timestamp = now();

    begin
        update dwh_ods.account_data_hist adh
           set valid_to_dttm = now() - interval '1 second'
         where exists (select null
                       from cdc_from_stage_to_ods.stage_account_data_cdc src
                       where src.operation_type in ('U', 'D')
                      AND adh.id = src.id);
        
        insert into dwh_ods.account_data_hist (
            id 
            , status_nm 
            , create_dttm 
            , update_dttm 
            , operation_type 
            , updated_dttm  
			, hash 
			, load_dttm 
			, valid_from_dttm
			, valid_to_dttm 
			, deleted_flg 
			, deleted_dttm
        )
        select id 
             , status_nm
             , create_dttm
             , update_dttm
	         , operation_type 
	         , updated_dttm  
			 , hash 
			 , load_dttm 
             , now() valid_from_dttm
             , TO_TIMESTAMP('2999/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') valid_to_dttm
             , case 
	             when operation_type = 'D' 
	             then 'Y'
                 else NULL
             end deleted_flg
             , case 
	             when operation_type = 'D' 
	             then updated_dttm 
                 else null
             end deleted_dttm
        from cdc_from_stage_to_ods.stage_account_data_cdc ;
                
        return true;
    end
$function$
;


/*Аналитический справочник календарь*/
drop table if exists dwh_ods.dim_date;
CREATE TABLE dwh_ods.dim_date (
	date_key int4 NOT NULL,
	date_actual date NOT NULL,
	"year" int4 NOT NULL,
	quarter int4 NOT NULL,
	"month" int4 NOT NULL,
	week int4 NOT NULL,
	day_of_month int4 NOT NULL,
	day_of_week int4 NOT NULL,
	is_weekday bool NOT NULL,
	is_holiday bool NOT NULL,
	fiscal_year int4 NOT NULL,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);


/*Функция загрузки в календарь*/  
CREATE OR REPLACE FUNCTION dwh_ods.dim_date_uploading()
RETURNS BOOLEAN 
LANGUAGE plpgsql
AS $function$
DECLARE 
/* Устанавливаем начальную и конечную дату календаря, разбивем по дню. */
  start_date timestamp  := (SELECT DISTINCT MIN((SELECT DISTINCT MIN(date_trunc('day',create_dttm))
												FROM dwh_ods.account_data_hist) ) 
							FROM dwh_ods.account_data_hist);
						
  end_date timestamp := (SELECT DISTINCT MAX((SELECT DISTINCT max(date_trunc('day',create_dttm))
												FROM dwh_ods.account_data_hist) ) 
						FROM dwh_ods.account_data_hist);
BEGIN   
	truncate dwh_ods.dim_date;
  WHILE start_date <= end_date LOOP
    INSERT INTO dwh_ods.dim_date(
      date_key
      , date_actual
      , "year"
      , quarter
      , "month"
      , week
      , day_of_month
      , day_of_week
      , is_weekday
      , is_holiday
      , fiscal_year
    )
    SELECT 
      to_char(start_date, 'YYYYMMDD')::int date_key
      , start_date AS date_actual
      , DATE_PART('year', start_date) AS "year"
      , DATE_PART('quarter', start_date) AS quarter
      , DATE_PART('month', start_date) AS "month"
      , DATE_PART('week', start_date) AS week
      , DATE_PART('day', start_date) AS day_of_month
      /*Monday is 1, Sunday is 7*/
      , DATE_PART('isodow', start_date) AS day_of_week
      , CASE 
        	WHEN DATE_PART('isodow', start_date) BETWEEN 1 AND 5 THEN TRUE
        	ELSE FALSE
      	END AS is_weekday
      , CASE
       		WHEN DATE_PART('isodow', start_date) IN (6, 7) THEN TRUE
        	ELSE FALSE
      	END AS is_holiday
      , DATE_PART('year', start_date) AS fiscal_year
     ;
     
      start_date := start_date + INTERVAL '1 day';
  END LOOP;
  RETURN TRUE;
END;
$function$;


/*Отчет о крайне актуальных записях*/
drop table if exists report.account_data_recent_changes;
create table report.account_data_recent_changes (
    id int4 not null,
    status_nm text null,
    create_dttm timestamp null,
    update_dttm timestamp null,
    operation_type bpchar(1) not null,
    load_dttm timestamp not null,
    hash bytea null,
    valid_from_dttm timestamp null,
    valid_to_dttm timestamp null
);

/*Функция загрузки в отчет*/  
CREATE OR REPLACE FUNCTION report.load_account_data_recent_changes()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
begin
  	truncate table report.account_data_recent_changes;
 	insert into report.account_data_recent_changes (
            id
            , status_nm
            , create_dttm
            , update_dttm
            , operation_type
            , load_dttm
            , hash
            , valid_from_dttm
            , valid_to_dttm
      )
      select  id
      		, status_nm
      		, create_dttm
      		, update_dttm
            , operation_type
            , load_dttm, hash
            , valid_from_dttm
            , valid_to_dttm
        from dwh_ods.account_data_hist ods
        where ods.valid_to_dttm > now();
        return true;
    end $function$
;
--select * from report.load_account_data_recent_changes();
--select * from report.account_data_recent_changes;


/*Отчет по всем соверщенным операция в этот день*/
drop table if exists report.full_opeartions_report;
create table report.full_opeartions_report(
	insert_counts int not NULL
	, update_counts int not NULL
	, delete_counts int not null
);

/*Функция загрузки в отчет*/  
CREATE OR REPLACE FUNCTION report.load_full_opeartions_report()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
begin
  	truncate table report.full_opeartions_report;
 	insert into report.full_opeartions_report (
            insert_counts
            , update_counts
            , delete_counts
      )
      select (select count(1) 
            	 from dwh_ods.account_data_hist dst
            	where dst.operation_type = 'I') insert_counts
             , (select count(1) 
            	 from dwh_ods.account_data_hist dst
            	where dst.operation_type = 'U') update_counts
             , (select count(1) 
            	 from dwh_ods.account_data_hist dst
            	where dst.operation_type = 'D') delete_counts
       ;
        return true;
    end $function$
;

--select * from report.load_full_opeartions_report();
--select * from report.full_opeartions_report;

/*Отчет о количестве совершенных операция в зависимости от дня*/
drop table if exists report.day_dependence;
create table report.day_dependence(
	date_key int not NULL
	, date_actual date not null
	, is_holiday bool not null
	, is_weekday bool not null
	, day_of_month int not null
	, day_of_week int not null
	, day_of_week_nm text not null
	, full_that_day_operations int not null
	, insert_that_day_counts int not NULL
	, update_that_day_counts int not NULL
	, delete_that_day_counts int not null
);

/*Функция загрузки в отчет*/  
CREATE OR REPLACE FUNCTION report.load_day_dependence()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
begin
  	truncate table report.day_dependence;
 	insert into report.day_dependence (
            date_key 
			, date_actual 
			, is_holiday 
			, is_weekday 
			, day_of_month
			, day_of_week
			, day_of_week_nm
			, full_that_day_operations 
			, insert_that_day_counts 
			, update_that_day_counts 
			, delete_that_day_counts 
      )
      select src.date_key
             , src.date_actual
             , src.is_holiday 
			 , src.is_weekday 
			 , src.day_of_month 
			 , src.day_of_week 
			 , case when day_of_week = 1 then 'Monday'
			 		when day_of_week = 2 then 'Tuesday'
			 		when day_of_week = 3 then 'Wednesday'
			 		when day_of_week = 4 then 'Thursday'
			 		when day_of_week = 5 then 'Friday'
			 		when day_of_week = 6 then 'Saturday'
			 		when day_of_week = 7 then 'Sunday'
			 	end day_of_week_nm
			 , (select count(1) 
	               from dwh_ods.account_data_hist dst
	              where date_trunc('day', dst.create_dttm) = date_trunc('day', src.date_actual)) full_that_day_operations
             , (select count(1) 
	               from dwh_ods.account_data_hist dst
	              where dst.operation_type = 'I'
	              and date_trunc('day', dst.create_dttm) = date_trunc('day', src.date_actual)) insert_counts
	              
             , (select count(1) 
	               from dwh_ods.account_data_hist dst
	              where dst.operation_type = 'U'
	              and date_trunc('day', dst.create_dttm) = date_trunc('day', src.date_actual)) update_counts
             , (select count(1) 
	               from dwh_ods.account_data_hist dst
	              where dst.operation_type = 'D'
	              and date_trunc('day', dst.create_dttm) = date_trunc('day', src.date_actual)) delete_counts
        from dwh_ods.dim_date src;
        return true;
    end $function$
;

--select * from report.load_day_dependence();
--select * from report.day_dependence;


/*Отчет о данных из детального слоя*/
DROP TABLE IF EXISTS report.dependencies;
CREATE TABLE report.dependencies (
  percentage_blocked NUMERIC(5,3) NOT NULL,
  percentage_arrested NUMERIC(5,3) NOT NULL,
  percentage_reserved NUMERIC(5,3) NOT NULL,
  percentage_closed NUMERIC(5,3) NOT NULL,
  reopened_account INTEGER NOT NULL
);

/*Функция загрузки в отчет*/  
CREATE OR REPLACE FUNCTION report.load_dependencies()
RETURNS boolean
LANGUAGE plpgsql
AS $function$
BEGIN
    TRUNCATE table report.dependencies;
    INSERT INTO report.dependencies (
        percentage_blocked,
        percentage_arrested,
        percentage_reserved,
        percentage_closed,
        reopened_account
    )
    SELECT 
	    (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'blocked') * 1.0
	      / 
	    (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'opened') * 100.0 AS percentage_blocked,
        
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'arrested') * 1.0
        	/ 
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'opened') * 100.0 AS percentage_arrested,
        
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'reserved') * 1.0
        	/ 
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'opened') * 100.0 AS percentage_reserved,
        
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'closed') * 1.0
        	/ 
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src WHERE src.status_nm = 'opened') * 100.0 AS percentage_closed,
        
        (SELECT COUNT(1) FROM dwh_ods.account_data_hist src 
        	where EXISTS (SELECT 1 
        				FROM dwh_ods.account_data_hist dst
        				WHERE dst.status_nm = 'closed' AND src.id = dst.id) 
        	AND src.status_nm = 'opened')
        AS reopened_account
    ;
    RETURN true;
END
$function$
;

--select * from report.load_dependencies();
--select * from report.dependencies;


--truncate oltp_src_system.account_data, oltp_cdc_src_system.account_data_changes ,cdc_from_stage_to_ods.stage_account_data_cdc,
--dwh_stage.account_data_src , dwh_ods.account_data_hist, dwh_ods.dim_date;

/*Вывод содержимого*/
--select * from oltp_src_system.account_data ad;
--select * from oltp_cdc_src_system.account_data_changes adc;
--select * from dwh_stage.account_data_src ads;
--select * from cdc_from_stage_to_ods.stage_account_data_cdc sadc;
--select * from dwh_ods.account_data_hist adh;

--select * from dwh_ods.dim_date dd;

/*Операции над задачами*/
--select * from oltp_src_system.create_tasks();
--select * from oltp_src_system.update_existed_task();
--select * from oltp_src_system.deleted_existed_task();

/*Загрузка в слои из oltp_cdc_src_system.account_data_changes*/
--select * from dwh_stage.load_stage_from_oltp_cdc_src_system();
--select * from cdc_from_stage_to_ods.load_stage_account_data_cdc();
--select * from dwh_ods.load_to_dwh_stage_account_data_hist();
--
--select * from dwh_ods.dim_date_uploading();


GRANT ALL PRIVILEGES ON oltp_src_system.account_data to airflow;

grant ALL PRIVILEGES ON ALL TABLES IN SCHEMA dwh_stage to airflow;
grant  ALL PRIVILEGES ON ALL TABLES IN SCHEMA dwh_ods to airflow;
grant ALL PRIVILEGES ON ALL TABLES IN SCHEMA cdc_from_stage_to_ods to airflow;
grant  ALL PRIVILEGES ON ALL TABLES IN SCHEMA report to airflow;
grant ALL PRIVILEGES ON ALL TABLES IN SCHEMA oltp_src_system to airflow;
grant  ALL PRIVILEGES ON ALL TABLES IN SCHEMA oltp_cdc_src_system to airflow;

GRANT ALL PRIVILEGES ON function oltp_src_system.create_tasks to airflow;
GRANT ALL PRIVILEGES ON function oltp_src_system.deleted_existed_task to airflow;
GRANT ALL PRIVILEGES ON function oltp_src_system.update_existed_task to airflow;
GRANT ALL PRIVILEGES ON function dwh_stage.load_stage_from_oltp_cdc_src_system to airflow;
GRANT ALL PRIVILEGES ON function cdc_from_stage_to_ods.load_stage_account_data_cdc to airflow;
GRANT ALL PRIVILEGES ON function dwh_ods.load_from_dwh_stage_account_data_hist to airflow;
GRANT ALL PRIVILEGES ON function dwh_ods.dim_date_uploading to airflow;

GRANT ALL PRIVILEGES ON function report.load_account_data_recent_changes to airflow;
GRANT ALL PRIVILEGES ON function report.load_day_dependence to airflow;
GRANT ALL PRIVILEGES ON function report.load_dependencies to airflow;
GRANT ALL PRIVILEGES ON function report.load_full_opeartions_report to airflow;


grant ALL PRIVILEGES ON SCHEMA dwh_stage to airflow;
grant  ALL PRIVILEGES ON SCHEMA dwh_ods to airflow;
grant ALL PRIVILEGES ON SCHEMA cdc_from_stage_to_ods to airflow;
grant  ALL PRIVILEGES ON SCHEMA report to airflow;
grant ALL PRIVILEGES ON SCHEMA oltp_src_system to airflow;
grant  ALL PRIVILEGES ON SCHEMA oltp_cdc_src_system to airflow;
GRANT CREATE, USAGE ON SCHEMA dwh_stage TO airflow;