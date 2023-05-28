
/* Собираем все физ лица из MDM системы */
drop table if exists dfct_phone_0010;
create table dfct_phone_0010  as 
select 
	c_.counterparty_rk,
	c_.effective_from_date,
	c_.effective_to_date,
	c_.counterparty_type_cd,
	c_.src_cd
from counterparty c_

inner join dict_counterparty_type_cd dctc_ 
	on dctc_.counterparty_type_cd = c_.counterparty_type_cd
	and dctc_.counterparty_type_desc = 'физическое лицо'
	and dctc_ .src_cd = 'MDMP'
where c_.src_cd = 'MDMP';

select * from dfct_phone_0010 dp;

drop table if exists dfct_phone_0020;
create table dfct_phone_0020 as 
with dfct_phone_0020_1 as (
	select 
		cc_.counterparty_rk,
		cc_.effective_from_date,
		cc_.effective_to_date,
		cc_.contact_type_cd as phone_type_cd,
		cc_.contact_desc as phone_num,
		
		cc_.trust_system_flg::int4,
		
		case when cc_.contact_quality_code ~*'GOOD' then 1 else 0 end as GOOD_flg, 
		
		case when cc_.contact_type_cd = 'NotificPhone' then 1 else 0 end as notification_flg,
		case when cc_.contact_type_cd = 'ATMPhone' then 1 else 0 end as atm_flg,
		case when cc_.contact_type_cd = 'MobilePersonalPhone' then 1 else 0 end as mobilePersonal_flg,
		case when cc_.contact_type_cd = 'MobileWorkNumber' then 1 else 0 end as mobileWork_flg,
		case when cc_.contact_type_cd = 'HomePhone' then 1 else 0 end as home_flg,
			
		case when cc_.src_cd = 'MDMP' then 1 else 0 end as MDMP_flg,
		case when cc_.src_cd = 'WAYN' then 1 else 0 end as WAYN_flg,
		case when cc_.src_cd = 'RTLL' then 1 else 0 end as RTLL_flg,
		case when cc_.src_cd = 'RTLS' then 1 else 0 end as RTLS_flg,
		case when cc_.src_cd = 'CFTB' then 1 else 0 end as CFTB_flg,
		
		case 
			when cc_.effective_from_date = max(cc_.effective_from_date) over (partition by cc_.contact_desc order by 1)
  			then 1
  			else 0 
  		end as date_weight_flg,	
		
		cc_.contact_quality_code,
		cc_.src_cd
	from counterparty_contact cc_	
	where cc_.src_cd in ('MDMP', 'RTLL', 'RTLS', 'CFTB', 'WAYN')
)
--select * from dfct_phone_0020_1
,

/* Джоиним MDM контакты с физ лицами */
dfct_phone_0020_2 as (
	select src_ .*
	from dfct_phone_0020_1 src_
	inner join dfct_phone_0010 dp_1_
		on dp_1_.counterparty_rk = src_.counterparty_rk
		and dp_1_.effective_to_date >= now()
		and dp_1_.effective_from_date < now()
	where src_.src_cd = 'MDMP'
)
--select * from dfct_phone_0020_2
,

/* Джоиним все контакты кроме MDM сначала с мостом потом с физ лицами */
dfct_phone_0020_3 as (
	select 
		cxuc_.uniq_counterparty_rk as counterparty_rk,
		src_.effective_from_date,
		src_.effective_to_date,
		src_.phone_type_cd,
		src_.phone_num,
		
		src_.trust_system_flg,
		
		src_.GOOD_flg, 
		
		src_.notification_flg,
		src_.atm_flg,
		src_.mobilePersonal_flg,
		src_.mobileWork_flg,
		src_.home_flg,
		
		src_.MDMP_flg,
		src_.WAYN_flg,
		src_.RTLL_flg,
		src_.RTLS_flg,
		src_.CFTB_flg,
		
		src_.date_weight_flg,
		src_.contact_quality_code,
		src_.src_cd
		
	from dfct_phone_0020_1 src_
	inner join counterparty_x_uniq_counterparty cxuc_
		on cxuc_.counterparty_rk = src_.counterparty_rk
		and cxuc_.effective_to_date >= now()
		and cxuc_.effective_from_date < now()
	inner join dfct_phone_0010 fdtd_
		on fdtd_.counterparty_rk = cxuc_.uniq_counterparty_rk 
		and fdtd_.effective_to_date >= now()
		and fdtd_.effective_from_date < now()
	where src_.src_cd != 'MDMP'
)
--select * from dfct_phone_0020_3
,

/*Обьединяем все системы в общую таблицу */
dfct_phone_0020_4 as (
	select * from dfct_phone_0020_3
	union
	select * from dfct_phone_0020_2
)
--select * from dfct_phone_0020_4
,

/*Находим duplication_flg  */
dfct_phone_0020_5 as (
	select 
		src_ .*,
		case 
			when (count(src_.phone_num) over (partition by src_.phone_num, src_.effective_to_date order by 1,2 ) > 1)
			then 1 
			else 0 
		end as duplication_flg
	from dfct_phone_0020_4 src_ 
),

dfct_phone_0020_6 as (
	select src_.*,
	/* Расчет приоритетов с учетом флагов */
		(
		(src_.effective_to_date >= now())::int4*100000 +
		src_.trust_system_flg*10000 + 
		src_.GOOD_flg*1000 +
		src_.MDMP_flg*500 + src_.WAYN_flg*400 + src_.RTLL_flg*300 + src_.RTLS_flg*200 + src_.CFTB_flg*100 +
		src_.notification_flg*50 + src_.atm_flg*40 + src_.mobilePersonal_flg*30 + src_.mobileWork_flg*20 + src_.home_flg*10 +
		src_.date_weight_flg) as weight
	from dfct_phone_0020_5 src_ 
)
--select * from dfct_phone_0020_6
, 
	/* Расчитывем  main_dup_flg main_phone_flg */
dfct_phone_0020_7 as (
	select 
		src_.counterparty_rk,
		src_.phone_type_cd,
		src_.effective_from_date,
		src_.effective_to_date,
		src_.phone_num,
		src_.notification_flg,
		src_.atm_flg,	
		src_.trust_system_flg,
		src_.duplication_flg,
		
		case when src_.duplication_flg = 1 and src_.weight = max(src_.weight) over (partition by src_.phone_num order by 1)
	        then 1
	        when src_.duplication_flg = 1 and src_.weight != max(src_.weight) over (partition by src_.phone_num order by 1)
			then 0
		    when src_.duplication_flg = 0
	        then 1
	    end as main_dup_flg,
	      
	    case when src_.weight = max(src_.weight) over (partition by src_.counterparty_rk)
	       	then 1
	       	else 0
	    end as main_phone_flg,
	       
		src_.contact_quality_code,
		src_.src_cd
		
	from dfct_phone_0020_6 src_
) 
select 
	src_.counterparty_rk,
	src_.phone_type_cd,
	src_.effective_from_date,
	src_.effective_to_date,
	src_.phone_num,
	src_.notification_flg,
	src_.atm_flg,	
	src_.trust_system_flg,
	src_.duplication_flg,
	src_.main_dup_flg,
    src_.main_phone_flg,
	src_.contact_quality_code,
	
	case
		when (max(src_.notification_flg) over (partition by src_.counterparty_rk, src_.phone_num 
			order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.notification_flg 
			or (max(src_.notification_flg) over (partition by src_.counterparty_rk, src_.phone_num 
				order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.notification_flg is null))
				
		and (max(src_.atm_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
	         rows between 1 preceding and 1 preceding) = src_.atm_flg 
			or (max(src_.atm_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.atm_flg is null))
				
		and (max(src_.trust_system_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
	         rows between 1 preceding and 1 preceding) = src_.trust_system_flg 
			or (max(src_.trust_system_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.trust_system_flg is null))
				
      and (max(src_.duplication_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.duplication_flg 
	      or (max(src_.duplication_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.duplication_flg is null))
				
      and (max(src_.main_dup_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.main_dup_flg 
	      or (max(src_.main_dup_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.main_dup_flg is null))
				
      and (max(src_.main_phone_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
	         rows between 1 preceding and 1 preceding) = src_.main_phone_flg
	      or (max(src_.main_phone_flg) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.main_phone_flg is null))
				
      and (max(src_.contact_quality_code) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) = src_.contact_quality_code 
	      or (max(src_.contact_quality_code) over (partition by src_.counterparty_rk, src_.phone_num order by src_.effective_from_date 
				rows between 1 preceding and 1 preceding) is null and src_.contact_quality_code is null))
				
      /* проверяем первую строку - если строка #1, тогда мы её оставляем, иначе, опираемся на вышепроведённое сравнение атрибутов */
		and row_number() over (partition by src_.counterparty_rk order by src_.effective_from_date) > 1
		then 0 
		else 1 
	end as FLG
		
	
from dfct_phone_0020_7 src_;

drop table if exists dfct_phone;
create table dfct_phone as 
	select 
		src_.counterparty_rk as mdm_customer_rk,
		src_.phone_type_cd,
		src_.effective_from_date as business_start_dt,
		coalesce(max(src_.effective_from_date) over ( /* достраиваем дату завершения версии */
			partition by src_.counterparty_rk 
			order by src_.effective_from_date rows between 1 following and 1 following)
		,cast('2999-12-31' as date)) as business_end_dt,
		src_.phone_num,
		
		src_.notification_flg::bool,
		src_.atm_flg::bool,	
		src_.trust_system_flg::bool,
		src_.duplication_flg::bool,
	
		src_.main_dup_flg::bool,
		
        src_.main_phone_flg::bool
        		
	from dfct_phone_0020 src_
	where src_.flg = 1;
	
--select * from dfct_phone dp



