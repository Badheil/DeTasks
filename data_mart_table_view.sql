-- DROP FUNCTION std10_44.f_load_mart_table(int4);

CREATE OR REPLACE FUNCTION std10_44.f_load_mart_table(p_month int4)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE 
	v_table_name TEXT;
	v_sql TEXT;
	v_return INT;
	v_view_name TEXT;
    v_month TEXT;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
	

BEGIN

	 /* perform std10_44.f_load_write_log_2(p_log_type := 'INFO',
									 p_log_message := 'Start f_load_mart',
									 p_location := 'std10_44.f_load_mart_2'); */

	IF p_month >= 10 THEN
		v_month := CAST(p_month AS text);
	ELSE
		v_month := '0' || CAST(p_month AS text);
	END IF;

	v_table_name := 'std10_44.plan_fact_2021'|| v_month;

	v_load_interval := '1 month'::INTERVAL;
	v_start_date := CAST('2021-' || v_month || '-01' AS timestamp);
	v_end_date := v_start_date + v_load_interval;
				
				
	v_sql = 'DROP TABLE IF EXISTS '||v_table_name|| ' CASCADE;
			 CREATE TABLE '||v_table_name||'
				WITH (
					appendonly=TRUE,
					orientation=COLUMN,
					compresstype=zstd,
					compresslevel=1)
				AS 
					with t1 as (
            select s.region, p.matdirec, s.distr_chan, sum(s.quantity) as fact
			from sales s inner join product p on s.material=p.material
			WHERE s.date >= ''' || v_start_date || ''' AND s.date < ''' || v_end_date || '''
			group by 1,2,3),

					t2 as (
			select s.region, s.material, row_number() over(partition by region order by sum(quantity) desc) as row_num
			from sales s
			WHERE s.date >= ''' || v_start_date || ''' AND s.date < ''' || v_end_date || '''
			group by 1,2
			order by 3
			limit 4
					)

			select p.region as region, 
				   p.matdirec as matdirec, 
			       p.distr_chan as distr_chan,
				   p.quantity as plan_qnt,
			       coalesce(t1.fact, 0) as fact_qnt,
			       round(coalesce(t1.fact::numeric/p.quantity::numeric*100, 0),2) as percent_of_plan, 
			       t2.material as best_selling_reg
			from std10_44.plan p 
			left join t1 on p.region=t1.region 
			and p.matdirec=t1.matdirec 
			and p.distr_chan=t1.distr_chan 
			left join t2 on p.region=t2.region
	WHERE (p.date >= ''' || v_start_date || ''') AND (p.date <= ''' || v_end_date || ''')
									   
    DISTRIBUTED RANDOMLY;';
	
	RAISE NOTICE 'LOAD_MART TABLE IS: %', v_sql;

	EXECUTE v_sql;
		
	EXECUTE 'SELECT count(*) FROM '||v_table_name INTO v_return;

	RETURN v_return;
	
	 /* perform std10_44.f_load_write_log_2(p_log_type := 'INFO',
									 p_log_message := v_return || ' rows inserted',
									 p_location := 'std10_44.f_load_mart_2'); */

	 /* perform std10_44.f_load_write_log_2(p_log_type := 'INFO',
									 p_log_message := 'End f_load_mart',
									 p_location := 'std10_44.f_load_mart_2');  */

END;

$$
EXECUTE ON ANY;


CREATE OR REPLACE FUNCTION std10_44.f_create_data_mart_view(p_month int4)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare 
	v_month_text text;
	v_view_name text;
	v_sql text;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
begin

	IF p_month >= 10 THEN
		v_month_text := CAST(p_month AS text);
	ELSE
		v_month_text := '0' || CAST(p_month AS text);
	END IF;

	v_view_name := 'std10_44.v_plan_fact_' || v_month_text;

	v_load_interval := '1 month'::INTERVAL;
	v_start_date := CAST('2021-' || v_month_text || '-01' AS timestamp);
	v_end_date := v_start_date + v_load_interval;

	v_sql := 'DROP VIEW IF EXISTS ' || v_view_name;
	RAISE NOTICE 'DROPPING TABLE: %', v_sql;
	EXECUTE v_sql;

	v_sql := 'CREATE VIEW ' || v_view_name || ' AS (
		SELECT d.region AS region, 
		r.txt AS region_text,
		d.matdirec AS matdirec,
		d.distr_chan AS distr_chan, c.txtsh AS chanel_text,
		d.percent_of_plan AS percentage_exec_of_plan,
		d.best_selling_reg AS best_selling_product,

		(SELECT prod.brand FROM product prod WHERE prod.material = d.best_selling_reg) AS best_selling_product_brand,
		(SELECT p.txt FROM product p WHERE p.material = d.best_selling_reg) AS best_selling_product_str,
		(SELECT AVG(p.price) FROM price p WHERE p.material = d.best_selling_reg AND p.region = d.region) AS best_selling_product_price
		FROM std10_44.plan_fact_2021' || v_month_text || ' d 
		JOIN std10_44.region r ON d.region = r.region
		JOIN std10_44.chanel c ON d.distr_chan = c.distr_chan
	);';
	RAISE NOTICE 'CREATING VIEW: %', v_sql;
	EXECUTE v_sql;

end;

$$
EXECUTE ON ANY;

