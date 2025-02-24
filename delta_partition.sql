CREATE OR REPLACE FUNCTION std10_44.f_load_simple_partition(p_table text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_pxf_table text, p_user_id text, p_pass text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
declare
	v_ext_table text; -- название внешней таблицы
	v_temp_table text; -- название временной таблицы
   v_sql text; -- команда sql
   v_pxf text; -- запрос подключения по pxf
   v_result int; -- то, что возвратит функция
   v_dist_key text; -- ключ дистрибуции
   v_params text; -- параметры целевой таблицы
   v_where text; -- условие where
   v_load_interval interval; -- какой-то интервал
   v_start_date date; --
   v_end_date date; --
   v_table_oid int4; -- уникальный id таблицы
	v_cnt int8; -- количество строк
	
begin
	
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';
	
	-- запишем уникальный id таблицы в переменную oid
	select c.oid
	into v_table_oid
	from pg_class as c inner join pg_namespace as n on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = p_table
	limit 1;
	
	-- передадим oid в переменную v_table_oid для получения ключа дистрибуции
	if v_table_oid = 0 or v_table_oid is null then
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;
	
	-- получим параметры целевой таблицы через запятую
	select coalesce('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')','')
	into v_params
	from pg_class
	where oid = p_table::REGCLASS;
	
	execute 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;
	
	v_load_interval = '1 month'::interval;
	v_start_date := DATE_TRUNC('month', p_start_date);
	v_end_date := DATE_TRUNC('month', p_start_date) + v_load_interval;
	
	-- строка для условия выборки данных из внешней таблицы
	v_where = p_partition_key ||' >= '''||v_start_date||'''::date AND '||p_partition_key||' < '''||v_end_date||'''::date';
	v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://**/postgres&USER='
					||p_user_id||'&PASS='||p_pass;
	
	raise notice 'PXF CONNECTION STRING: %', v_pxf;
	
	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table||'(LIKE '||p_table||')
			LOCATION ('''||v_pxf||'''
			) ON ALL
			FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
			ENCODING ''UTF8''';
	
	raise notice 'EXTERNAL TABLE IS: %', v_sql;
	
	execute v_sql;
	
	v_sql := 'DROP TABLE IF EXISTS '|| v_temp_table ||';
			 CREATE TABLE '|| v_temp_table ||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';
	
	raise notice 'TEMP TABLE IS: %', v_sql;
	
	execute v_sql;
	
	v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '||v_ext_table||' WHERE '||v_where;
	
	raise notice 'INSERT DATA SCRIPT: %', v_sql;
	execute v_sql;
	
	get diagnostics v_cnt = ROW_COUNT;
	raise notice 'INSERTED ROWS: %', v_cnt;
	v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '|| v_temp_table ||' WITH VALIDATION';
	
	raise notice 'EXCHANGE PARTITION SCRIPT: %', v_sql;
	
	execute v_sql;
	
	execute 'select count(1) from '||p_table||' WHERE '||v_where into v_result;
	
	return v_result;
	
end;
$$
EXECUTE ON ANY;
