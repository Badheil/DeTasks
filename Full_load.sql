CREATE OR REPLACE FUNCTION std10_44.f_full_load(p_table text, p_file_name text)
	RETURNS int8
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
	v_table_from text;
	v_gpfdist text;
	v_sql text;
	v_cnt int8;
begin
	v_table_from = p_table || '_csv';
	execute 'TRUNCATE TABLE ' || p_table;
	execute 'DROP EXTERNAL TABLE IF EXISTS ' || v_table_from;
	v_gpfdist = 'gpfdist://***/' || p_file_name;
	
	v_sql = format(
       'CREATE EXTERNAL TABLE %I (LIKE %I)
       LOCATION (''%s'') ON ALL
       FORMAT ''CSV'' (HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'')
       ENCODING ''UTF8''',
       v_table_from, p_table, v_gpfdist
   );
	
	raise notice 'external table is: %', v_sql;
	
	execute v_sql;
	
	EXECUTE format('INSERT INTO %I SELECT * FROM %I', p_table, v_table_from);
	
	EXECUTE format('SELECT COUNT(*) FROM %I', p_table) INTO v_cnt;
	
	
	return v_cnt;
end;
$$
EXECUTE ON ANY
