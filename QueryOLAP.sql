-- OLAP Model based on Specimen


-- Tables

create table INBIO.taxon_dimension(
    taxon_id int primary key,
    kingdoom_name varchar(50),
    phylum_division_name varchar(50),
    class_name varchar(50),
    order_name varchar(50),
    family_name varchar(50),
    genus_name varchar(50),
    species_name varchar(100),
    scientific_name varchar(100)
);

create table INBIO.site_dimension(
    site_id int primary key,
    latitude float not NULL,
    longitude float not NULL,
    site_description text
);


create table INBIO.gathering_dimension(
    gathering_id int primary key,
    day int not null,
    month int not null,
    year int not null
);

create table INBIO.gathering_responsible_dimension(
    gathering_responsible_id int primary key,
    name varchar(50) not NULL
);

create table INBIO.specimen_fact(
    specimen_id int primary key,
    taxon_id int references INBIO.taxon_dimension(taxon_id),
    site_id int references INBIO.site_dimension(site_id),
    gathering_id int references INBIO.gathering_dimension(gathering_id),
    gathering_responsible_id int references INBIO.gathering_responsible_dimension(gathering_responsible_id),
    specimen_count int,
    cost_sum float
);



-- Insert procedures

create or replace procedure INBIO.insertar_dimension_taxon(taxon_id int, kingdom_name varchar, phylum_division_name varchar, class_name varchar, order_name varchar, family_name varchar, genus_name varchar, species_name varchar, scientific_name varchar)
language plpgsql
as $$
begin
    insert into INBIO.taxon_dimension values(taxon_id, kingdom_name, phylum_division_name, class_name, order_name, family_name, genus_name, species_name, scientific_name)
    on conflict do nothing;
end;
$$;


create or replace procedure INBIO.insertar_dimension_site(site_id int, latitude float, longitude float, site_description text)
language plpgsql
as $$
begin
    insert into INBIO.site_dimension values(site_id, latitude, longitude, site_description)
    on conflict do nothing;
end;
$$;

create or replace procedure INBIO.insertar_dimension_gathering(gathering_id int, day int, month int, year int)
language plpgsql
as $$
begin
    insert into INBIO.gathering_dimension values(gathering_id, day, month, year)
    on conflict do nothing;
end;
$$;


create or replace procedure INBIO.insertar_dimension_gathering_responsible(gathering_responsible_id int, name varchar)
language plpgsql
as $$
begin
    insert into INBIO.gathering_responsible_dimension values(gathering_responsible_id, name)
    on conflict do nothing;
end;
$$;

create or replace procedure INBIO.insertar_facts()
language plpgsql
as $$
declare
    c cursor for select s.specimen_id, t.*, si.*, g.gathering_id, g.gathering_date, gr.*, count(s.specimen_id) as specimen_count, sum(s.specimen_cost) as cost_sum 
    FROM INBIO.site si, INBIO.taxon t, INBIO.gathering g, INBIO.gathering_responsible gr, INBIO.specimen s
    where t.taxon_id=s.taxon_id and g.gathering_id=s.gathering_id and gr.gathering_responsible_id=g.gathering_responsible_id and g.site_id=si.site_id
    group by s.specimen_id,si.site_id, t.taxon_id, g.gathering_id, gr.gathering_responsible_id;
    year int;
    month int;
    day int;
begin
    for r in c loop
        select extract(year from r.gathering_date) into year;
        select extract(month from r.gathering_date) into month;
        select extract(day from r.gathering_date) into day;
        
        call INBIO.insertar_dimension_taxon(r.taxon_id, r.kingdom_name, r.phylum_division_name, r.class_name,
        r.order_name, r.family_name, r.genus_name, r.species_name, r.scientific_name);
        call INBIO.insertar_dimension_site(r.site_id, r.latitude, r.longitude, r.site_description);
        call INBIO.insertar_dimension_gathering(r.gathering_id, day, month, year);
        call INBIO.insertar_dimension_gathering_responsible(r.gathering_responsible_id, r.name);

        insert into INBIO.specimen_fact values (r.specimen_id, r.taxon_id, r.site_id, r.gathering_id, 
        r.gathering_responsible_id, r.specimen_count, r.cost_sum)
        on conflict (specimen_id) do update set site_id=excluded.site_id, gathering_id=excluded.gathering_id, 
        gathering_responsible_id=excluded.gathering_responsible_id, specimen_count=excluded.specimen_count, cost_sum=excluded.cost_sum;
    end loop;
end;
$$;


-- Functions
--1
create or replace function orden(
    pMes int) returns refcursor
    LANGUAGE 'plpgsql' as $body$DECLARE
    cursor1 refcursor;
	bEGIN
    open cursor1 FOR select t.order_name, sum(f.specimen_count) as cantidad from Hecho_specimen f, Dimension_taxon t , Dimension_gathering g where f.taxon_id=t.taxon_id 
    and g.gathering_id=f.gathering_id and g.mes=pMes group by t.order_name order by cantidad desc;
    
	return cursor1;
    END;$body$;

--1.1		
create or replace FUNCTION fn_sum_specimen(conjunto varchar) returns float
    LANGUAGE 'plpgsql' as $body$DECLARE
    temp varchar;
	temp2 varchar;
    suma float;
    total float :=0;
    BEGIN
	temp:=conjunto;
    while POSITION( ',' in temp )>0 loop
    temp2:=substring(conjunto,1,POSITION(  ',' in conjunto )-1);
    select sum(cost_sum) into suma from Hecho_specimen where specimen_id=CAST(trim(temp2) AS int);
    total:=total+suma;
	
    temp:= trim(substr(temp,POSITION(',' in temp)+1));
	RAISE NOTICE 'total:%',temp;
    end loop;
	
    select sum(cost_sum) into suma from Hecho_specimen where specimen_id=CAST(trim(temp) AS int);
    total:=total+suma;	
    return total;
 END;$body$;

--1.2
create or replace FUNCTION fn_count_specimen(preino varchar)returns INT
LANGUAGE 'plpgsql' as $body$DECLARE
cantidad int;
BEGIN
select sum(specimen_count) into cantidad from Hecho_specimen f, Dimension_taxon t where f.taxon_id=t.taxon_id and t.kingdoom_name=preino;
return cantidad;
END;$body$;


--Pruebas y llamadas a proceDimensionientos y funciones
call INBIO.insertar_facts();

-- 2
select g.año,g.mes, sum(f.specimen_count) as cantidad, sum(f.cost_sum) as costo from Hecho_specimen f, 
		Dimension_gathering g where f.gathering_id=g.gathering_id group by rollup(año,mes);

-- 3
select g.año,t.kingdoom_name, sum(f.specimen_count) as cantidad, sum(f.cost_sum) as costo 
		from Hecho_specimen f, Dimension_gathering g, Dimension_taxon t where f.gathering_id=g.gathering_id 
		and f.taxon_id=t.taxon_id group by cube(año,kingdoom_name);

DO $$
DECLARE 
total float;
BEGIN
  total :=  fn_sum_specimen('1111576,1463555,1508341,1508350');
	raise notice '%', total;
END $$;
		
		
DO $$
DECLARE 
total int;
BEGIN
  total :=  fn_count_specimen('Plantae');
	raise notice '%', total;
END $$;		



DO $$
DECLARE taxones refcursor;
rec record;
BEGIN
  taxones :=  orden(1);
	loop
	fetch taxones into  rec;
	
	exit when not found;
	

	raise notice '%,%', rec.order_name,rec.cantidad;

	end loop;
	
END $$;
