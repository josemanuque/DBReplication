-- Create a new database called 'MainDB;' and then run this script.
USE MainDB;

create schema INBIO;

create table INBIO.taxon(
    taxon_id int primary key,
    kingdom_name varchar(50),
    phylum_division_name varchar(50),
    class_name varchar(50),
    order_name varchar(50),
    family_name varchar(50),
    genus_name varchar(50),
    species_name varchar(50),
    scientific_name varchar(50)

);

create table INBIO.gathering_responsible(
    gathering_Responsible_id serial primary key,
    name varchar(50) not NULL
);

create table INBIO.site(
    site_id int primary key,
    latitude float not NULL,
    longitude float not NULL,
    site_description varchar(500) not NULL
);

create table INBIO.gathering(
    gathering_id int primary key,
    gathering_date date not null,
    gathering_Responsible_id int references INBIO.Gathering_Responsible(gathering_Responsible_id),
    site_id int references INBIO.site(site_id)
);

create table INBIO.specimen(
    Specimen_ID int primary key,
    taxon_id int,
    gathering_id int REFERENCES INBIO.gathering(gathering_id),
    specimen_description varchar(10000) not null,
    specimen_cost float not null 
);

create table INBIO.temp(
    specimen_id int,
    taxon_id int,
    gathering_date varchar(10),
    kingdom_name varchar(50),
    phylum_division_name varchar(50),
    class_name varchar(50),
    order_name varchar(50),
    family_name varchar(50),
    genus_name varchar(50),
    species_name varchar(50),
    scientific_name varchar(200),
    gathering_responsible varchar(50),
    site_id int,
    latitude float,
    longitude float,
    site_description varchar(10000),
    specimen_description varchar(10000),
    specimen_cost float
);


-- Procedures to insert into tables

create or replace procedure INBIO.insertar_taxon(taxon_id int, kingdom_name varchar, phylum_division_name varchar, class_name varchar, order_name varchar, family_name varchar, genus_name varchar, species_name varchar, scientific_name varchar)
language plpgsql
as $$
begin
    insert into INBIO.taxon values(taxon_id,kingdom_name,phylum_division_name,class_name,order_name,family_name,genus_name,species_name,scientific_name)
    on conflict do nothing;
end;
$$;


create or replace procedure INBIO.insertar_specimen(specimen_id int, taxon_id int, gathering_id int, specimen_description varchar, specimen_cost float)
language plpgsql
as $$
begin
    insert into INBIO.specimen values(specimen_id,taxon_id,gathering_id,specimen_description,specimen_cost)
    on conflict do nothing;
end;
$$;

create or replace procedure INBIO.insertar_gathering(gathering_id int, gathering_date varchar, gathering_responsible_id int, site_id int)
language plpgsql
as $$
begin
    insert into INBIO.gathering values(gathering_id,to_date(gathering_date, 'DD-MM-YYYY'),gathering_responsible_id,site_id)
    on conflict do nothing;
end;
$$;


--- Procedures to normalize the data from the the temp table into the tables


create or replace procedure INBIO.normalize()
language plpgsql
as $$
declare
    cur cursor for select * from INBIO.temp;
    row INBIO.temp%rowtype;
    gathering_temp_id int;
begin
    for row in cur loop
        call INBIO.insertar_taxon(row.taxon_id,row.kingdom_name,row.phylum_division_name,row.class_name,row.order_name,row.family_name,row.genus_name,row.species_name,row.scientific_name);
        
        insert into INBIO.gathering_responsible(name) values(row.gathering_responsible)
        on conflict do nothing;

        select gathering_responsible_id into gathering_temp_id from INBIO.gathering_responsible where name = row.gathering_responsible;
        
        insert into INBIO.site values(row.site_id,row.latitude,row.longitude,row.site_description)
        on conflict do nothing;
        
        call INBIO.insertar_gathering(gathering_temp_id,row.gathering_date,gathering_temp_id,row.site_id);

        call INBIO.insertar_specimen(row.specimen_id,row.taxon_id,gathering_temp_id,row.specimen_description,row.specimen_cost);
    end loop;
end;
$$;

-- Insert from csv file into temp table

copy INBIO.temp from 'C:\temp.csv' with (format csv, header true, delimiter '|');

-- Call the procedure to normalize the data

call inbio.normalize();

-- Tests
call INBIO.insertar_gathering(1,'2019-01-01',1,1);

insert into INBIO.gathering_responsible (name) values('Hola')
        on conflict do nothing;

select gathering_Responsible_id from INBIO.gathering_responsible where name = 'Hola';

truncate table INBIO.temp;

-- Selects 
select * from INBIO.gathering_responsible;
select * from INBIO.taxon;
select * from INBIO.site;
select * from INBIO.gathering;
select * from INBIO.specimen;
select * from INBIO.temp;