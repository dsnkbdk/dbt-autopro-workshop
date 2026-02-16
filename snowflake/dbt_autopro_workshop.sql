---> set Role
USE ROLE accountadmin;
---> set Warehouse
USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE raw;
CREATE OR REPLACE DATABASE analytics;
CREATE OR REPLACE SCHEMA raw.autopro_workshop;

--> Create the Table
CREATE OR REPLACE TABLE raw.autopro_workshop.customers
(
customer_id varchar,
first_name varchar,
last_name varchar,
email varchar,
phone varchar,
address varchar,
city varchar,
postcode varchar,
preferred_workshop varchar,
_etl_loaded_at timestamp_ltz,
_etl_file_name string,
_etl_row_number number
);

CREATE OR REPLACE TABLE raw.autopro_workshop.servicetransactions
(
transaction_id varchar,
transaction_date date,
workshop_id varchar,
customer_id varchar,
vehicle_id varchar,
technician_id varchar,
service_type_id varchar,
labour_hours number,
labour_cost number,
parts_cost number,
total_cost number,
payment_method varchar,
status varchar,
modified_date date,
_etl_loaded_at timestamp_ltz,
_etl_file_name string,
_etl_row_number number
);

CREATE OR REPLACE TABLE raw.autopro_workshop.servicetypes
(
service_type_id varchar,
service_type_name varchar,
category varchar,
_etl_loaded_at timestamp_ltz,
_etl_file_name string,
_etl_row_number number
);

CREATE OR REPLACE TABLE raw.autopro_workshop.technicians
(
technician_id varchar,
first_name varchar,
last_name varchar,
workshop_id varchar,
qualification_level varchar,
hourly_rate number,
hire_date date,
_etl_loaded_at timestamp_ltz,
_etl_file_name string,
_etl_row_number number
);

CREATE OR REPLACE TABLE raw.autopro_workshop.vehicles
(
vehicle_id varchar,
customer_id varchar,
make varchar,
model varchar,
year integer,
rego varchar,
odometer integer,
color varchar,
_etl_loaded_at timestamp_ltz,
_etl_file_name string,
_etl_row_number number
);

CREATE OR REPLACE TABLE raw.autopro_workshop.workshops
(
workshop_id varchar,
workshop_name varchar,
city varchar,
region varchar,
manager_name varchar,
_etl_loaded_at timestamp_ltz,
_etl_file_name string,
_etl_row_number number
);


-- Azure
CREATE OR REPLACE STAGE raw.public.loading
URL = 'azure://wennandbt.blob.core.windows.net/autopro-workshop/'
CREDENTIALS = (azure_sas_token = 'sp=rl&st=2026-02-16T04:28:33Z&se=2026-03-16T12:43:33Z&spr=https&sv=2024-11-04&sr=c&sig=JJD1u1%2B2vxpJ6DfH7Iv6JngujM9iyVxjL2BLtIoWitA%3D');


-- Format
CREATE OR REPLACE FILE FORMAT raw.public.csv_format
type = CSV
field_delimiter = ','
skip_header = 1
escape_unenclosed_field = NONE
null_if = ('NULL', 'null', '')
error_on_column_count_mismatch = false
;


COPY INTO raw.autopro_workshop.customers
from (
    select
        t.$1 as customer_id,
        t.$2 as first_name,
        t.$3 as last_name,
        t.$4 as email,
        t.$5 as phone,
        t.$6 as address,
        t.$7 as city,
        t.$8 as postcode,
        t.$9 as preferred_workshop,
        current_timestamp() as _etl_loaded_at,
        metadata$filename as _etl_file_name,
        metadata$file_row_number as _etl_row_number
    from @raw.public.loading/customers/ t
)
file_format = raw.public.csv_format
pattern = 'customers.*\.csv'
;

COPY INTO raw.autopro_workshop.servicetransactions
from (
    select
        t.$1 as transaction_id,
        t.$2 as transaction_date,
        t.$3 as workshop_id,
        t.$4 as customer_id,
        t.$5 as vehicle_id,
        t.$6 as technician_id,
        t.$7 as service_type_id,
        t.$8 as labour_hours,
        t.$9 as labour_cost,
        t.$10 as parts_cost,
        t.$11 as total_cost,
        t.$12 as payment_method,
        t.$13 as status,
        t.$14 as modified_date,
        current_timestamp() as _etl_loaded_at,
        metadata$filename as _etl_file_name,
        metadata$file_row_number as _etl_row_number
    from @raw.public.loading/servicetransactions/ t
)
file_format = raw.public.csv_format
pattern = 'servicetransactions.*\.csv'
;

COPY INTO raw.autopro_workshop.servicetypes
from (
    select
        t.$1 as service_type_id,
        t.$2 as service_type_name,
        t.$3 as category,
        current_timestamp() as _etl_loaded_at,
        metadata$filename as _etl_file_name,
        metadata$file_row_number as _etl_row_number
    from @raw.public.loading/servicetypes/ t
)
file_format = raw.public.csv_format
pattern = 'servicetypes.*\.csv'
;

COPY INTO raw.autopro_workshop.technicians
from (
    select
        t.$1 as technician_id,
        t.$2 as first_name,
        t.$3 as last_name,
        t.$4 as workshop_id,
        t.$5 as qualification_level,
        t.$6 as hourly_rate,
        t.$7 as hire_date,
        current_timestamp() as _etl_loaded_at,
        metadata$filename as _etl_file_name,
        metadata$file_row_number as _etl_row_number
    from @raw.public.loading/technicians/ t
)
file_format = raw.public.csv_format
pattern = 'technicians.*\.csv'
;

COPY INTO raw.autopro_workshop.vehicles
from (
    select
        t.$1 as vehicle_id,
        t.$2 as customer_id,
        t.$3 as make,
        t.$4 as model,
        t.$5 as year,
        t.$6 as rego,
        t.$7 as odometer,
        t.$8 as color,
        current_timestamp() as _etl_loaded_at,
        metadata$filename as _etl_file_name,
        metadata$file_row_number as _etl_row_number
    from @raw.public.loading/vehicles/ t
)
file_format = raw.public.csv_format
pattern = 'vehicles.*\.csv'
;

COPY INTO raw.autopro_workshop.workshops
from (
    select
        t.$1 as workshop_id,
        t.$2 as workshop_name,
        t.$3 as city,
        t.$4 as region,
        t.$5 as manager_name,
        current_timestamp() as _etl_loaded_at,
        metadata$filename as _etl_file_name,
        metadata$file_row_number as _etl_row_number
    from @raw.public.loading/workshops/ t
)
file_format = raw.public.csv_format
pattern = 'workshops.*\.csv'
;


-- Test
select
'customers' as table_name, count(*) as cnt
from raw.autopro_workshop.customers
union all
select
'servicetransactions', count(*)
from raw.autopro_workshop.servicetransactions
union all
select
'technicians', count(*)
from raw.autopro_workshop.technicians
union all
select
'vehicles', count(*)
from raw.autopro_workshop.vehicles
union all
select
'servicetypes', count(*)
from raw.autopro_workshop.servicetypes
union all
select
'workshops', count(*)
from raw.autopro_workshop.workshops
;

