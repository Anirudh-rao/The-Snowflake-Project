use database ETL;
use schema raw;

create or replace table raw_quotes
(
    data variant
);

Select * from raw_quotes;

--insert into a raw table data
insert into raw_quotes
select parse_json(get_api_data());

Select * from raw_quotes;


insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));
insert into raw_quotes
 select parse_json(replace(replace(get_api_data(),'['),']'));

 


 --now to parse each value into a different column, you need the outer element (the brackets) removed
select data:author::text as author
     , data:quote::text as quote
     , data:category::text as category
from raw_quotes;


select data:author::text as author
     , snowflake.cortex.sentiment(data:quote::text) as quote_sentiment
     , snowflake.cortex.translate(data:quote::text,'en', 'sv') as quote_swedish
     , data:quote::text as original_quote
     , data:category::text as category
from raw_quotes;


