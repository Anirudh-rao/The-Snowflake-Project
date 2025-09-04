-- DOCUMENT AI
use role accountadmin;
use database ETL;
use schema ETL.pre_mart;

-- Create role
create or replace role doc_ai_role;

grant database role SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR to role doc_ai_role;

select current_user;

grant role doc_ai_role to user ANIRUDH;

-- Create warehouse
create or replace warehouse doc_ai_wh with warehouse_size = 'x-small';

grant USAGE, OPERATE, MODIFY on WAREHOUSE doc_ai_wh to role doc_ai_role;

-- Grant Access to Database to Doc_AI_Role
grant CREATE SCHEMA, MODIFY, USAGE on DATABASE ETL to role doc_ai_role;
grant MODIFY, USAGE on Schema ETL.RAW to role doc_ai_role;
grant USAGE on Schema ETL.PRE_MART to role doc_ai_role;
grant CREATE STAGE ON SCHEMA ETL.PRE_MART to role doc_ai_role;
grant CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE on SCHEMA ETL.PRE_MART  to role doc_ai_role;
grant create model on schema etl.pre_mart to role doc_ai_role;

CREATE STAGE lyrics_pdfs 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' ) 
	COMMENT = 'Stage for data Extractions';

grant read, write on stage etl.pre_mart.lyrics_pdfs to role doc_ai_role;

SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'The Girl From Ipanema by Amy Winehouse.pdf'), 1
);


-- DOCUMENT AI
use role accountadmin;
use database ETL;
use schema ETL.pre_mart;

-- Create role
create or replace role doc_ai_role;

grant database role SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR to role doc_ai_role;

select current_user;

grant role doc_ai_role to user ANIRUDH;

-- Create warehouse
create or replace warehouse doc_ai_wh with warehouse_size = 'x-small';

grant USAGE, OPERATE, MODIFY on WAREHOUSE doc_ai_wh to role doc_ai_role;

-- Grant Access to Database to Doc_AI_Role
grant CREATE SCHEMA, MODIFY, USAGE on DATABASE ETL to role doc_ai_role;
grant CREATE TABLE,MODIFY, USAGE on Schema ETL.RAW to role doc_ai_role;
grant USAGE on Schema ETL.PRE_MART to role doc_ai_role;
grant CREATE STAGE ON SCHEMA ETL.PRE_MART to role doc_ai_role;
grant CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE on SCHEMA ETL.PRE_MART  to role doc_ai_role;
grant create model on schema etl.pre_mart to role doc_ai_role;
grant read, write on stage etl.pre_mart.lyrics_pdfs to role doc_ai_role;

-- Model Run
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'The Girl From Ipanema by Amy Winehouse.pdf'), 1
);


-- Create new schema to store files
create table etl.raw.raw_songs(
    Json variant
);


--  Storing Data from our document AI function
insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'The Girl From Ipanema by Amy Winehouse.pdf'), 1
);

insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'Ribbon in the Sky by Stevie Wonder.pdf'), 1
);

insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', '99 Red Balloons by Nena.pdf'), 1
);


insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'The Girl From Ipanema by Amy Winehouse.pdf'), 1
);


-- Parsing our data
select json:__documentMetadata:ocrScore as overall_confidence_score
    , json:song_title[0].score as song_title_score
    , json:song_title[0].value::text as song_title
    , json:performing_artist[0].score as perf_art_score
    , json:performing_artist[0].value::text as performing_artist
    , json:lyrics[0].score as lyrics_score
    , json:lyrics[0].value::text as song_lyrics
    , json:authors[0].score as authors_score
    , json:authors[0].value::text as authors
    , json:lyrics_copyright_holders[0].score as copyright_score
    , json:lyrics_copyright_holders[0].value::text as copyright_holders
    , json:lyric_licensor_provider[0].score as provider_score
    , json:lyric_licensor_provider[0].value::text as licensor_provider
    , json:source[0].score as source_score
    , json:source[0].value::text as source
  from etl.raw.raw_songs;
