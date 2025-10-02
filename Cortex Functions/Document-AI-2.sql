Use database ETL;
Use role DOC_AI_ROLE;
use warehouse doc_ai_wh;

use role accountadmin;
grant usage, modify ,create table , create view on schema etl.cleaned to role doc_ai_role;

-- Creating view
use role doc_ai_role;

Create view etl.cleaned.parsed_song_data as
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


  -- Creating sentiment analysis and summarize functions view
create view etl.cleaned.song_summarize as
select song_title
    , snowflake.cortex.summarize(song_lyrics) as song_summary
    , snowflake.cortex.sentiment(song_lyrics) as song_sentiment
from etl.cleaned.parsed_song_data;

-- Inserting more data
ls @LYRICS_PDFS/training_files;


insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'training_files/Beyond the Sea by Bobby Darin.pdf'), 1
);

insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT
(
  GET_PRESIGNED_URL('@"ETL"."PRE_MART"."LYRICS_PDFS"', 'training_files/Cutie Pie by One Way.pdf'), 1
);


-- Automating the process
-- run the list command on our newest set of files 
ls @lyrics_pdfs/automated;

--view the relative_path of all files in the stage
select relative_path
from directory(@lyrics_pdfs);

--view the relative path of ONLY the files in a particular directony
select relative_path
from directory(@lyrics_pdfs)
where relative_path like '%automated%';


insert into etl.raw.raw_songs
SELECT ETL.PRE_MART.LYRICS_EXTRACTOR!PREDICT(
  GET_PRESIGNED_URL(@LYRICS_PDFS, relative_path), 1)
  from directory(@lyrics_pdfs)
  where relative_path like '%automated%';


Select * from etl.cleaned.song_summarize;