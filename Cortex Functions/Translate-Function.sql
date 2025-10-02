-- We can call translate function as below
--1. English to japanese
select snowflake.cortex.translate('hello','en','ja');

--2. Japanese to English
select snowflake.cortex.translate('こんにちは','ja','en');

-- 3. Reverse translation
set my_text = 'My favorite color is blue';
select $my_text;

select $my_text as OG_ENGLISH
    , snowflake.cortex.translate($my_text, 'en', 'fr') as EN_2_FR
    , snowflake.cortex.translate(EN_2_FR, 'fr', 'en') as EN_2_FR_2_EN
    , OG_ENGLISH = EN_2_FR_2_EN as Reverse_Translation_Success;


-- Some Examples
set my_text = 'Love is composed of a single soul inhabiting two bodies';
select $my_text;

select $my_text as OG_ENGLISH
    , snowflake.cortex.translate($my_text, 'en', 'fr') as EN_2_FR
    , snowflake.cortex.translate(EN_2_FR, 'fr', 'en') as EN_2_FR_2_EN
    , OG_ENGLISH = EN_2_FR_2_EN as Reverse_Translation_Success;

