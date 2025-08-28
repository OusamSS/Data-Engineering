--DDL OF SCD TABLE BASED ON THE CUMULATION TABLE
CREATE OR REPLACE TABLE actors_history_scd (
    actorid VARCHAR,
    actor VARCHAR,
    quality_class VARCHAR,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);

-- BACKFILL FOR THE TABLE ACTORS SCD
--insert into actors_history_scd
with with_previous as (
select
  actorid,
	actor,
	quality_class,
	is_active,
	lag(quality_class, 1) over(partition by actorid order by year) as previous_quality_class,
	lag(is_active, 1) over(partition by actorid order by year) as previous_is_active,
	year
from actors
),
with_indicators as (
	select *,
			case when quality_class <> previous_quality_class then 1
			when is_active <> previous_is_active then 1
			else 0
			end as change_indicator			
	from with_previous
),
with_streaks as (
	select *,
			sum(change_indicator) over(partition by actor order by year) as streak_identifier
	from with_indicators
)
select
  actorid,actor,
	quality_class,is_active,
	min(year) as start_year,
	max(year) as end_year
from with_streaks
group by actorid, actor, quality_class, is_active, streak_identifier
order by actorid, start_year;

--MERGE INTO actors_history_scd AS target
USING (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        year as start_year
    FROM actors
    WHERE year = (SELECT MAX(year) FROM actors)
) AS source
ON target.actorid = source.actorid
AND target.end_year IS NULL

WHEN MATCHED AND (
    target.quality_class <> source.quality_class
    OR target.is_active <> source.is_active
) THEN UPDATE SET
    target.end_year = source.start_year - 1

WHEN NOT MATCHED BY TARGET THEN INSERT (
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year
) VALUES (
    source.actorid,
    source.actor,
    source.quality_class,
    source.is_active,
    source.start_year,
    NULL
);
