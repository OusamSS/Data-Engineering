
--DDL OF THE ACTORS TABLE
CREATE OR REPLACE TABLE actors (
    actorid VARCHAR,
    actor VARCHAR,
    films STRUCT(film VARCHAR,votes INTEGER,rating DOUBLE,filmid VARCHAR)[],
    quality_class VARCHAR,
    is_active BOOLEAN,
    year INTEGER
);

-- INCREMENTAL CUMULATIVE DESIGN OF ACTORS
--INSERT INTO actors (year, actorid, actor, films, is_active, quality_class)
WITH most_recent_year AS (SELECT MAX(year) as max_year FROM actor_films
),
last_year AS (
    SELECT *
    FROM actors
    WHERE year = max_year - 1
),
this_year AS (
    SELECT
        actorid,
        actor,
        ARRAY_AGG(STRUCT_PACK(film, votes, rating, filmid)) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = max_year
    GROUP BY ALL
),
combined AS (
    SELECT
        COALESCE(t.actorid, l.actorid) AS actorid,
        COALESCE(t.actor, l.actor) AS actor,
        t.avg_rating AS avg_rating_this_year,
        CASE
            WHEN t.films IS NOT NULL AND l.films IS NOT NULL THEN ARRAY_CONCAT(l.films, t.films)
            WHEN t.films IS NOT NULL THEN t.films
            ELSE l.films
        END AS films,
        t.actorid IS NOT NULL AS is_active
    FROM last_year l
    FULL OUTER JOIN this_year t
    ON l.actorid = t.actorid
)
SELECT
    1974 AS year,
    actorid,
    actor,
    films,
    is_active,
    CASE
        WHEN avg_rating_this_year > 8 THEN 'star'
        WHEN avg_rating_this_year > 7 AND avg_rating_this_year <= 8 THEN 'good'
        WHEN avg_rating_this_year > 6 AND avg_rating_this_year <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class
FROM combined;

-- BACKFILL FOR THE TABLE ACTORS
--insert into actors
with all_years as (
	select *
	from generate_series(1970, 2022)
),
first_year as (
  select actorid,actor,min(year) as debut_year from actor_films group by 1,2
),
actors_years as (
  select 
    actorid,actor, debut_year, generate_series as gen_years
from all_years 
join first_year on debut_year <= generate_series
),
windowed as (
select 
  ay.*, year,
  list_filter(array_agg(
    case when year is not null then struct_pack(year,filmid,film,votes,rating) 
    end) over(partition by ay.actor order by gen_years),x->x is not null) as films,
    year is not null as is_active
from actors_years ay left join actor_films af on ay.actorid = af.actorid and gen_years = year
), 
average_ratings as (
select 
  *,
  list_avg(list_transform(list_filter(films, x -> x.year = gen_years), x -> x.rating)) as current_year_avg_rating
from windowed order by actorid
)
select
  actorid,actor,
  films,
  CASE WHEN current_year_avg_rating IS NULL THEN NULL
       WHEN current_year_avg_rating > 8 THEN 'star'
       WHEN current_year_avg_rating > 7 THEN 'good'
       WHEN current_year_avg_rating > 6 THEN 'average'
       ELSE 'bad' 
  END as quality_class,
  is_active,
  gen_years
from average_ratings;
