-- table have duplicate
select game_id,team_id,player_id, count(1)
from game_details
group by 1,2,3
having count(1) > 1;

-- we then dedup and remodel the table to suit best practices of fact data
with deduplicate as (
  select 
    gd.*, 
    game_date_est, 
    season,
    home_team_id
  from game_details gd join games g using(game_id)
  qualify row_number() over(partition by game_id,team_id,player_id order by game_date_est) < 2
)

select
  game_date_est,
  season,
  game_id,
  team_id,
  team_id = home_team_id as is_playing_home,
  player_id, player_name,
  coalesce(position('DNP' in comment),0) > 0 as dnp,
  coalesce(position('DND' in comment),0) > 0 as dnd,
  coalesce(position('NWT' in comment),0) > 0 as nwt,
  try_cast(split_part(min,':',1) as float4) + try_cast(split_part(min,':',2) as float4)/60 as minutes
from deduplicate;
