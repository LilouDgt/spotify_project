with source as (
    select 
      id as music_id,
      name as music_name,
      popularity,
      extract(year from release_date) as release_year,
      album_name

    from {{ source('spotify_data', 'raw_bad_bunny_track') }}
)

select *
from source