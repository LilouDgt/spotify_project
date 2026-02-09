with tracks as (
    select 
      music_id,
      music_name,
      release_year,
      album_name,
      case when popularity >= 80 then TRUE else FALSE end as is_hit
    from {{ ref('stg_bad_bunny_track') }}
)

select * from tracks