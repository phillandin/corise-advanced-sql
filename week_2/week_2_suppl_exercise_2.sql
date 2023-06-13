with unique_cities as (
    select 
        city_name,
        state_abbr,
        min(city_id) as city_id
    from vk_data.resources.us_cities
    group by 
        city_name,
        state_abbr
),

city_details as (
    select 
        city.city_id,
        trim(upper(city.city_name)) as city_name,
        trim(upper(city.state_abbr)) as state_abbr,
        city.lat,
        city.long,
        city.geo_location
    from vk_data.resources.us_cities city
    left join unique_cities on city.city_id = unique_cities.city_id
),

suppliers as (
    select
        supplier_id,
        supplier_name,
        supplier_city || ', ' || supplier_state as supplier_location,
        trim(upper(supplier_city)) as supplier_city,
        trim(upper(supplier_state)) as supplier_state
    from vk_data.suppliers.supplier_info
),

supplier_geography as (
    select
        supplier_id,
        supplier_name,
        suppliers.supplier_location,
        geo_location
    from suppliers
    left join city_details
        on suppliers.supplier_city = city_details.city_name and suppliers.supplier_state = city_details.state_abbr
),

backup_suppliers as (
    select 
        sg1.supplier_id as supplier_main,
        sg2.supplier_id as supplier_backup,
        sg1.supplier_location as location_main,
        sg2.supplier_location as location_backup,
        st_distance(sg1.geo_location, sg2.geo_location) as distance_measure
    from supplier_geography sg1
    cross join supplier_geography sg2
),

shortest_backup_distances as (
    select 
        supplier_main,
            min(distance_measure) as closest_distance
    from backup_suppliers
    where distance_measure > 0
    group by supplier_main
)

select
    suppliers.supplier_id,
    suppliers.supplier_name,
    backup_suppliers.location_main,
    backup_suppliers.location_backup,
    round(backup_suppliers.distance_measure / 1609) as travel_miles
from shortest_backup_distances
left join backup_suppliers
    on shortest_backup_distances.closest_distance = backup_suppliers.distance_measure
        and shortest_backup_distances.supplier_main = backup_suppliers.supplier_main
left join suppliers
    on shortest_backup_distances.supplier_main = suppliers.supplier_id
order by suppliers.supplier_name;
