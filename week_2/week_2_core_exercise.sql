with affected_customer_addresses as (
/* This cte cleans the customer city and state fields so they're ready to compare with the us_cities table
and applies the filters that were previously in the where clause. This improve readability (and performance,
I think) of the entire query.

The where clause is also formatted to be consistent across each clause and to read easier. Since the state
and city fields are already cleaned, the where clause filters on exact string values instead of using ilike
in order to be more precise. */
    select
        customer_id,
        upper(trim(customer_state)) as cleaned_state,
        upper(trim(customer_city)) as cleaned_city
    from vk_data.customers.customer_address
    where
        cleaned_state = 'KY'
            and (
                cleaned_city in (
                    'CONCORD',
                    'GEORGETOWN',
                    'ASHLAND'
                )
            )
        or cleaned_state = 'CA'
            and (
                cleaned_city in (
                    'OAKLAND',
                    'PLEASANT_HILL'
                )
            )
        or cleaned_state = 'TX'
            and (
                cleaned_city in (
                    'ARLINGTON',
                    'BROWNSVILLE'
                )
            )
),

clean_cities as (
/* This cte cleans the city data so that it's ready to be compared with the customer address data */
    select
        upper(trim(state_abbr)) as state_abbr,
        upper(trim(city_name)) as city_name,
        geo_location
    from vk_data.resources.us_cities
),

customer_food_pref_count as (
    /* I created this cte to replace the first subquery for improved readability */
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
),

chicago_geo as (
    /* This cte replaces the Chicago geography subquery for improved readability */
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),

gary_geo as (
    /* This cte replaces the Gary geography subquery for improved readability */
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)

select 
    first_name || ' ' || last_name as customer_name,
    affected_customer_addresses.cleaned_city,
    affected_customer_addresses.cleaned_state,
    customer_food_pref_count.food_pref_count,
    (st_distance(clean_cities.geo_location, chicago_geo.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(clean_cities.geo_location, gary_geo.geo_location) / 1609)::int as gary_distance_miles
from affected_customer_addresses

/* I changed the alias for customer_data from 'c' to 'customer_data' to make it more understandable and used
a left join to ensure that (in the case that their data was not in the customer_data table for some reason)
they would not be dropped */
left join vk_data.customers.customer_data as customer_data
    on affected_customer_addresses.customer_id = customer_data.customer_id
left join clean_cities
    on affected_customer_addresses.cleaned_state = clean_cities.state_abbr
        and affected_customer_addresses.cleaned_city = clean_cities.city_name
left join customer_food_pref_count
    on affected_customer_addresses.customer_id = customer_food_pref_count.customer_id
cross join chicago_geo
cross join gary_geo;
