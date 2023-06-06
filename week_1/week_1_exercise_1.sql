-- `addresses`
    -- This cte uses an inner join to join the table `customer_address` with `us_cities` to get each
    -- address's geo location (cleaning the address data to match the city data), thus excluding any
    -- addresses that don't appear in `us_cities`. It joins the results to `customer_data` to get
    -- first and last names and email.
-- `suppliers`
    -- This cte joins the `supplier_info` with `us_cities` to get the geo location for each supplier.
-- `customer_supplier_distances`
    -- Cross joins `addresses` with `suppliers` (finding every possible address/supplier combination)
    -- and calculates the distance between each customer address and supplier address, converting the
    -- distance into miles.
-- `ranked_distances`
    -- Partitions `customer_supplier_distances` by customer_id, orders by distance_in_miles (shortest
    -- to longest), and assigns a row number based on that order.
-- Finally, I select from `ranked_distances` only rows with the row value of 1 and order by last and
-- first name.

with addresses as (
    select ca.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        uc.geo_location geo
    from vk_data.customers.customer_address ca
        inner join vk_data.resources.us_cities uc on upper(trim(ca.customer_city)) = uc.city_name
        and upper(trim(ca.customer_state)) = uc.state_abbr
        join vk_data.customers.customer_data cd on ca.customer_id = cd.customer_id
),
suppliers as (
    select si.supplier_name,
        si.supplier_id,
        uc.geo_location geo
    from vk_data.suppliers.supplier_info si
        join vk_data.resources.us_cities uc on upper(si.supplier_city) = uc.city_name
        and upper(si.supplier_state) = uc.state_abbr
),
customer_supplier_distances as (
    select a.*,
        s.*,
        st_distance(a.geo, s.geo) / 1000 / 1.609 distance_in_miles
    from addresses a
        cross join suppliers s
),
ranked_distances as (
    select *,
        row_number() over (
            partition by customer_id
            order by distance_in_miles
        ) rn
    from customer_supplier_distances
)
select customer_id,
    first_name,
    last_name,
    email,
    supplier_id,
    supplier_name,
    distance_in_miles
from ranked_distances
where rn = 1
order by last_name,
    first_name;