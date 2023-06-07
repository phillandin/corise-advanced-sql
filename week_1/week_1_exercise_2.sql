-- `customers`
    -- Takes customer_id from `customer_address` and uses an inner join with `us_cities` to find
    -- only customers eligible to order from VK, then joins with `customer_data` to return
    -- additional customer data.
-- `food_tags`
    -- Takes the eligible customer data from `customers`, inner joins it with `customer_survey`
    -- (excluding customers that didn't fill out the survey) to find the tag_id of food
    -- properties each customer indicated liking on the survey, and then joins with `recipe_tags`
    -- to get the tag_property (food property, e.g. "grilling") associated with each tag_id. This
    -- creates a row for each tag_property that a customer indicated, with many customers having
    -- multiple rows. In order to select just the first three food property for each customer,
    -- the data is partitioned by customer_id and each row is assigned a row number for its rank
    -- in its respective partition.
-- `food_preferences`
    -- Pivots `food_tags` to make the first three food properties for each customer into its own
    -- column so that each customer has one row.
-- `recipe_with_tags`
    -- Splits the array of tags associated with each recipe in `recipes` into individual, cleaned
    -- tags using flatten. MIN() is used on recipe_name and the results are grouped by tag
    -- so that each tag ends up with one recipe.
-- Finally, all the customer data from `food_preferences` is joined with `recipe_with_tags` based
-- on each customers first food property preference so that each customer has a corresponding
-- suggested recipe.

with customers as (
    select ca.customer_id,
        cd.first_name,
        cd.email
    from vk_data.customers.customer_address ca
        inner join vk_data.resources.us_cities uc on upper(trim(ca.customer_city)) = uc.city_name
        and upper(trim(ca.customer_state)) = uc.state_abbr
        join vk_data.customers.customer_data cd on ca.customer_id = cd.customer_id
),
food_tags as (
    select c.*,
        rt.tag_property,
        row_number() over (partition by c.customer_id order by tag_property) rn
    from customers c
        inner join vk_data.customers.customer_survey cs
        on c.customer_id = cs.customer_id
        join vk_data.resources.recipe_tags rt
        on rt.tag_id = cs.tag_id
),
food_preferences as (
    select *
    from food_tags
    pivot (max(tag_property) for rn in (1, 2, 3))
        as pivot_values (customer_id, first_name, email, preference_1, preference_2, preference_3)
),
recipe_with_tags as (
    select trim(flat_tag_list.value) tag,
        min(recipe_name) suggested_recipe
    from vk_data.chefs.recipe,
        table(flatten(tag_list)) as flat_tag_list
    group by 1
)
select fp.*,
    rt.suggested_recipe
from food_preferences fp
left join recipe_with_tags rt
    on fp.preference_1 = rt.tag
order by email;