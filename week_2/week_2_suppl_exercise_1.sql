with recipe_ingredients as (
    select 
        recipe_id,
        recipe_name,
        flat_ingredients.index,
        trim(upper(replace(flat_ingredients.value, '"', ''))) as ingredient
    from vk_data.chefs.recipe,
    table(flatten(ingredients)) as flat_ingredients
    where recipe_name in (
        'birthday cookie',
        'a perfect sugar cookie',
        'honey oatmeal raisin cookies',
        'frosted lemon cookies',
        'snickerdoodles cinnamon cookies'
    )
),

ingredient_nutrition as (
    select 
        trim(upper(replace(substring(ingredient_name, 1, charindex(',', ingredient_name)), ',', ''))) as ingredient_name,
        min(id) as first_record,
        max(calories) as calories,
        max(total_fat) as total_fat
    from vk_data.resources.nutrition 
    group by 1
),

cookie_recipe_nutrition as (
    select 
        recipe_ingredients.recipe_id,
        recipe_ingredients.recipe_name,
        recipe_ingredients.ingredient,
        ingredient_nutrition.first_record,
        ingredient_nutrition.calories,
        ingredient_nutrition.total_fat
    from recipe_ingredients
    left join ingredient_nutrition
        on recipe_ingredients.ingredient = ingredient_nutrition.ingredient_name
)
select
    cookie_recipe_nutrition.recipe_name,
    sum(cookie_recipe_nutrition.calories) as total_calories,
    sum(cast(replace(cookie_recipe_nutrition.total_fat, 'g', '') as int)) as total_fat
from cookie_recipe_nutrition
join vk_data.resources.nutrition n on cookie_recipe_nutrition.first_record = n.id
group by cookie_recipe_nutrition.recipe_name
order by cookie_recipe_nutrition.recipe_name;
