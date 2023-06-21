/* performance optimizations:
    - All of the ctes that draw from the website_activity table make use of a row number that numbers the 
    events in the order they occur in a given session. This is achieved by partitioning over the session_id
    Instead of performing the partition every time a cte draws from the source table, I made it once in the
    first cte, which forms the starting point for all other ctes.
    - At first, I added the field session_date (using the date() function on the event_timestamp field) in
    the first cte. For performance's sake, I ended up delaying that so that there's one less field to store
    in memory.
    - I tried to drop all unnecessary fields. */

with session_event_orders as (
    /* This is the base cte on which all the others build. It selects the fields which will be relevant for
     all subsequent ctes and numbers the records according to their order of sequence in a given session */
    select date(event_timestamp) as session_date,
        session_id,
        event_timestamp,
        event_details,
        row_number() over (
            partition by session_id
            order by session_id
        ) as event_order
    from vk_data.events.website_activity
),

session_start_end as (
    select session_id,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end
    from session_event_orders
    group by session_id
),

avg_session_lengths as (
    /* finds the average length of each session by averaging the difference between the session start and
     end timestamps */
    select date(session_start) as session_date,
        avg(
            timestampdiff(
                'seconds',
                session_start,
                session_end
            )
        ) as avg_session_length_seconds
    from session_start_end
    group by session_date
),

unique_sessions as (
    select session_date,
        count(*) as total_unique_sessions
    from session_event_orders
    where event_order = 1
    group by session_date
),

search_events as (
    /* selects all records with event type of search */
    select date(event_timestamp) as session_date,
        session_id,
        event_order,
        parse_json(event_details) as parsed_details
    from session_event_orders
    where parsed_details :event = 'search'
),

view_recipe_events as (
    /* selects all records with event type of view_recipe */
    select date(event_timestamp) as session_date,
        session_id,
        event_order,
        parse_json(event_details) as parsed_details
    from session_event_orders
    where parsed_details :event = 'view_recipe'
),

search_before_view_count as (
    /* joining the cte search_view_recipe_events with the cte recipe_view_events according to
     session_date and session_id, this finds the difference between the event_order of the first
     search event and the event_order of the first view_recipe event */
    select view_recipe_events.session_date,
        view_recipe_events.session_id,
        view_recipe_events.event_order - min(search_events.event_order) as search_count
    from view_recipe_events
        inner join search_events on view_recipe_events.session_id = search_events.session_id
    group by view_recipe_events.session_date,
        view_recipe_events.session_id,
        view_recipe_events.event_order qualify row_number() over (
            partition by view_recipe_events.session_id
            order by view_recipe_events.session_id
        ) = 1
),

avg_search_before_view_count as (
    select session_date,
        avg(search_count) as avg_search_count
    from search_before_view_count
    group by session_date
),

recipe_view_counts as (
    /* counts the number of view_recipe events per recipe on each day */
    select session_date,
        parsed_details :recipe_id as recipe_id,
        count(*) as recipe_views
    from view_recipe_events
    group by session_date,
        recipe_id
),

max_recipe_views as (
    select session_date,
        max(recipe_views) as max_views
    from recipe_view_counts
    group by session_date
),

most_viewed_recipe as (
    /* finds the most viewed recipe for each day by selecting the recipe with the view count
     that matches that day's max recipe views */
    select recipe_view_counts.session_date,
        min(recipe_view_counts.recipe_id) as recipe_id
    from recipe_view_counts
        inner join max_recipe_views on recipe_view_counts.session_date = max_recipe_views.session_date
    where recipe_view_counts.recipe_views = max_recipe_views.max_views
    group by recipe_view_counts.session_date
)

select unique_sessions.session_date as activity_date,
    unique_sessions.total_unique_sessions,
    avg_session_lengths.avg_session_length_seconds,
    avg_search_before_view_count.avg_search_count,
    most_viewed_recipe.recipe_id as most_viewed_recipe_id
from unique_sessions
    left join avg_session_lengths on unique_sessions.session_date = avg_session_lengths.session_date
    left join avg_search_before_view_count on unique_sessions.session_date = avg_search_before_view_count.session_date
    left join most_viewed_recipe on unique_sessions.session_date = most_viewed_recipe.session_date;