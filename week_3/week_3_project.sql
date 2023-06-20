with web_activity_by_session as (
    select
        event_id,
        session_id,
        user_id,
        event_details,
        event_timestamp,
        row_number() over (partition by session_id order by session_id, event_timestamp) as event_order
    from vk_data.events.website_activity
),

unique_sessions as (
    select
        date(event_timestamp) as session_date,
        count(*) as total_unique_sessions
    from web_activity_by_session
    where event_order = 1
    group by session_date
),

session_starts as (
/* takes the timestamp from the first event in each session to determine the session's start */
    select
        session_id,
        web_activity_by_session.event_timestamp as session_start_timestamp
    from web_activity_by_session
    where event_order = 1
),

session_start_end_timestamps as (
/* joins with above cte so that each event's record includes the start timestamp of the session
during which the event occurred */
    select
        web_activity_by_session.event_id,
        web_activity_by_session.session_id,
        web_activity_by_session.user_id,
        web_activity_by_session.event_details,
        web_activity_by_session.event_timestamp,
        session_starts.session_start_timestamp,
        web_activity_by_session.event_order
    from session_starts
    join web_activity_by_session
    on session_starts.session_id = web_activity_by_session.session_id
),

session_lengths as (
/* finds the difference between each event's timestamp and the start of that session, then
selects the maximum difference, which will be the amount of time between session start and
the session's last event, i.e. the length of the session */
    select
        date(session_start_timestamp) as session_date,
        session_id,
        max(
            timestampdiff(
                'seconds',
                session_start_timestamp,
                event_timestamp
            )
        ) as session_length_in_sec
    from session_start_end_timestamps
    group by
        session_start_timestamp,
        session_id
),

session_length_by_date as (
    select
        session_date,
        avg(session_length_in_sec) as avg_session_length_seconds
    from session_lengths
    /* The following commented line could be used to exclude sessions consisting of one event */
    -- where session_length_in_sec != 0
    group by session_date
),

search_view_recipe_events as (
/* There are three event types in the event_details field: 'pageview', 'search', 'view_recipe'.
Since we want to find the average number of searchs made before viewing a recipe, we need only
search and view_recipe events. */
    select
        date(event_timestamp) as session_date,
        session_id,
        event_order,
        parse_json(event_details) as parsed_details
    from
        web_activity_by_session
    where
        parsed_details:event in ('search', 'view_recipe')
),

view_recipe_events as (
/* selects the first view_recipe event from each session in order to see how many search events
preceded it in the next cte */
    select
        session_date,
        session_id,
        event_order,
        parsed_details
    from search_view_recipe_events
    where parsed_details:event = 'view_recipe'
    qualify row_number() over (partition by session_id order by session_id) = 1
),

search_before_view_count as (
/* joining the cte with search and view_recipe events with the cte containing only first view_recipe
events according to session id, this finds the difference between the timestamp of the first search
event and the first view_recipe event */
    select
        view_recipe_events.session_date,
        view_recipe_events.session_id,
        view_recipe_events.event_order - min(search_view_recipe_events.event_order) as search_count
    from view_recipe_events
    inner join search_view_recipe_events
    on view_recipe_events.session_id = search_view_recipe_events.session_id
    group by 
        view_recipe_events.session_date,
        view_recipe_events.session_id,
        view_recipe_events.event_order
),

avg_search_before_view_count as (
    select
        session_date,
        avg(search_count) as avg_search_count
    from search_before_view_count
    group by session_date
),

recipe_view_counts as (
/* counts the number of view_recipe events per recipe on each day */
    select
        session_date,
        parsed_details:recipe_id as recipe_id,
        count(*) as recipe_views
    from
        view_recipe_events
    group by
        session_date,
        recipe_id
),

max_recipe_views as (
    select
        session_date,
        max(recipe_views) as max_views
    from recipe_view_counts
    group by session_date
),

recipe_views_w_max as (
/* finds the most viewed recipe for each day by selecting the recipe with the view count
that matches that day's max recipe views */
    select
        recipe_view_counts.session_date,
        recipe_view_counts.recipe_id,
        recipe_view_counts.recipe_views,
        max_recipe_views.max_views
    from recipe_view_counts
    inner join max_recipe_views
    on recipe_view_counts.session_date = max_recipe_views.session_date
    where recipe_view_counts.recipe_views = max_recipe_views.max_views
),

most_viewed_recipe as (
/* There are days with more than one most viewed recipes due to view count ties. This cte
selects only one recipe per day using min() */
    select
        session_date,
        min(recipe_id) as recipe_id
    from recipe_views_w_max
    group by session_date
)

select
    unique_sessions.session_date as activity_date,
    unique_sessions.total_unique_sessions,
    session_length_by_date.avg_session_length_seconds,
    avg_search_before_view_count.avg_search_count,
    most_viewed_recipe.recipe_id as most_viewed_recipe_id
from unique_sessions
inner join session_length_by_date
on unique_sessions.session_date = session_length_by_date.session_date
inner join avg_search_before_view_count
on unique_sessions.session_date = avg_search_before_view_count.session_date
inner join most_viewed_recipe
on unique_sessions.session_date = most_viewed_recipe.session_date
order by activity_date;