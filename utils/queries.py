temp_tables = (
    """ 
    WITH value_props_views AS (
        SELECT 
            p.value_prop,
            to_date(p."day", 'YYYY-MM-DD') print_date,
            count(p.value_prop) as seen_prints,
            p.user_id
        FROM prints p
        group by p.value_prop, print_date, p.user_id 
    ), clicked_value_props AS (
        SELECT 
            p.value_prop,
            to_date(p."day", 'YYYY-MM-DD') as print_date,
            sum(
                case
                when t.user_id is null then 0
                else 1
                end
            ) as clicked_prints,
            p.user_id
        FROM prints p
        left join taps t on t.user_id = p.user_id and t.value_prop = p.value_prop
        group by p.value_prop, print_date, p.user_id
    ), prints_last_week AS (
        select
            p.user_id,
            to_date(p."day", 'YYYY-MM-DD') as print_date,
            case
            when t.user_id is null then 0
            else 1
            end as clicked
        FROM prints p
        left join taps t on t.user_id = p.user_id and t.value_prop = p.value_prop
        where to_date(p.day, 'YYYY-MM-DD') >= to_date(p."day",'YYYY-MM-DD') - INTERVAL '7 days'
    )
    """
)

final_dataset_query = (
    """
    select
        plw.user_id,
        plw.print_date,
        plw.clicked,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'cellphone_recharge') as cellphone_recharge_views,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'credits_consumer') as credits_consumer_views,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'link_cobro') as link_cobro_views,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'point') as point_views,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'prepaid') as prepaid_views,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'send_money') as send_money_views,
        count(vpv.seen_prints) filter (where vpv.value_prop = 'transport') as transport_views,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'cellphone_recharge') as cellphone_recharge_clicked_prints,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'credits_consumer') as credits_consumer_clicked_prints,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'link_cobro') as link_cobro_clicked_prints,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'point') as point_clicked_prints,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'prepaid') as prepaid_clicked_prints,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'send_money') as send_money_clicked_prints,
        count(cvp.clicked_prints) filter (where cvp.value_prop = 'transport') as transport_clicked_prints
    from prints_last_week plw
    left join value_props_views vpv on plw.user_id = vpv.user_id
    left join clicked_value_props cvp on plw.user_id = cvp.user_id
    where cvp.print_date >= plw.print_date - INTERVAL '21 days'
    and vpv.print_date >= plw.print_date - INTERVAL '21 days'
    group by
        plw.user_id,
        plw.print_date,
        plw.clicked
    """
)
