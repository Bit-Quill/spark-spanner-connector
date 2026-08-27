select
    c_count,
    count(*) as custdist
from
    (
        select
            c.c_custkey,
            count(o_orderkey) as c_count  -- Alias the column here
        from
            customer as c left outer join orders as o on
                c.c_custkey = o.c_custkey
                    and o_comment not like '%pending%requests%'
        group by
            c.c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc
