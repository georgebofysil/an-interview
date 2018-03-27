-- Postgres
-- Creating features:
--     Response     : xseller
--     Seasonality  : orderyear, ordermonth, orderday
--     Kits Ordered : kits_ordered
--     Activation   : days_to_activation, kits_activated, kit_activation_rate
--     Completion   : kits_received, kit_completion_rate

WITH prospect_min_order AS (

  SELECT prospectid,
         MIN(ordernumber) AS min_ordernumber
    FROM gbofysil.cross_sell_data2
  GROUP BY 1

), prospect_min_date AS (

  SELECT prospectid,
         MIN(ordercreatedate) AS min_ordercreatedate
    FROM gbofysil.cross_sell_data2
   GROUP BY 1

), prospect_first_order AS (

SELECT a.*
  FROM prospect_min_order min
  JOIN gbofysil.cross_sell_data2 a
       ON a.ordernumber = min.min_ordernumber

), kits_per_prospect AS (

SELECT f.prospectid,
       f.ordernumber,
       f.ordercreatedate,
       f.xsell_gsa,
       f.xsell_day_exact,
       COUNT(a.ordernumber) AS kits_ordered,
       COUNT(a.dnatestactivationdayid) AS kits_activated,
       COUNT(a.daystogetresult_grp) AS kits_received

FROM gbofysil.cross_sell_data2 a
JOIN prospect_first_order f
     ON  a.prospectid  = f.prospectid
     AND a.ordernumber >= f.ordernumber
     AND a.ordercreatedate <= f.ordercreatedate + INTERVAL '120 days'
     AND CASE WHEN f.xsell_gsa = 1 THEN
              a.ordercreatedate <= f.ordercreatedate + f.xsell_day_exact * INTERVAL '1 day'
              ELSE 1 = 1
              END

GROUP BY 1,2,3,4,5
ORDER BY 6 DESC
)

SELECT f.prospectid,
       f.ordernumber,
       f.ordercreatedate,
       EXTRACT('year' FROM f.ordercreatedate)  AS orderyear,
       EXTRACT('month' FROM f.ordercreatedate) AS ordermonth,
       EXTRACT('dow'   FROM f.ordercreatedate) AS orderday,
       f.regtenure,
       f.customer_type_group,
       f.dnatestactivationdayid,
       DATE_PART('day', f.dnatestactivationdayid - f.ordercreatedate) AS days_to_activation,
       f.daystogetresult_grp,
       f.weekstogetresult_grp,
       f.dna_visittrafficsubtype,
       f.traffic_source,
       k.kits_ordered,
       k.kits_activated,
       k.kits_received,
       k.kits_activated::float / k.kits_ordered::float  AS kit_activation_rate,
       k.kits_received  / k.kits_ordered  AS kit_completion_rate,
       CASE WHEN k.kits_activated > 0
            THEN 1 - k.kits_received::float / k.kits_activated::float ELSE NULL END AS kit_error_rate,
       f.xsell_gsa,
       f.xsell_day_exact,
       CASE WHEN f.xsell_gsa = 1 AND f.xsell_day_exact <= 120
            THEN 1 ELSE 0
            END AS xseller

  FROM prospect_first_order f
  JOIN kits_per_prospect k
       ON  f.ordernumber = k.ordernumber
