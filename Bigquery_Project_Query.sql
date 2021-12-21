-- Big project for SQL


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  SUM(totals.visits) as visits,
  SUM(totals.pageviews) as pageviews,
  SUM(totals.transactions) as transactions,
  SUM(totals.totalTransactionRevenue)/POWER(10,6) as revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
   date BETWEEN "20170101" AND "20170331"
GROUP BY month
ORDER BY month

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
  trafficSource.source,
  COUNT(trafficSource.source) as total_visits,
  SUM(totals.bounces) as total_no_of_bounces,
  ROUND((SUM(totals.bounces)/COUNT(trafficSource.source))*100,8) as bounce_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY COUNT(trafficSource.source) DESC
LIMIT 4

-- Query 3: Revenue by traffic source by week, by month in June 2017

WITH month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY revenue DESC
)

SELECT * FROM month_data
union all
SELECT * FROM week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH purchaser AS
(
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  ROUND(SUM(totals.pageviews)/COUNT(DISTINCT(fullVisitorId)),9) as avg_pageviews_purchase
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE
      date BETWEEN "20170601" AND "20170731"
  AND totals.transactions >= 1
GROUP BY month
),

non_purchaser AS
(
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  ROUND(SUM(totals.pageviews)/COUNT(DISTINCT(fullVisitorId)),9) as avg_pageviews_non_purchase
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE
      date BETWEEN "20170601" AND "20170731"
  AND totals.transactions IS NULL
GROUP BY month
)

SELECT
  month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
FROM
  purchaser
JOIN non_purchaser USING(month)
ORDER BY month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  ROUND(SUM(totals.transactions)/COUNT(DISTINCT(fullVisitorId)),9) as Avg_total_transactions_per_user
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  totals.transactions >= 1
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  AVG(totals.totalTransactionRevenue) as Avg_total_transactions_per_user
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  fullVisitorId IS NOT NULL
GROUP BY month


-- Query 07: Products purchased by customers who purchased product "YouTube Men's Vintage Henley"
#standardSQL
WITH purchaser AS
(
SELECT
  DISTINCT(fullVisitorId) as id,
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) as hits,
  UNNEST(hits.product) as product
WHERE
      product.productRevenue IS NOT NULL
  AND product.v2ProductName = "YouTube Men's Vintage Henley"
)

SELECT
  product.v2ProductName as other_purchased_products,
  SUM(product.productQuantity) as quantity
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as t,
  UNNEST(hits) as hits,
  UNNEST(hits.product) as product
INNER JOIN purchaser ON t.fullVisitorId = purchaser.id
WHERE
      product.productRevenue IS NOT NULL
  AND product.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity DESC
LIMIT 4


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH n_view AS
(
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  COUNT(product.v2ProductName) as num_product_view
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) as hits,
  UNNEST(hits.product) as product
WHERE
      hits.ecommerceaction.action_type = '2'
  AND date BETWEEN "20170101" AND "20170331"
GROUP BY month
),

n_addtocart AS
(
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  COUNT(product.v2ProductName) as num_addtocart
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) as hits,
  UNNEST(hits.product) as product
WHERE
      hits.ecommerceaction.action_type = '3'
  AND date BETWEEN "20170101" AND "20170331"
GROUP BY month
),

n_purchase AS
(
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) as month,
  COUNT(product.v2ProductName) as num_purchase
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) as hits,
  UNNEST(hits.product) as product
WHERE
      hits.ecommerceaction.action_type = '6'
  AND date BETWEEN "20170101" AND "20170331"
GROUP BY month
)

SELECT * FROM n_view
JOIN n_addtocart USING(month)
JOIN n_purchase USING(month)
ORDER BY month
