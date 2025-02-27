-- TASK 1

with rank_country as (
  select *, row_number() over(order by totalTransactionRevenue desc) rank from (
  select country, sum(totalTransactionRevenue) totalTransactionRevenue from `hardy-device-431214-f8.2002Han.public` group by 1)
), top_5_country as (
  select * from rank_country where rank <= 5
), final_pool_data as (
SELECT a.country, channelGrouping, a.totalTransactionRevenue FROM `hardy-device-431214-f8.2002Han.public` a left join top_5_country b on a.country = b.country where b.country is not null) 
select country, channelGrouping, sum(totalTransactionRevenue) from final_pool_data group by 1,2 order by 1,2

-- TASK 2
with avg_all_population as (
  select avg(case when timeOnSite > 0 then timeOnSite end) avgtimeOnSiteAll
  , avg(case when pageviews > 0 then pageviews end) avgpageviewsAll
  , avg(case when sessionQualityDim > 0 then sessionQualityDim end) avgsessionQualityDimAll
  from `hardy-device-431214-f8.2002Han.public`
), avg_population_spec as (
  select fullVisitorId, avg(case when timeOnSite > 0 then timeOnSite end) avgtimeOnSite
  , avg(case when pageviews > 0 then pageviews end) avgpageviews
  , avg(case when sessionQualityDim > 0 then sessionQualityDim end) avgsessionQualityDim
  from `hardy-device-431214-f8.2002Han.public` group by 1
) select fullVisitorId from ( select a.*, b.* from avg_population_spec a, avg_all_population b)
where avgtimeOnSite > avgtimeOnSiteAll and avgpageviews < avgpageviewsAll

-- TASK 3
with raw_data as (SELECT v2ProductName, sum(cast(productRevenue as int)) productRevenue, sum(cast(productQuantity as int)) productQuantity, sum(cast(productRefundAmount as int)) productRefundAmount FROM `hardy-device-431214-f8.2002Han.public` group by 1)
select *, row_number() over(order by netRevenue desc) rank, case when productRefundAmount > (0.1 * netRevenue) then 1 else 0 end as flag   from (
select v2ProductName, productRevenue,  productQuantity, productRefundAmount, (productRevenue * productQuantity) - productRefundAmount as netRevenue from raw_data)

-- there is a problem in this dataset cause mostly the column that used on task 3 is null value (missing), so we need do deep analytic to find out how to fix the missing value issue