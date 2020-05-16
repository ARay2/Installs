with brdta as 
(select (timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second') as "Date1",
     case when (branchdata."last_attributed_touch_data__~advertising_partner_name" ilike '' or branchdata."last_attributed_touch_data__~advertising_partner_name" is null ) and (branchdata."last_attributed_touch_data__~campaign" ilike '' or branchdata."last_attributed_touch_data__~campaign" is null) then 'Organic'
        when (branchdata."last_attributed_touch_data__~advertising_partner_name" ilike '' or branchdata."last_attributed_touch_data__~advertising_partner_name" is null ) and ((branchdata."last_attributed_touch_data__~campaign" not ilike '' or branchdata."last_attributed_touch_data__~campaign" is not null) and branchdata."last_attributed_touch_data__~channel" not in ('youtube', 'app','Web_Homepage_Direct','Web_Homepage_SMS','Web_Homepage_Playstore_Button','Web_Homepage') ) then 'SEO'
        when (branchdata."last_attributed_touch_data__~advertising_partner_name" ilike '' or branchdata."last_attributed_touch_data__~advertising_partner_name" is null ) and (branchdata."last_attributed_touch_data__~channel" in ('Web_Homepage_Direct','Web_Homepage_SMS','Web_Homepage_Playstore_Button','Web_Homepage')) then 'Web'
        when (branchdata."last_attributed_touch_data__~advertising_partner_name" ilike '' or branchdata."last_attributed_touch_data__~advertising_partner_name" is null ) and (branchdata."last_attributed_touch_data__~channel" in ('youtube')) then 'Youtube'
        when (branchdata."last_attributed_touch_data__~advertising_partner_name" ilike '' or branchdata."last_attributed_touch_data__~advertising_partner_name" is null ) and (branchdata."last_attributed_touch_data__~channel" in ('app')) then 'Referral'
        else branchdata."last_attributed_touch_data__~advertising_partner_name" end as "Source",
    case when branchdata."last_attributed_touch_data__~advertising_partner_name" ilike 'Mobvista' then branchdata."last_attributed_touch_data__~customer_campaign"
            else branchdata."last_attributed_touch_data__~campaign" end as "Campaign",
    --    branchdata."last_attributed_touch_data__~secondary_publisher" as "Publisher",
    --  branchdata."last_attributed_touch_type" as "Touch_Type",
       case when branchdata."user_data__aaid" = '' or branchdata."user_data__aaid" is null then branchdata."last_attributed_touch_data__$aaid"
            when branchdata."last_attributed_touch_data__$aaid" = '' or branchdata."last_attributed_touch_data__$aaid" is null then branchdata."user_data__aaid" 
            else branchdata."user_data__aaid" end as "GAID"

from branchdatarealtime.data as branchdata 
where branchdata.name='INSTALL' and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) >= {{startdate}} and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) <= {{end_date}}
group by 1,2,3,4)
select  date_trunc('hour',brdta.Date1) as "Date",
        extract(minute from brdta.Date1)::int /5 as minutes_5_Slots,
       brdta.Source as "Source",
       brdta.Campaign as "Campaign",
      -- brdta.Publisher as "Publisher",
      --brdta.Touch_Type as "Touch Type",
     count(distinct(brdta.GAID)) as "Installs",
     count(distinct(case when clickstream_growthapp.eventlabel ilike 'app_screen_open'  and timestampist::date<=brdta.Date1 then  clickstream_growthapp.gaid end)) as "Signups D0",
     count(distinct(case when clickstream_growthapp.eventlabel ilike 'app_screen_open'  and timestampist::date>=brdta.Date1 and timestampist::date-7<=brdta.Date1 then clickstream_growthapp.gaid end)) as "Signups D7",
     count(distinct(case when clickstream_growthapp.eventlabel ilike 'app_screen_open'  and timestampist::date>=brdta.Date1 and timestampist::date<=current_date then clickstream_growthapp.gaid end)) as "Signups MTD"
       
from brdta 
     left join clickstream_growthapp on clickstream_growthapp.gaid = brdta.GAID
     
where clickstream_growthapp.timestampist::date >= {{startdate}}
     
group by 1,2,3,4
order by  1,2,3,4
