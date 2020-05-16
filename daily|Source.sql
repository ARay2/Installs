select (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second'))::date as "Date2",
        count(distinct(branchdata.user_data__aaid)) as "Total_Installs",
        case when ("last_attributed_touch_data__~channel" is null or "last_attributed_touch_data__~channel" = '') then 'Organic'
             when "last_attributed_touch_data__~channel" ilike '%mweb%' then 'SEO'
             when "last_attributed_touch_data__~channel" = 'youtube' then 'youtube'
             when "last_attributed_touch_data__~channel" = 'telegram_migration' then 'Telegram'
             when "last_attributed_touch_data__~channel" = 'app' then 'Referral'
             when "last_attributed_touch_data__~campaign" ilike '%[Essence]_%' then 'UAC_Agency'
             when "last_attributed_touch_data__~channel" in ('facebook','Facebook') then 'Facebook'
             when "last_attributed_touch_data__~campaign" ilike '%UAC%' then 'UAC'
             when "last_attributed_touch_data__~channel" = 'downloaded_pdf' then 'Down_PDF' 
             else 'Other' end as "Source"
                 
 from branchdatarealtime."data" as branchdata
 where branchdata.name='INSTALL'
 and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) >= '20190701' and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) <= '20190731'
 group by 1,3
 order by 1 desc
