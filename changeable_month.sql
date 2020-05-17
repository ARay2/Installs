with branchd as 
(
    Select (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) as "Date1",
    count(distinct(branchdata.id)) as "Total_install"

    from branchdatarealtime."data" as branchdata
    where branchdata.name='INSTALL'
        and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) > current_date - 180
    group by 1
),
usersegd as 
(
    select count(distinct(user_app_segments.gaid_first)) as "gaid",
    user_app_segments.user_sessiondatetime_first::date as "Date1"
    
    from user_app_segments
    group by 2
    
)
select  case when (branchd.Date1::date >= {{startdate}} and branchd.Date1::date <= {{enddate}}) then 'February'
             when (branchd.Date1::date >= {{startdate1}} and branchd.Date1::date <= {{enddate1}}) then 'March'
             when (branchd.Date1::date >= {{startdate2}} and branchd.Date1::date <= {{enddate2}}) then 'April'
             when (branchd.Date1::date >= {{startdate3}} and branchd.Date1::date <= {{enddate3}}) then 'May'
             when (branchd.Date1::date >= {{startdate4}} and branchd.Date1::date <= {{enddate4}}) then 'June' end as "Month",
        sum(branchd.Total_install) as "Total Install",
        sum(usersegd.gaid) as "Total Signups"

from branchd
     inner join usersegd on usersegd.Date1 = branchd.Date1
group by 1
