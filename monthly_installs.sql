with clks as(
select clickstream_growthapp.timestampist as "Date1",
       clickstream_growthapp.userid as "User",
       clickstream_growthapp.gaid as "gaid",
       user1.studentinfo_grade as "Grade",
       count(case when clickstream_growthapp.eventlabel = 'app_doubt_submit' then 1 end) as "Doubts",
       count(case when clickstream_growthapp.eventlabel = 'app_study_pdf_item_clicked' then 1 end) as "PDF",
       count(case when clickstream_growthapp.eventlabel in ('app_classroom_thumbnail_clicked','app_classroom_video_clicked','app_classroom_playlist_viewall_clicked','app_home_video_click','app_home_playlist_click', 'app_classroom_videodetail_item_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_video_buttons_clicked') then 1 end) as "Video_Viewed",
       count(case when clickstream_growthapp.eventlabel in ('app_test_upcoming_playlist','app_test_notify_click','app_test_ongoing_click','app_test_normal_click','app_test_normal_viewall','app_test_attempted_tab_click','app_test_attempted_card_click') then 1 end) as "Test_viewed"
       
from clickstream_growthapp
     left join public."user" as user1 on user1.email = clickstream_growthapp.useremail

where clickstream_growthapp.timestampist::date > '20190201'

group by 1,2,3,4),
brdta as (
Select substring((trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second'))::text,6,5) as "Date2",
       branchdata.id as "install",
       branchdata.user_data__aaid as "aaid"
       
from branchdatarealtime."data" as branchdata
where branchdata.name='INSTALL'
and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) > '20190201'
)
select case when (user_app_segments.user_sessiondatetime_first::date >= '20190101' and user_app_segments.user_sessiondatetime_first::date <= '20190131') then 'January'
            when (user_app_segments.user_sessiondatetime_first::date >= '20190201' and user_app_segments.user_sessiondatetime_first::date <= '20190228') then 'February'
            when (user_app_segments.user_sessiondatetime_first::date >= '20190301' and user_app_segments.user_sessiondatetime_first::date <= '20190331') then 'March'
            when (user_app_segments.user_sessiondatetime_first::date >= '20190401' and user_app_segments.user_sessiondatetime_first::date <= '20190430') then 'April'
            when (user_app_segments.user_sessiondatetime_first::date >= '20190501' and user_app_segments.user_sessiondatetime_first::date <= '20190531') then 'May'
            when (user_app_segments.user_sessiondatetime_first::date >= '20190601' and user_app_segments.user_sessiondatetime_first::date <= '20190630') then 'June' end as "Month",
       clks.Grade as "Grade",
       count(distinct(clks.User)) as "Total Signups",
       count(distinct(brdta.install)) as "Total_install"

from user_app_segments
     left join clks on clks.User = user_app_segments.userid
     left join brdta on brdta.aaid = clks.gaid

group by 1,2
order by 1
