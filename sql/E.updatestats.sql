#start transaction

#UPDATING Queries:------------------------------------------------------------------------------------------------------
SELECT @startid:=(select max(last_statid) from prefix_t_last_stat); #this was included in the last update
SELECT @thelast:=( select max(id) from prefix_stats);

SET @maxrows:=400000;
SET @endid:= ( select if ( @startid+@maxrows < @thelast,@startid + @maxrows ,@thelast) ); #do not allow the update to encompass too much in a run
SET @startid:= @startid+1;					#start one after the last previously processed.

#=======================================CLICK STATS UPDATE=====================================================
#  per day:-----------------------------------------------------------
#first insert all new stats per day from prevlastid up to newlastid, per url in clickstats as type 7( temporaries are +6 of the normal type)

insert into `prefix_t_temp_clicks`(userid,urlid,type,date,clicks)
select `urluserid`,`urlid`,7, makedate(year(`date`),dayofyear(`date`)) as yearmonthday, count(id)
from prefix_stats
where id between @startid AND @endid
group by urlid,yearmonthday;
#GOOD

#then update all rows in prefix_t_click_url where the same url and date exists in temp clicks, setting the sum of the clicks as new value

update prefix_t_click_url as t
inner join prefix_t_temp_clicks as r
on t.urlid=r.urlid and t.date=r.date
set 	t.clicks=t.clicks+r.clicks,
	r.type=4 #updated row sum
where t.type=1;

# prefix_t_temp_clicks type was turned to 4 for those already in url_click. now insert the all new ones

insert into prefix_t_click_url(userid, urlid,type,date,clicks)
select userid, urlid,1,date,clicks
from prefix_t_temp_clicks
where type=7 and date >=(CURRENT_DATE()-30);

#return all rows to type 7(temporary per day) in prefix_temp_clicks

update prefix_t_temp_clicks
set type=7
where type=4;

#PER DAY UPDATE DONE.
#--------------------------------------------------------------
#per month:
#from the type 7 records, create the new per month and url records of type 8

insert into prefix_t_temp_clicks(userid, urlid, type,date,clicks)
select r.userid, r.urlid, 8,makedate(year(`date`),dayofyear(`date`)-dayofmonth(`date`)+1) as yearmonth , sum(r.clicks)
from prefix_t_temp_clicks as r
where type=7
group by urlid,yearmonth;

#delete type 7 records( new daystats)
delete from prefix_t_temp_clicks
where type=7;

#update existing records to new sum

update prefix_t_click_url as t
inner join prefix_t_temp_clicks as r
on t.urlid=r.urlid and t.date=r.date
set     t.clicks=t.clicks+r.clicks,
        r.type=5 #updated combo url+yearmonth
where t.type=2;

# prefix_t_temp_clicks type was turned to 4 for those already in url_click. now insert the all new ones

insert into prefix_t_click_url(userid, urlid,type,date,clicks)
select userid, urlid,2,date,clicks
from prefix_t_temp_clicks
where type=8;

#set type 5(updated permonth) to type 8(per month) rows in prefix_temp_clicks for the year stat to work
update prefix_t_temp_clicks
set type=8
where type=5;

#PER MONTH DONE ---------------------------------------------------------

#per year:

# from type 8 records, create per year, url records  of type 9(new per year)

insert into prefix_t_temp_clicks(userid, urlid, type,date,clicks)
select r.userid, r.urlid, 9,makedate(year(`date`),1) as theyear , sum(r.clicks)
from prefix_t_temp_clicks as r
where type=8
group by urlid,theyear;

#delete type 8 records( temp monthstats), not useful anymore
delete from prefix_t_temp_clicks
where type=8;

#update year records to sum of clicks for all rows that exist both in temp_clicks and click_ul

update prefix_t_click_url as t
inner join prefix_t_temp_clicks as r
on t.urlid=r.urlid and t.date=r.date
set     t.clicks=t.clicks+r.clicks,
        r.type=6 #udated year row counts
where t.type=3;# year rows

# prefix_t_temp_clicks type was turned to 6 for those already in url_click. now insert the all new ones

insert into prefix_t_click_url(userid, urlid,type,date,clicks)
select userid, urlid,3,date,clicks
from prefix_t_temp_clicks
where type=9; #unused

# purge the array
truncate table `prefix_t_temp_clicks` ;

# end of clickstats updating.

#===========UPDATE COUNTRY, REFERRER, BROWSER, OS LOOKUP TABLES ======================================================

# insert all new values from the stats columns into the respective indexing tables. insert ignore will not return error if name exists

insert ignore into prefix_t_browser(name) select distinct browser from prefix_stats where id between @startid AND @endid;
insert ignore into prefix_t_os(name) select distinct os from prefix_stats where id between @startid AND @endid;
insert ignore into prefix_t_country(name) select distinct country from prefix_stats where id between @startid AND @endid;
insert ignore into prefix_t_referrer(url) select distinct SUBSTRING_INDEX(referer,'/',3) from prefix_stats where id between @startid AND @endid;

#-------browser per url update----------------------------------
#process prefix_stat into temporary rows.

insert into `prefix_t_temp_catstat`(userid,urlid,catid,done,clicks)
select t.urluserid, t.urlid, b.id, 0, t.cnt
from ( ( select urluserid, urlid, browser, count(id) as cnt from prefix_stats where id between @startid and @endid group by urlid, browser) as t
inner join prefix_t_browser as b
on t.browser = b.name
);

#then update all already existing url-browser pair counts, marking used rows from the temp table

update prefix_t_click_browser as b
inner join prefix_t_temp_catstat as t
on b.urlid=t.urlid and b.browser=t.catid
set     b.clicks=b.clicks+t.clicks,
        t.done=1;

# now insert the all new rows

insert into prefix_t_click_browser(userid, urlid,browser,clicks)
select userid, urlid,catid,clicks
from prefix_t_temp_catstat
where done=0;

# clear temp table

truncate table `prefix_t_temp_catstat`;
#-----browser done-------------------
#-------country per url update----------------------------------
#process prefix_stat into temporary rows.

insert into `prefix_t_temp_catstat`(userid,urlid,catid,done,clicks)
select t.urluserid, t.urlid, c.id, 0, t.cnt
from ( ( select urluserid, urlid, country, count(id) as cnt from prefix_stats where id between @startid and @endid group by urlid, country) as t
inner join prefix_t_country as c
on t.country = c.name
);

#then update all already existing url-country pair counts, marking used rows from the temp table

update prefix_t_click_country as c
inner join prefix_t_temp_catstat as t
on c.urlid=t.urlid and c.country=t.catid
set     c.clicks=c.clicks+t.clicks,
        t.done=1;

# now insert the all new rows

insert into prefix_t_click_country(userid, urlid,country,clicks)
select userid, urlid,catid,clicks
from prefix_t_temp_catstat
where done=0;

# clear temp table

truncate table `prefix_t_temp_catstat`;
#-----countries done-------------------
#-------os per url update----------------------------------
#process prefix_stat into temporary rows.

insert into `prefix_t_temp_catstat`(userid,urlid,catid,done,clicks)
select t.urluserid, t.urlid, o.id, 0, t.cnt
from ( ( select urluserid, urlid, os, count(id) as cnt from prefix_stats where id between @startid and @endid group by urlid, os) as t
inner join prefix_t_os as o
on t.os = o.name
);

#then update all already existing url-os pair counts, marking used rows from the temp table

update prefix_t_click_os as o
inner join prefix_t_temp_catstat as t
on o.urlid=t.urlid and o.os=t.catid
set     o.clicks=o.clicks+t.clicks,
        t.done=1;

# now insert the all new rows

insert into prefix_t_click_os(userid, urlid,os,clicks)
select userid, urlid,catid,clicks
from prefix_t_temp_catstat
where done=0;

# clear temp table

truncate table `prefix_t_temp_catstat`;
#-----os done-------------------
#-------referrer per url update----------------------------------
#process prefix_stat into temporary rows.

insert into `prefix_t_temp_catstat`(userid,urlid,catid,done,clicks)
select t.urluserid, t.urlid, r.id, 0, t.cnt
from ( ( select urluserid, urlid, substring_index(referer,'/',3) as ref, count(id) as cnt from prefix_stats where id between @startid and @endid group by urlid, ref) as t
inner join prefix_t_referrer as r
on t.ref = r.url
);

#then update all already existing url-referrer pair counts, marking used rows from the temp table

update prefix_t_click_referrer as r
inner join prefix_t_temp_catstat as t
on r.urlid=t.urlid and r.referrer=t.catid
set     r.clicks=r.clicks+t.clicks,
        t.done=1;

# now insert the all new rows

insert into prefix_t_click_referrer(userid, urlid,referrer,clicks)
select userid, urlid,catid,clicks
from prefix_t_temp_catstat
where done=0;

# clear temp table

truncate table `prefix_t_temp_catstat`;

#-----referrer done-------------------
#=========RECORD LAST ZE_STATS ID INCLUDED IN THE PROCESSED STATS==============================================
#in a php script version of this, you could record the errors that happened into the errors field

insert into prefix_t_last_stat(last_statid,last_date,errors) 
values(@endid, now(),0);

#prefix_stats records are no longer needed.
delete from prefix_stats where id <= @endid;

#compact stats table when updates are on new click records only.
# OPTIMIZE TABLE prefix_stats;
OPTIMIZE TABLE prefix_t_click_url;
OPTIMIZE TABLE prefix_t_temp_catstat;
OPTIMIZE TABLE prefix_t_temp_clicks;
#----------------------------------------------------------------------------------------------------------------

#commit
