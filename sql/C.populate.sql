
insert into `prefix_t_country`(`name`) select distinct `country` from `prefix_stats` order by `country` asc;
insert into `prefix_t_browser`(`name`) select distinct `browser` from `prefix_stats` order by `browser` asc;
insert into `prefix_t_os`(`name`) select distinct `os` from `prefix_stats` order by `os` asc;

# keep only the main part of the referrer, discarding subfolders, queries, get requests etc.
insert into prefix_t_referrer(url) select distinct substring_index(referer,'/',3) from prefix_stats;


SELECT @lastid=max(id) from prefix_stats;

#per year stats for each url
insert into `prefix_t_click_stats`(userid,urlid,type,date,clicks) select `urluserid`,`urlid`,3, makedate(year(`date`),1) as theyear, count(id) from prefix_stats where id<=@lastid group by urlid,theyear;

#per month stats for each url
insert into `prefix_t_click_stats`(userid,urlid,type,date,clicks) select `urluserid`,`urlid`,2, makedate(year(`date`),dayofyear(`date`)-dayofmonth(date)+1) as yearmonth, count(id) from prefix_stats where id<=@lastid group by urlid,yearmonth;

#per day for last thirty days for each url
insert into `prefix_t_click_stats`(userid,urlid,type,date,clicks) select `urluserid`,`urlid`,1, makedate(year(`date`),dayofyear(`date`)) as yearmonthday, count(id) from prefix_stats where id<=@lastid AND `date`>=(CURRENT_DATE()-30) group by urlid,yearmonthday;


#populate initial url-browser pair clicks table
insert into prefix_t_click_browser(userid,urlid,browser,clicks)
select t.urluserid, t.urlid, b.id, t.cnt 
from ( ( select urluserid, urlid, browser, count(id) as cnt from prefix_stats where id<=@lastid group by urlid, browser) as t
inner join prefix_t_browser as b
on t.browser = b.name
);

#populate url-country click stats table
insert into prefix_t_click_country(userid,urlid,country,clicks)
select t.urluserid, t.urlid, c.id, t.cnt
from ( ( select urluserid, urlid, country, count(id) as cnt from prefix_stats where id<=@lastid group by urlid, country) as t
inner join prefix_t_country as c
on t.country = c.name
);

#populate os click table
insert into prefix_t_click_os(userid,urlid,os,clicks)
select t.urluserid, t.urlid, o.id, t.cnt
from ( ( select urluserid, urlid, os, count(id) as cnt from prefix_stats where id<=@lastid group by urlid, os) as t
inner join prefix_t_os as o
on t.os = o.name
);

#populate referrer click table
insert into prefix_t_click_referrer(userid,urlid,referrer,clicks)
select t.urluserid, t.urlid, r.id, t.cnt
from ( ( select urluserid, urlid, substring_index(referer,'/',3) as ref, count(id) as cnt from prefix_stats where id<=@lastid group by urlid, ref) as t
inner join prefix_t_referrer as r
on t.ref = r.url
);

#insert into last stat
insert into prefix_last_stat(last_statid, date, errors) values(@lastid,now(),0);

#AT this point, install the newest version, test. if all is good, perform next step and go to D. change prefix_stats
// test thoroughly
#take care, this will take time
delete from prefix_stats where id<=@lastid;
optimize table prefix_stats;
#needs a repair, or optimize db afterwards.
