<?php
/***********************************************************
 *  zeep.ly Statistics Updater Script:
 *  
 *  (c) 2019, Thanasis Karpetis (tkarpetis@gmail.com)
 *
 *  Compacts raw stat table to a number of small tables 
 *  keeping clicks per url and time interval, country,
 *  browser, os and referrer.
 *
 *  This file is meant to be run as a cron job.
 *
 *  The file structure was based on the updater
 *  file shipped with Premium URL Shortener, and is
 *  using parts thereof to promote familiarity.
 *
 **********************************************************/

include("/home/zepadm/public_html/includes/config.php");


$db = new PDO("mysql:host=".$dbinfo["host"].";dbname=".$dbinfo["db"]."", $dbinfo["user"], $dbinfo["password"]);

$res = $db->query("SELECT `var` FROM `{$dbinfo["prefix"]}settings` WHERE `config`='maintenance' LIMIT 1");
if ($res==false) 
    exit();
$maintenance = $res->fetch(PDO::FETCH_ASSOC);

if ($maintenance["var"]=='1')
    exit();
//------odd-even stat table backups-----

$res= $db->query("SELECT NOW() AS `now`");
$weeknum=getWeek($res,"now",(int)floor(time()/(7*24*3600)));


$res= $db->query("SELECT MAX(`date`) AS `latest` FROM `{$dbinfo["prefix"]}t_last_stat` LIMIT 1");
$prevweek=getWeek($res,"latest",$weeknum);

if ($weeknum%2 ==1) 	
    $stat_suffix="odd";
else
    $stat_suffix="even";

if ($weeknum != $prevweek) { //new week starting now, purge all records older than last week
       $db->query("TRUNCATE TABLE `{$dbinfo["prefix"]}t_stats_{$stat_suffix}`");	
       $db->query("OPTIMIZE TABLE `{$dbinfo["prefix"]}t_stats_{$stat_suffix}`");
}


//- up to here-------------------

$query=get_query($dbinfo,$stat_suffix);
$i=0;
$numerrors=0;
foreach ($query as $q) {
	++$i;
	if ($db->query($q)==false) {
		++$numerrors;
		//$numerrors=$i;
		echo "error in query",i,"\n";
	}
}
// final action, report on this update.
$db->query("INSERT INTO `{$dbinfo["prefix"]}t_last_stat`(`last_statid`,`date`,`errors`)"
		." VALUES(@endid, now(),{$numerrors})");
		
unset($maintenance);
unset($res);
unset($config);
unset($query);
unset($dbinfo);
unset($db);		

//----------------------------------------------------------------------------------------------------------

function getWeek( $queryres, $fieldname, $defaultval) {
    
    if ($queryres==false) {  //query failed to execute
         $res=$defaultval;
    }
    else {
        $array=$queryres->fetch(PDO::FETCH_ASSOC);
        $timestring=$array[$fieldname];
        $res =(int)floor(strtotime($timestring)/(7*24*3600));//convert to timestamp, then to whole weeks
    }    
    return $res;
    
}

//----------------------------------------------------------------------------------------------------------

function get_query($dbinfo,$stat_suffix) {
	$query=array();
	//start transaction
	
	//start and ending id for the stats records. will be used throughout and recorded in the end.
	//
	$query[]="SELECT max(`last_statid`) FROM `{$dbinfo["prefix"]}t_last_stat` INTO @startid"; //this was included in the last update
	$query[]="SELECT max(`id`) FROM `{$dbinfo["prefix"]}stats` INTO @thelast";

	
	$query[]="SET @maxrows:=200000"; // tweak this number to adjust runtime.

	// do not allow the UPDATE to encompass too much in a run, this is a frequently run file.
	$query[]="SET @endid:= ( SELECT IF ( @startid+@maxrows < @thelast, @startid + @maxrows, @thelast) )";
	
	$query[]="SELECT min(`id`) FROM `{$dbinfo["prefix"]}stats` into @deletestart"; //the start of the delete interval
	$query[]="SET @deleteend:= (SELECT IF ( @deletestart+@maxrows > @endid, @endid, @deletestart + @maxrows) )";
	
    $query[]="SET @startid:= @startid+1"; // not to include previous last
    
    $query[]="SET @copystart:= 132118255";// playing cautious, ensuring return to original version for a few days

	$query[]="SET @copystart:= (SELECT IF ( @deletestart > @copystart, @deletestart, @copystart) )";// playing cautious, ensuring return to original version for a few days
	$query[]="SET @copyend:= @deleteend";
    
//=======================================CLICK STATS UPDATE=====================================================
//
//	IMPORTANT: 	in t_click_url, type 1,2,3 are day,month,year aggregate click stats
//			following this, t_temp_clicks use [type]+6 for new or still useful records
//			and [type]+3 for temporary 'used' status.
//			Aggregate clicks are gathered by day, then used to produce per month clicks
//			which in turn are used to create per year entries.	
//

//per day:-----------------------------------------------------------
	//first insert all new stats per day from startidid up to endid, per url in temp_clicks as type 7

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_clicks`(`userid`,`urlid`,`type`,`date`,`clicks`)"
		." SELECT `urluserid`,`urlid`,7, makedate(year(`date`),dayofyear(`date`)) AS `yearmonthday`, COUNT(`id`)"
		." FROM `{$dbinfo["prefix"]}stats`"
		." WHERE `id` BETWEEN @startid AND @endid"
		." GROUP BY `urlid`,yearmonthday";

	//then update all rows in t_click_url where the same url and date exists in temp clicks, 
	//setting the sum of the clicks as the new value

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_url` AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_clicks` AS r"
		." ON t.`urlid`=r.`urlid` AND t.`date`=r.`date`"
		." SET t.`clicks`=t.`clicks`+r.`clicks`,"
		." r.`type`=4" 		//marks AS used, MYSQL SPECIFIC. performs changes in both TABLEs.
		." WHERE t.`type`=1";

	//t_temp_clicks type was turned to 4 for those already in url_click. now INSERT the all new ones

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_url`(`userid`, `urlid`,`type`,`date`,`clicks`)"
		." SELECT `userid`, `urlid`,1,`date`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_clicks`"
		." WHERE `type`=7 AND `date`>= (CURRENT_DATE()-INTERVAL 30 DAY)";

	//return all rows to type 7 in temp_clicks, to be used in creating monthly sums

	$query[]="UPDATE `{$dbinfo["prefix"]}t_temp_clicks`"
		." SET `type`=7"
		." WHERE `type`=4";

//PER DAY UPDATE DONE.
//--------------------------------------------------------------
//per month:
	//from the type 7 records, create the new per month AND url records of type 8

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_clicks`(`userid`, `urlid`, `type`,`date`,`clicks`)"
		." SELECT r.`userid`, r.`urlid`, 8,makedate(year(`date`),dayofyear(`date`)-dayofmonth(`date`)+1) AS `yearmonth` ,sum(r.`clicks`)"
		." FROM `{$dbinfo["prefix"]}t_temp_clicks` AS r"
		." WHERE `type`=7"
		." GROUP BY `urlid`,`yearmonth`";

	//delete type 7 records( new daystats)
	$query[]="DELETE FROM `{$dbinfo["prefix"]}t_temp_clicks`"
		." WHERE `type`=7";

	//UPDATE existing records to new sum

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_url` AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_clicks` AS r"
		." ON t.`urlid`=r.`urlid` AND t.`date`=r.`date`"
		." SET t.`clicks`=t.`clicks`+r.`clicks`,"
		."     r.`type`=5"	 //UPDATEd combo url+yearmonth
		." WHERE t.`type`=2";

	//t_temp_clicks type was turned to 4 for those already in url_click. now INSERT the all new ones

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_url`(`userid`, `urlid`,`type`,`date`,`clicks`)"
		." SELECT `userid`, `urlid`,2,`date`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_clicks`"
		." WHERE `type`=8";

	//SET type 5(UPDATEd permonth) to type 8(per month) rows in {$dbinfo["prefix"]}temp_clicks for the year stat to work
	$query[]="UPDATE `{$dbinfo["prefix"]}t_temp_clicks`"
		." SET `type`=8"
		." WHERE `type`=5";

//PER MONTH DONE 
//---------------------------------------------------------
//per year:

	// FROM type 8 records, create per year, url records  of type 9(new per year)

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_clicks`(`userid`, `urlid`, `type`,`date`,`clicks`)"
		." SELECT r.`userid`, r.`urlid`, 9,MAKEDATE(YEAR(`date`),1) AS `theyear` , SUM(r.`clicks`)"
		." FROM `{$dbinfo["prefix"]}t_temp_clicks` AS r"
		." WHERE `type`=8"
		." GROUP BY `urlid`,`theyear`";

	//DELETE type 8 records( temp monthstats), not useful anymore
	$query[]="DELETE FROM `{$dbinfo["prefix"]}t_temp_clicks`"
		." WHERE `type`=8";

	//UPDATE year records to sum of clicks for all rows that exist both in temp_clicks AND click_ul

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_url` AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_clicks` AS r"
		." ON t.`urlid`=r.`urlid` AND t.`date`=r.`date`"
		." SET     t.`clicks`=t.`clicks`+r.`clicks`,"
		."         r.`type`=6" 		//udated year row counts
		." WHERE t.`type`=3";		// year rows

	// t_temp_clicks type was turned to 6 for those already in url_click. now INSERT the all new ones

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_url`(`userid`, `urlid`,`type`,`date`,`clicks`)"
		." SELECT `userid`, `urlid`,3,`date`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_clicks`"
		." WHERE `type`=9"; //unused

	// purge the array, we are done
	$query[]="TRUNCATE TABLE `{$dbinfo["prefix"]}t_temp_clicks`" ;

// end of clicks_url updating.

//===========UPDATE COUNTRY, REFERRER, BROWSER, OS LOOKUP TABLES ======================================================

// INSERT all new VALUES FROM the stats columns INTO the respective indexing TABLEs. INSERT IGNORE will not return error if name exists

	$query[]="INSERT IGNORE INTO `{$dbinfo["prefix"]}t_browser`(`name`) SELECT DISTINCT `browser` FROM `{$dbinfo["prefix"]}stats`"
       		." WHERE `id` BETWEEN @startid AND @endid";
	$query[]="INSERT IGNORE INTO `{$dbinfo["prefix"]}t_os`(`name`) SELECT DISTINCT `os` FROM `{$dbinfo["prefix"]}stats`"
       		." WHERE `id` BETWEEN @startid AND @endid";
	$query[]="INSERT IGNORE INTO `{$dbinfo["prefix"]}t_country`(`name`) SELECT DISTINCT `country` FROM `{$dbinfo["prefix"]}stats`"
		." WHERE `id` BETWEEN @startid AND @endid";
	$query[]="INSERT IGNORE INTO `{$dbinfo["prefix"]}t_referrer`(`url`) SELECT DISTINCT SUBSTRING_INDEX(`referer`,'/',3) "
		." FROM `{$dbinfo["prefix"]}stats` WHERE `id` BETWEEN @startid AND @endid";

//-------browser per url UPDATE----------------------------------
	//process raw stats INTO temporary rows.

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_catstat`(`userid`,`urlid`,`catid`,`done`,`clicks`)"
		." SELECT t.`urluserid`, t.`urlid`, b.`id`, 0, t.`cnt`"
		." FROM ( ( SELECT `urluserid`, `urlid`, `browser`, COUNT(`id`) AS `cnt`"
		." 		FROM `{$dbinfo["prefix"]}stats`"
		." 		WHERE `id` BETWEEN @startid AND @endid GROUP BY `urlid`, `browser`) AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_browser` AS b"
		." ON t.`browser` = b.`name`"
		." )";

	//then UPDATE all already existing url-browser pair counts, marking used rows FROM the temp TABLE

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_browser` AS b"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_catstat` AS t"
		." ON b.`urlid`=t.`urlid` AND b.`browser`=t.`catid`"
		." SET     b.`clicks`=b.`clicks`+t.`clicks`,"
		."         t.`done`=1";

	//now INSERT the all new rows

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_browser`(`userid`, `urlid`,`browser`,`clicks`)"
		." SELECT `userid`, `urlid`,`catid`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_catstat`"
		." WHERE `done`=0";

	// clear temp TABLE

	$query[]="TRUNCATE TABLE `{$dbinfo["prefix"]}t_temp_catstat`";

//-----browser done-------------------
//-------country per url UPDATE----------------------------------
	
	//process stat INTO temporary rows.

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_catstat`(`userid`,`urlid`,`catid`,`done`,`clicks`)"
		." SELECT t.`urluserid`, t.`urlid`, c.`id`, 0, t.`cnt`"
		." FROM ( ( SELECT `urluserid`, `urlid`, `country`, COUNT(`id`) AS `cnt`"
		."		FROM `{$dbinfo["prefix"]}stats`"
		." 		WHERE `id` BETWEEN @startid AND @endid GROUP BY `urlid`, `country`) AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_country` AS c"
		." ON t.`country` = c.`name`"
		." )";

	//then UPDATE all already existing url-country pair counts, marking used rows FROM the temp TABLE

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_country` AS c"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_catstat` AS t"
		." ON c.`urlid`=t.`urlid` AND c.`country`=t.`catid`"
		." SET     c.`clicks`=c.`clicks`+t.`clicks`,"
		."         t.`done`=1";

	// now INSERT the all new rows

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_country`(`userid`, `urlid`,`country`,`clicks`)"
		." SELECT `userid`, `urlid`,`catid`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_catstat`"
		." WHERE `done`=0";

	// clear temp TABLE

	$query[]="TRUNCATE TABLE `{$dbinfo["prefix"]}t_temp_catstat`";

//-----countries done-------------------
//-------os per url UPDATE----------------------------------
	
	//process stat INTO temporary rows.

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_catstat`(`userid`,`urlid`,`catid`,`done`,`clicks`)"
		." SELECT t.`urluserid`, t.`urlid`, o.`id`, 0, t.`cnt`"
		." FROM ( ( SELECT `urluserid`, `urlid`, `os`, COUNT(`id`) AS `cnt`"
		." 		FROM `{$dbinfo["prefix"]}stats`"
		." 		WHERE `id` BETWEEN @startid AND @endid GROUP BY `urlid`, `os`) AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_os` AS o"
		." ON t.`os` = o.`name`"
		." )";

	//then UPDATE all already existing url-os pair counts, marking used rows FROM the temp TABLE

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_os` AS o"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_catstat` AS t"
		." ON o.`urlid`=t.`urlid` AND o.`os`=t.`catid`"
		." SET     o.`clicks`=o.`clicks`+t.`clicks`,"
		."         t.`done`=1";

	// now INSERT the all new rows

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_os`(`userid`, `urlid`,`os`,`clicks`)"
		." SELECT `userid`, `urlid`,`catid`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_catstat`"
		." WHERE `done`=0";

	// clear temp TABLE

	$query[]="TRUNCATE TABLE `{$dbinfo["prefix"]}t_temp_catstat`";

//-----os done-------------------
//-------referrer per url UPDATE----------------------------------
	
	//process stat INTO temporary rows.

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_temp_catstat`(`userid`,`urlid`,`catid`,`done`,`clicks`)"
		." SELECT t.`urluserid`, t.`urlid`, r.`id`, 0, t.`cnt`"
		." FROM ( ( SELECT `urluserid`, `urlid`, SUBSTRING_INDEX(`referer`,'/',3) AS `ref`, COUNT(`id`) AS `cnt`"
		." 		FROM `{$dbinfo["prefix"]}stats`"
		." 		WHERE `id` BETWEEN @startid AND @endid GROUP BY `urlid`, `ref`) AS t"
		." INNER JOIN `{$dbinfo["prefix"]}t_referrer` AS r"
		." ON t.`ref` = r.`url`"
		." )";

	//then UPDATE all already existing url-referrer pair counts, marking used rows FROM the temp TABLE

	$query[]="UPDATE `{$dbinfo["prefix"]}t_click_referrer` AS r"
		." INNER JOIN `{$dbinfo["prefix"]}t_temp_catstat` AS t"
		." ON r.`urlid`=t.`urlid` AND r.`referrer`=t.`catid`"
		." SET     r.`clicks`=r.`clicks`+t.`clicks`,"
		."         t.`done`=1";

	// now INSERT the all new rows

	$query[]="INSERT INTO `{$dbinfo["prefix"]}t_click_referrer`(`userid`, `urlid`,`referrer`,`clicks`)"
		." SELECT `userid`, `urlid`,`catid`,`clicks`"
		." FROM `{$dbinfo["prefix"]}t_temp_catstat`"
		." WHERE `done`=0";

	// clear temp TABLE

	$query[]="TRUNCATE TABLE `{$dbinfo["prefix"]}t_temp_catstat`";

//-----referrer done-------------------
	
    $query[]="INSERT INTO `{$dbinfo["prefix"]}stats_new` ( SELECT * FROM `{$dbinfo["prefix"]}stats` WHERE `id` between @copystart and @copyend )";	//remove this after two weeks have passed. sat july 6 2019.
    $query[]="INSERT INTO `{$dbinfo["prefix"]}t_stats_{$stat_suffix}` ( SELECT * FROM `{$dbinfo["prefix"]}stats` WHERE `id` between @copystart and @copyend )";	
    
//stats records are no longer needed. deleting little by little.
	$query[]="DELETE FROM `{$dbinfo["prefix"]}stats` WHERE `id` between @deletestart and @deleteend";

//delete daily stats for days too far in the past
	$query[]="DELETE FROM `{$dbinfo["prefix"]}t_click_url` WHERE `type`=1 AND `date`< (CURRENT_DATE()-INTERVAL 62 DAY)";
	$query[]="DELETE FROM `{$dbinfo["prefix"]}t_click_url` WHERE `type`=2 AND `date`< (CURRENT_DATE()-INTERVAL 25 MONTH)";
	$query[]="DELETE FROM `{$dbinfo["prefix"]}t_last_stat` WHERE  `date`< (CURRENT_DATE()-INTERVAL 30 DAY)";

//compact stats TABLE
	$query[]="OPTIMIZE TABLE `{$dbinfo["prefix"]}stats`";//enable this when the table is fully incorporated
	$query[]="OPTIMIZE TABLE `{$dbinfo["prefix"]}t_click_url`";
	$query[]="OPTIMIZE TABLE `{$dbinfo["prefix"]}t_last_stat`";
	$query[]="OPTIMIZE TABLE `{$dbinfo["prefix"]}t_temp_catstat`";
	$query[]="OPTIMIZE TABLE `{$dbinfo["prefix"]}t_temp_clicks`";
//----------------------------------------------------------------------------------------------------------------

//commit
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	return $query;
}
?>
