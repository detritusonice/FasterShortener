
CREATE TABLE `prefix_t_browser` ( `id` INT NOT NULL AUTO_INCREMENT , `name` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL , PRIMARY KEY (`id`), UNIQUE `idx_browsername` (`name`(50)) USING BTREE) ENGINE = MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `prefix_t_country` ( `id` INT NOT NULL AUTO_INCREMENT , `name` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL , PRIMARY KEY (`id`) USING BTREE, UNIQUE `idx_countryname` (`name`(50))) ENGINE = MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `prefix_t_os` ( `id` INT NOT NULL AUTO_INCREMENT , `name` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL , PRIMARY KEY (`id`) USING BTREE, UNIQUE `idx_osname` (`name`(50))) ENGINE = MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `prefix_t_referrer` ( `id` INT NOT NULL AUTO_INCREMENT , `url` VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL , PRIMARY KEY (`id`) USING BTREE, UNIQUE `idx_url` (`url`(100))) ENGINE = MyISAM DEFAULT CHARSET=utf8;
# keeping the last processed entry from the stat table

CREATE TABLE `prefix_t_last_stat` ( `id` BIGINT NOT NULL AUTO_INCREMENT , `last_statid` BIGINT NOT NULL , `date` DATETIME NOT NULL , `errors` SMALLINT NOT NULL DEFAULT '0', PRIMARY KEY (`id`), INDEX `idx_lastid` (`last_statid`)) ENGINE = MyISAM DEFAULT CHARSET=utf8;

#this is mandatory for the updateStats script to begin processing
INSERT INTO `prefix_t_last_stat` (`last_statid`,`date`,`errors`) VALUES(0,NOW(),0);

CREATE TABLE `prefix_t_click_url` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `type` INT NOT NULL , `date` DATE NOT NULL , `clicks` BIGINT NOT NULL , INDEX (`userid`) USING BTREE, INDEX `urlid_idx` (`urlid`) USING BTREE, INDEX `type_idx` (`type`) USING BTREE, INDEX `date_idx` (`date`) USING BTREE) ENGINE = MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `prefix_t_click_browser` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `browser` INT NOT NULL , `clicks` BIGINT NOT NULL , INDEX `idx_userid` (`userid`), INDEX `idx_urlid` (`urlid`), INDEX `idx_browser` (`browser`)) ENGINE = MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `prefix_t_click_os` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `os` INT NOT NULL , `clicks` BIGINT NOT NULL , INDEX `idx_userid` (`userid`), INDEX `idx_urlid` (`urlid`), INDEX `idx_os` (`os`)) ENGINE = MyISAM;

CREATE TABLE `prefix_t_click_country` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `country` INT NOT NULL , `clicks` BIGINT NOT NULL , INDEX `idx_userid` (`userid`), INDEX `idx_urlid` (`urlid`), INDEX `idx_country` (`country`)) ENGINE = MyISAM DEFAULT CHARSET=utf8;


CREATE TABLE `prefix_t_click_referrer` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `referrer` INT NOT NULL , `clicks` BIGINT NOT NULL , INDEX `idx_userid` (`userid`), INDEX `idx_urlid` (`urlid`), INDEX `idx_referrer` (`referrer`)) ENGINE = MyISAM DEFAULT CHARSET=utf8;

#-------temporary tables to avoid insert-delete cycles in the processed stat tables
CREATE TABLE `prefix_t_temp_clicks` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `type` INT NOT NULL , `date` DATE NOT NULL , `clicks` BIGINT NOT NULL , INDEX (`userid`) USING BTREE, INDEX `urlid_idx` (`urlid`) USING BTREE, INDEX `type_idx` (`type`) USING BTREE, INDEX `date_idx` (`date`) USING BTREE) ENGINE = MyISAM DEFAULT CHARSET=utf8; 

CREATE TABLE `prefix_t_temp_catstat` ( `userid` INT NOT NULL , `urlid` BIGINT NOT NULL , `catid` INT NOT NULL , `done` SMALLINT NOT NULL , `clicks` BIGINT NOT NULL , INDEX `idx_userid` (`userid`), INDEX `idx_urlid` (`urlid`), INDEX `idx_catid` (`catid`), INDEX `idx_done` (`done`)) ENGINE = MyISAM DEFAULT CHARSET=utf8;

