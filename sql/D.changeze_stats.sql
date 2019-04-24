# stats table

# prevent stat id from overflow !!!! do not do this in an overpopulated table
ALTER TABLE `prefix_stats` CHANGE `id` `id` BIGINT NOT NULL AUTO_INCREMENT;

#reduce size, speedup queries, enable indexing by removing 'text' blobs

ALTER TABLE `prefix_stats` CHANGE `browser` `browser` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL;
ALTER TABLE `prefix_stats` CHANGE `os` `os` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL;
ALTER TABLE `prefix_stats` CHANGE `country` `country` VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL;

# also able to index some columns for the stats to be faster. producing some large indexes.
ALTER TABLE `prefix_stats` ADD INDEX `browser_idx` (`browser`(50)) USING BTREE;
ALTER TABLE `prefix_stats` ADD INDEX `os_idx` (`os`(50)) USING BTREE;
ALTER TABLE `prefix_stats` ADD INDEX `country_idx` (`country`(50)) USING BTREE;
ALTER TABLE `prefix_stats` ADD INDEX `urlid_idx` (`urlid`) USING BTREE;


