#after merged version 5.5 is tested

#all minor tables, add indexes to speedup joins
ALTER TABLE `prefix_bundle` ADD INDEX `userid_idx` (`userid`) USING BTREE;

ALTER TABLE `prefix_domains` ADD INDEX `userid_idx` (`userid`) USING BTREE;

ALTER TABLE `prefix_payment` ADD INDEX `userid_idx` (`userid`) USING BTREE;
ALTER TABLE `prefix_payment` ADD INDEX `date_idx` (`date`) USING BTREE;
ALTER TABLE `prefix_payment` ADD INDEX `expiry_idx` (`expiry`) USING BTREE;

ALTER TABLE `prefix_url` ADD INDEX `expiry_idx` (`expiry`) USING BTREE;
ALTER TABLE `prefix_url` ADD INDEX `user_idx` (`userid`) USING BTREE;

