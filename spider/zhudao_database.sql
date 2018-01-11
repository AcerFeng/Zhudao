CREATE TABLE `platform` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30) DEFAULT NULL,
  `url` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `category` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL DEFAULT '',
  `short_name` varchar(50) NOT NULL DEFAULT '',
  `pic` varchar(256) NOT NULL DEFAULT '',
  `icon` varchar(256) DEFAULT NULL,
  `small_icon` varchar(256) DEFAULT NULL,
  `count` int(11) DEFAULT NULL,
  `mb_url` varchar(256) DEFAULT '',
  `pc_url` varchar(256) DEFAULT '',
  `platform_id` int(11) unsigned DEFAULT NULL,
  `zhanqi_cateid` varchar(20) DEFAULT NULL,
  `created_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `platform_id` (`platform_id`),
  CONSTRAINT `category_ibfk_1` FOREIGN KEY (`platform_id`) REFERENCES `platform` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=607 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `anchor` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT NULL,
  `room_id` varchar(40) DEFAULT NULL,
  `room_name` varchar(128) DEFAULT NULL,
  `avatar` varchar(128) DEFAULT NULL,
  `owner_uid` varchar(30) DEFAULT NULL,
  `watchs` varchar(20) DEFAULT NULL,
  `desc` varchar(256) DEFAULT NULL,
  `category_id` int(11) unsigned DEFAULT NULL,
  `online` varchar(11) DEFAULT NULL,
  `cover` varchar(256) DEFAULT NULL,
  `platform_id` int(10) unsigned DEFAULT NULL,
  `created_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `platform_id` (`platform_id`),
  KEY `category_id` (`category_id`),
  CONSTRAINT `anchor_ibfk_1` FOREIGN KEY (`platform_id`) REFERENCES `platform` (`id`),
  CONSTRAINT `anchor_ibfk_2` FOREIGN KEY (`category_id`) REFERENCES `category` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;