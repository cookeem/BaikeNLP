DROP TABLE IF EXISTS `dics_list`;
DROP TABLE IF EXISTS `dics_term`;
DROP TABLE IF EXISTS `proc_term`;
DROP TABLE IF EXISTS `proc_tags`;

CREATE TABLE `dics_list` (
  `dicid` int(10) NOT NULL AUTO_INCREMENT COMMENT '搜狗词典ID',
  `name` varchar(50) NOT NULL DEFAULT '' COMMENT '词典名称',
  `category` varchar(100) NOT NULL DEFAULT '' COMMENT '词典大类',
  `count` int(10) NOT NULL DEFAULT '0' COMMENT '词典下载数量',
  PRIMARY KEY (`dicid`),
  KEY `idx_count` (`count`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='搜狗词典表';

CREATE TABLE `dics_term` (
  `termid` int(10) NOT NULL AUTO_INCREMENT COMMENT '单词ID',
  `term` varchar(40) NOT NULL DEFAULT '' COMMENT '词汇',
  `termfreq` int(10) NOT NULL DEFAULT '0' COMMENT '词频',
  `dicid` int(10) NOT NULL DEFAULT '0' COMMENT '来源的搜狗词典ID',
  PRIMARY KEY (`termid`),
  KEY `idx_term` (`term`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='搜狗词汇表';

CREATE TABLE `proc_term` (
  `termid` int(10) NOT NULL AUTO_INCREMENT COMMENT '单词ID',
  `term` varchar(40) NOT NULL DEFAULT '' COMMENT '词汇',
  `termtype` varchar(10) NOT NULL DEFAULT '' COMMENT '单词类型',
  `termfreq` int(10) NOT NULL DEFAULT '0' COMMENT '词频',
  PRIMARY KEY (`termid`),
  KEY `idx_term` (`term`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='处理的百科词汇表';

CREATE TABLE `proc_tags` (
  `tagid` int(10) NOT NULL AUTO_INCREMENT COMMENT '标签ID',
  `tag` varchar(40) NOT NULL DEFAULT '' COMMENT '标签',
  `tagcount` int(10) NOT NULL DEFAULT '0' COMMENT '标签出现次数',
  PRIMARY KEY (`tagid`),
  UNIQUE KEY `idx_tag` (`tag`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='处理的百科标签表';
