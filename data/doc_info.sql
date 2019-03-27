DROP TABLE IF EXISTS `doc_info`;

CREATE TABLE `doc_info` (
  `id`   int(11) NOT NULL,
  `doc_terms` int(11) NULL,
  `page_rank` double NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;