DROP TABLE IF EXISTS `inverted_index`;

CREATE TABLE `inverted_index` (
  `term` varchar(1000) NOT NULL,
  `id`   int(11) NOT NULL,
  `freq` int(11) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE index inverted_index_wiki_id_fk
  ON inverted_index (id);

