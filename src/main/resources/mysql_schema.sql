USE weather;

CREATE TABLE IF NOT EXISTS `station` (
  `station_id` int PRIMARY KEY,
  `station_code` VARCHAR(100) NOT NULL,
  `station_name` VARCHAR(200),
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `elevation` double DEFAULT NULL,
  `last_update` timestamp NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `measures` (
  `station_id` int NOT NULL,
  `date` date DEFAULT NULL,
  `precip` double DEFAULT NULL,
  `max_temp` int DEFAULT NULL,
  `min_temp` int DEFAULT NULL,
  `avg_wind_speed` double DEFAULT NULL,
  `peak_gust_time` double DEFAULT NULL,
  `last_update` timestamp NOT NULL,
   FOREIGN KEY (station_id) REFERENCES station(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `measure_errors` (
  `station_id` int NOT NULL,
  `date` date DEFAULT NULL,
  `precip` double DEFAULT NULL,
  `max_temp` int DEFAULT NULL,
  `min_temp` int DEFAULT NULL,
  `avg_wind_speed` double DEFAULT NULL,
  `peak_gust_time` double DEFAULT NULL,
  `error_timestamp` timestamp NOT NULL,
  `error_cause` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `measure_aggregates` (
  `month_of_year` VARCHAR(7),
  `pct_rainy_days` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




