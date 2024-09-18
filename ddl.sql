CREATE TYPE cafe.restaurant_type AS ENUM 
    ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

CREATE TABLE cafe.restaurants
(
	restaurant_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	restaurant_name VARCHAR(50),
	restaurant_gm GEOMETRY,
	restaurant_type cafe.restaurant_type,
	restaurant_menu JSONB
);

CREATE TABLE cafe.managers
(
	manager_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	manager_name VARCHAR(50),
	manager_phone VARCHAR(50) UNIQUE
);

CREATE TABLE cafe.restaurant_manager_work_dates
(
	restaurant_uuid UUID REFERENCES cafe.restaurants(restaurant_uuid),
	manager_uuid UUID REFERENCES cafe.managers(manager_uuid),
	start_date DATE,
	end_date DATE,
	PRIMARY KEY (restaurant_uuid, manager_uuid)
);

CREATE TABLE cafe.sales
(
	date DATE,
	restaurant_uuid UUID REFERENCES cafe.restaurants(restaurant_uuid),
	avg_check numeric(6,2),
	PRIMARY KEY (date, restaurant_uuid)
);

INSERT INTO cafe.restaurants (restaurant_name, restaurant_gm, restaurant_type, restaurant_menu)
SELECT DISTINCT rs.cafe_name, ST_POINT(rs.longitude, rs.latitude), rs.type::cafe.restaurant_type, rm.menu
FROM raw_data.sales rs
LEFT JOIN raw_data.menu rm on rs.cafe_name = rm.cafe_name;

INSERT INTO cafe.managers (manager_name, manager_phone)
SELECT DISTINCT manager, manager_phone
FROM raw_data.sales;

INSERT INTO cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, start_date, end_date)
SELECT res.restaurant_uuid, man.manager_uuid, MIN(sales.report_date), MAX(sales.report_date)
FROM cafe.restaurants res
JOIN raw_data.sales sales ON res.restaurant_name = sales.cafe_name
JOIN cafe.managers man ON man.manager_name = sales.manager
GROUP BY res.restaurant_uuid, man.manager_uuid;

INSERT INTO cafe.sales (date, restaurant_uuid, avg_check)
SELECT sales.report_date, res.restaurant_uuid, sales.avg_check
FROM cafe.restaurants res
JOIN raw_data.sales sales ON res.restaurant_name = sales.cafe_name;

CREATE VIEW cafe.top_restaurants_by_avg_check AS
SELECT restaurant_type, restaurant_name, average_check
FROM (
  SELECT restaurant_type, restaurant_name, average_check,
    ROW_NUMBER() OVER (PARTITION BY restaurant_type ORDER BY average_check DESC) AS rank
  FROM (
    SELECT r.restaurant_type, r.restaurant_name, ROUND(AVG(s.avg_check), 2) AS average_check
    FROM cafe.sales s
    JOIN cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
    GROUP BY r.restaurant_type, r.restaurant_name
  ) AS avg_check_per_restaurant
) AS ranked_restaurants
WHERE rank <= 3;

SELECT * FROM cafe.top_restaurants_by_avg_check;

CREATE MATERIALIZED VIEW cafe.avg_change AS
SELECT 
    year,
    restaurant_name,
    restaurant_type,
    avg_check,
    LAG(avg_check) OVER (PARTITION BY restaurant_name, restaurant_type ORDER BY year) AS prev_avg_check,
    ROUND(((avg_check - LAG(avg_check) OVER (PARTITION BY restaurant_name, restaurant_type ORDER BY year)) / LAG(avg_check) OVER (PARTITION BY restaurant_name, restaurant_type ORDER BY year)) * 100, 2) AS percentage_change
FROM 
    (SELECT 
         r.restaurant_uuid,
         r.restaurant_name,
         r.restaurant_type,
         s.year,
         s.avg_check
     FROM 
         cafe.restaurants r
     LEFT JOIN 
         (SELECT 
              EXTRACT(YEAR FROM s.date) AS year,
              s.restaurant_uuid,
              ROUND(AVG(s.avg_check)::numeric, 2) AS avg_check
          FROM 
              cafe.sales s
          WHERE 
              EXTRACT(YEAR FROM s.date) != 2023
          GROUP BY 
              year, s.restaurant_uuid) s ON s.restaurant_uuid = r.restaurant_uuid
    ) subquery;

SELECT * FROM cafe.avg_change;

SELECT 
    r.restaurant_name,
    COUNT(DISTINCT rm.manager_uuid) AS manager_change
FROM 
    cafe.restaurants r
JOIN 
    cafe.restaurant_manager_work_dates rm USING (restaurant_uuid)
GROUP BY 
    r.restaurant_name
ORDER BY 
    manager_change DESC
LIMIT 3;

SELECT restaurant_name, pizza_amount
FROM (SELECT restaurant_name, (SELECT COUNT(*) AS pizza_amount FROM 
    jsonb_object_keys(restaurant_menu -> 'Пицца')),  DENSE_RANK() OVER (ORDER BY (SELECT COUNT(*) AS pizza_amount FROM 
    jsonb_object_keys(restaurant_menu -> 'Пицца')) DESC) AS rank
FROM cafe.restaurants
WHERE restaurant_type = 'pizzeria') AS subquery
WHERE rank = 1;

WITH menu_cte AS (
  SELECT
    restaurant_uuid,
    restaurant_name,
    restaurant_type,
    (jsonb_each_text(restaurant_menu->'Пицца')).*
  FROM
    cafe.restaurants
  WHERE
    restaurant_type = 'pizzeria'
),
menu_with_rank AS (
  SELECT
    restaurant_name,
    restaurant_type,
    key AS pizza_name,
    CAST(value AS INT) AS price,
    ROW_NUMBER() OVER (PARTITION BY restaurant_uuid ORDER BY CAST(value AS INT) DESC) AS price_rank
  FROM
    menu_cte
)
SELECT
  restaurant_name,
  restaurant_type,
  pizza_name,
  price
FROM
  menu_with_rank
WHERE
  price_rank = 1
ORDER BY
  restaurant_name ASC;

WITH dist AS (
  SELECT
    r1.restaurant_name AS rest1,
    r2.restaurant_name AS rest2,
    r1.restaurant_type AS type,
    ST_Distance(r1.restaurant_gm::geography, r2.restaurant_gm::geography) AS distance
  FROM cafe.restaurants r1
  JOIN cafe.restaurants r2 ON r1.restaurant_type = r2.restaurant_type
  WHERE r1.restaurant_name <> r2.restaurant_name
)
SELECT
  rest1 AS first_restaurant,
  rest2 AS second_restaurant,
  type,
  distance
FROM dist
GROUP BY rest1, type, rest2, distance
ORDER BY distance
LIMIT 1;

WITH restaurant_counts AS (
  SELECT
    d.id AS district_id,
    d.district_name,
    COUNT(r.restaurant_uuid) AS num_restaurants
  FROM
    cafe.districts d
    LEFT JOIN cafe.restaurants r
      ON ST_Within(ST_SetSRID(r.restaurant_gm, 4326), d.district_geom)
  GROUP BY
    d.id,
    d.district_name
)

SELECT
  district_name,
  num_restaurants
FROM
  restaurant_counts
WHERE
  num_restaurants = (
    SELECT
      MAX(num_restaurants)
    FROM
      restaurant_counts
  )

UNION

SELECT
  district_name,
  num_restaurants
FROM
  restaurant_counts
WHERE
  num_restaurants = (
    SELECT
      MIN(num_restaurants)
    FROM
      restaurant_counts
  )
ORDER BY
  num_restaurants DESC;
