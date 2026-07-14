-- =====================================================
-- REINEX ANALYTICS LAYER
-- Business Intelligence Queries
-- =====================================================


-- =====================================================
-- 1. Total Revenue
-- =====================================================
SELECT
    ROUND(SUM(fare_amount), 2) AS total_revenue
FROM fact_trip;


-- =====================================================
-- 2. Average Fare
-- =====================================================
SELECT
    ROUND(AVG(fare_amount), 2) AS average_fare
FROM fact_trip;


-- =====================================================
-- 3. Average Tip
-- =====================================================
SELECT
    ROUND(AVG(tip_amount), 2) AS average_tip
FROM fact_trip;


-- =====================================================
-- 4. Total Tolls Collected
-- =====================================================
SELECT
    ROUND(SUM(tolls_amount), 2) AS total_tolls
FROM fact_trip;


-- =====================================================
-- 5. Trips Per Day
-- =====================================================
SELECT
    date_key,
    COUNT(*) AS total_trips
FROM fact_trip
GROUP BY date_key
ORDER BY date_key;


-- =====================================================
-- 6. Peak Pickup Hours
-- =====================================================
SELECT
    t.hour,
    COUNT(*) AS total_trips
FROM fact_trip f
JOIN dim_time t
ON f.date_key = t.date_key
GROUP BY t.hour
ORDER BY total_trips DESC;


-- =====================================================
-- 7. Weekend vs Weekday Trips
-- =====================================================
SELECT
    t.is_weekend,
    COUNT(*) AS total_trips
FROM fact_trip f
JOIN dim_time t
ON f.date_key = t.date_key
GROUP BY t.is_weekend;


-- =====================================================
-- 8. Top Drivers by Revenue
-- =====================================================
SELECT
    driver_id,
    ROUND(SUM(fare_amount),2) AS revenue
FROM fact_trip
GROUP BY driver_id
ORDER BY revenue DESC
LIMIT 10;


-- =====================================================
-- 9. Trips Per Driver
-- =====================================================
SELECT
    driver_id,
    COUNT(*) AS total_trips
FROM fact_trip
GROUP BY driver_id
ORDER BY total_trips DESC
LIMIT 10;


-- =====================================================
-- 10. Top Pickup Locations
-- =====================================================
SELECT
    pickup_location_id,
    COUNT(*) AS pickups
FROM fact_trip
GROUP BY pickup_location_id
ORDER BY pickups DESC
LIMIT 10;


-- =====================================================
-- 11. Top Dropoff Locations
-- =====================================================
SELECT
    dropoff_location_id,
    COUNT(*) AS dropoffs
FROM fact_trip
GROUP BY dropoff_location_id
ORDER BY dropoffs DESC
LIMIT 10;


-- =====================================================
-- 12. Most Popular Routes
-- =====================================================
SELECT
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) AS total_trips
FROM fact_trip
GROUP BY
    pickup_location_id,
    dropoff_location_id
ORDER BY total_trips DESC
LIMIT 10;


-- =====================================================
-- 13. Average Trip Duration
-- =====================================================
SELECT
    ROUND(AVG(trip_duration_sec)/60,2) AS avg_trip_duration_minutes
FROM fact_trip;


-- =====================================================
-- 14. Average Trip Distance
-- =====================================================
SELECT
    ROUND(AVG(trip_distance),2) AS avg_trip_distance
FROM fact_trip;


-- =====================================================
-- 15. Longest Trips
-- =====================================================
SELECT
    trip_id,
    trip_distance,
    trip_duration_sec
FROM fact_trip
ORDER BY trip_distance DESC
LIMIT 10;