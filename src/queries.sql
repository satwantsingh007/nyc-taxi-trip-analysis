-- queries.sql

-- Query for daily trips in 2019 and 2020
SELECT date, SUM(total_trips) AS total_trips
FROM Taxi_trip
WHERE strftime('%Y', date) IN ('2019', '2020')
GROUP BY date
ORDER BY date;

-- Query for monthly trips in 2019 and 2020
SELECT strftime('%Y-%m-01', date) AS month, SUM(total_trips) AS total_trips
FROM Taxi_trip
WHERE strftime('%Y', date) IN ('2019', '2020')
GROUP BY month
ORDER BY month;
