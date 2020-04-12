SELECT hotel, COUNT(*) FROM hotel WHERE is_canceled=0 GROUP BY hotel;


SELECT hotel, country, COUNT (is_canceled) AS COUNT FROM hotel WHERE is_canceled=0 AND arrival_date_month ='December' GROUP BY hotel, country ORDER BY COUNT DESC LIMIT 10;

