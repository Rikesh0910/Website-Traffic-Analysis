-- ETL process

CREATE DATABASE db_website_traffic;

USE db_website_traffic;

CREATE TABLE website_data(
    session_id INT PRIMARY KEY,
    page_views INT CHECK (page_views >= 0),
    session_duration DECIMAL(10,2),
    bounce_rate DECIMAL(5,4) CHECK (bounce_rate BETWEEN 0 AND 1),
    traffic_source VARCHAR(100),
    time_on_page DECIMAL(10,2),
    previous_visits INT CHECK (previous_visits >= 0),
    conversion_rate DECIMAL(5,4) CHECK (conversion_rate BETWEEN 0 AND 1),
    user_type VARCHAR(50),
    conversion_status VARCHAR(50)
);

ALTER TABLE website_data
ADD bounce_category VARCHAR(255);

UPDATE website_data
SET bounce_category = 
	CASE 
		WHEN bounce_rate < 0.5 THEN "Low Bounce"
		ELSE "High Bounce"
    END;

SELECT
	*
FROM
	website_data;
    
SELECT
	COUNT(*)
FROM
	website_data;

SELECT
	MAX(bounce_rate),
    MIN(bounce_rate)
FROM
	website_data;
    
SELECT
	MAX(conversion_rate),
    MIN(conversion_rate)
FROM
	website_data;

SELECT DISTINCT 
	traffic_source
FROM
	website_data;
    
SELECT
	AVG(bounce_rate) AS avg_bounce_rate,
    AVG(conversion_rate) AS avg_conversion_rate,
    AVG(session_duration) AS avg_session_duration,
    AVG(page_views) AS avg_page_views
FROM
	website_data;
    
SELECT
	traffic_source,
    AVG(bounce_rate) AS avg_bounce_rate,
    AVG(conversion_rate) AS avg_conversion_rate,
    AVG(session_duration) AS avg_session_duration,
    AVG(page_views) AS avg_page_views
FROM
	website_data
GROUP BY traffic_source;

SELECT
	user_type,
	COUNT(*) AS sessions,
	AVG(conversion_rate) AS conversion_rate,
	AVG(session_duration) AS avg_duration
FROM 
	website_data
GROUP BY user_type;

SELECT
	bounce_category,
	COUNT(*) AS sessions,
	AVG(conversion_rate) AS conversion_rate
FROM
	website_data
GROUP BY bounce_category;

SELECT
	CASE
		WHEN page_views >= 5 THEN 'High Engagement'
		ELSE 'Low Engagement'
	END AS engagement_level,
	AVG(conversion_rate) AS conversion_rate
FROM 
	website_data
GROUP BY engagement_level;

UPDATE website_data
SET conversion_status = NULL;

UPDATE website_data
SET conversion_status = 
	CASE
    WHEN conversion_rate = 1 THEN "Guaranteed"
    WHEN conversion_rate >= 0.8 THEN "Highly Likely"
    WHEN conversion_rate >= 0.5 THEN "Likely"
    ELSE "Unlikely"
    END;
    
SELECT
	conversion_status,
	COUNT(*) AS total_sessions
FROM
	website_data
GROUP BY conversion_status;

SELECT
	conversion_status,
    AVG(bounce_rate) AS avg_bounce_rate
FROM
	website_data
GROUP BY conversion_status
ORDER BY avg_bounce_rate DESC;

-- Analytical tables

CREATE TABLE visitor_behavior AS
SELECT
    user_type,
    COUNT(*) AS sessions,
    AVG(time_on_page) AS avg_time_on_page,
    AVG(page_views) AS avg_pages,
    AVG(conversion_rate) AS avg_conversion_rate,
    AVG(bounce_rate) AS avg_bounce_rate
FROM website_data
GROUP BY user_type;
    
CREATE TABLE traffic_performance AS
SELECT
    traffic_source,
    COUNT(*) AS total_sessions,
    AVG(page_views) AS avg_pages_viewed,
    AVG(time_on_page) AS avg_time_on_page,
    AVG(bounce_rate) AS avg_bounce_rate,
    AVG(conversion_rate) AS avg_conversion_rate
FROM website_data
GROUP BY traffic_source;    

CREATE TABLE conversion_insights AS
SELECT
    conversion_status,
    COUNT(*) AS sessions
FROM website_data
GROUP BY conversion_status;