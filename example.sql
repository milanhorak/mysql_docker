USE mysql_docker_db;

SELECT * FROM result_tb ORDER by date, adId;

-- how many impressions the ad with ID X had on day Y?
SELECT date, adId, impressionsCount FROM result_tb WHERE date = '2021-04-21' AND adID = 1;

-- the total number of clicks in the system per day Y?
SELECT date, clicksCount FROM result_tb WHERE date = '2021-04-21';
SELECT SUM(clicksCount) AS 'SUM_clicks' FROM result_tb WHERE date = '2021-04-21';

DELETE FROM result_tb;
DROP TABLE result_tb;
DROP DATABASE seznam_vr_db;
