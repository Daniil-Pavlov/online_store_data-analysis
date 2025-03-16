-- Расчет для каждого дня, представленного в таблицах user_actions и courier_actions следующих показателей:
-- Число новых пользователей.
-- Число новых курьеров.
-- Общее число пользователей на текущий день.
-- Общее число курьеров на текущий день.
-- Прирост числа новых пользователей.
-- Прирост числа новых курьеров.
-- Прирост общего числа пользователей.
-- Прирост общего числа курьеров.

SELECT date, new_users, new_couriers, total_users, total_couriers, new_users_change, new_couriers_change, 
ROUND((total_users::decimal-(LAG(total_users) OVER (ORDER BY date)))*100/(LAG(total_users) OVER (ORDER BY date)),2)  AS total_users_growth,
ROUND((total_couriers::decimal-(LAG(total_couriers) OVER (ORDER BY date)))*100/(LAG(total_couriers) OVER (ORDER BY date)),2) AS total_couriers_growth
FROM (  SELECT date,new_users,new_couriers,
        SUM(new_users) OVER (ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::INT AS total_users,
        SUM(new_couriers) OVER (ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::INT AS total_couriers,
        ROUND((new_users::decimal-(LAG(new_users) OVER (ORDER BY date)))*100/(LAG(new_users) OVER (ORDER BY date)),2) AS new_users_change,
        ROUND((new_couriers::decimal-(LAG(new_couriers) OVER (ORDER BY date)))*100/(LAG(new_couriers) OVER (ORDER BY date)),2) AS new_couriers_change
        
        
        FROM (  SELECT date,COUNT(courier_id) FILTER (WHERE number_ac_cu = 1) AS new_couriers
                FROM (  SELECT time::DATE AS date,courier_id,
                        ROW_NUMBER() OVER (PARTITION BY courier_id ORDER BY time ) AS number_ac_cu
                        FROM courier_actions) AS courier
                GROUP BY date  ) AS c
        FULL JOIN ( SELECT date,COUNT(user_id) FILTER (WHERE number_ac_us = 1) AS new_users
                    FROM (  SELECT time::DATE AS date,user_id,
                            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time ) AS number_ac_us
                            FROM user_actions) AS use
                    GROUP BY date  ) AS u
        USING (date)
        ORDER BY date) AS a


