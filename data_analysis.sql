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
        
-- Расчет для каждого дня, представленного в таблицах user_actions и courier_actions следующих показателей:
        
-- Число платящих пользователей.
-- Число активных курьеров.
-- Долю платящих пользователей в общем числе пользователей на текущий день.
-- Долю активных курьеров в общем числе курьеров на текущий день.
        
SELECT  date,
paying_users,active_couriers, 
ROUND(paying_users::decimal *100/ SUM(paying_users_share) OVER (ORDER BY date RANGE BETWEEN unbounded preceding and current row),2) AS paying_users_share,
ROUND(active_couriers::decimal *100/ SUM(active_couriers_share) OVER (ORDER BY date RANGE BETWEEN unbounded preceding and current row),2) AS active_couriers_share
FROM (  SELECT  time::DATE AS date, 
                COUNT(DISTINCT(user_id)) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS paying_users,
                COUNT(DISTINCT(user_id)) FILTER (WHERE row_number = 1) AS paying_users_share
        FROM (  SELECT *, row_number() OVER (PARTITION BY user_id ORDER BY time)
                FROM user_actions) AS fs
        GROUP BY 1) AS first
FULL JOIN ( SELECT time::DATE AS date, 
                COUNT(DISTINCT(courier_id)) FILTER (WHERE order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')) AS active_couriers,
                COUNT(DISTINCT(courier_id)) FILTER (WHERE row_number = 1) AS active_couriers_share
            FROM (  SELECT *, row_number() OVER (PARTITION BY courier_id ORDER BY time)
                    FROM courier_actions) AS se
            GROUP BY 1) AS second
USING (date)
ORDER BY date

-- Расчет для каждого дня, представленного в таблице user_actions следующих показателей:

-- Долю пользователей, сделавших в этот день всего один заказ, в общем количестве платящих пользователей.
-- Долю пользователей, сделавших в этот день несколько заказов, в общем количестве платящих пользователе

SELECT date, 
ROUND(single_order_users_share*100::decimal/paying_users,2) AS single_order_users_share,
ROUND(several_orders_users_share*100::decimal/paying_users,2) AS several_orders_users_share
FROM (  SELECT time::DATE AS date, COUNT(DISTINCT(user_id))  AS paying_users
        FROM (  SELECT *, row_number() OVER (PARTITION BY user_id ORDER BY time)
                FROM user_actions) AS a
        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order' )
        GROUP BY 1
        ORDER BY 1) AS d
FULL JOIN ( SELECT date, 
            COUNT (user_id) FILTER (WHERE paying_users = 1 ) AS single_order_users_share,
            COUNT (user_id) FILTER (WHERE paying_users > 1 ) AS several_orders_users_share
            FROM (  SELECT time::DATE AS date,user_id, COUNT(order_id)  AS paying_users
                    FROM (  SELECT *, row_number() OVER (PARTITION BY user_id ORDER BY time)
                            FROM user_actions) AS c
                    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order' )
                    GROUP BY 1,2) AS b
            GROUP BY 1) AS f
USING (date)

-- Расчет для каждого дня, представленного в таблице user_actions следующих показателей:

-- Общее число заказов.
-- Число первых заказов (заказов, сделанных пользователями впервые).
-- Число заказов новых пользователей (заказов, сделанных пользователями в тот же день, когда они впервые воспользовались сервисом).
-- Долю первых заказов в общем числе заказов (долю п.2 в п.1).
-- Долю заказов новых пользователей в общем числе заказов (долю п.3 в п.1).

SELECT time::date AS date, 
COUNT(order_id) AS orders, 
COUNT(order_id) FILTER (WHERE time = fst) AS first_orders,  
COUNT(order_id) FILTER (WHERE time::date = reg::date) AS new_users_orders,
ROUND(COUNT(order_id) FILTER (WHERE time = fst)::decimal*100/COUNT(order_id),2) AS first_orders_share,
ROUND(COUNT(order_id) FILTER (WHERE time::date = reg::date)::decimal*100/COUNT(order_id),2) AS new_users_orders_share

FROM (  SELECT *,  MIN (time) OVER (PARTITION BY user_id ORDER BY time  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS reg,
        MIN (CASE WHEN order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') THEN time END) OVER (PARTITION BY user_id ORDER BY time  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fst
                                          
        FROM user_actions
        ORDER BY user_id, time) AS a
WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
GROUP BY 1
ORDER BY 1 

-- Расчет для каждого дня, представленного в таблицах user_actions, courier_actions и orders следующих показателей:

-- Число платящих пользователей на одного активного курьера.
-- Число заказов на одного активного курьера.

SELECT date,ROUND( us/cou::decimal,2) AS users_per_courier, ROUND( ord/cou::decimal,2) AS orders_per_courier
FROM (  SELECT time::date AS date, COUNT(DISTINCT(user_id)) AS us, COUNT(user_id) AS ord
        FROM user_actions
        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
        GROUP BY time::date) AS a 
FULL JOIN ( SELECT time::date AS date, COUNT(DISTINCT(courier_id)) AS cou
            FROM courier_actions
            WHERE order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')
            GROUP BY time::date) AS b
USING (date)
ORDER BY date

-- Расчет для каждого дня, представленного в таблице courier_actions следующих показателей:

-- Среднее время доставки заказа.

SELECT date, CEIL(AVG(minutes_to_deliver))::int AS minutes_to_deliver
FROM (  SELECT time::date AS date,  DATE_PART('minute',(MAX(time) OVER (PARTITION BY order_id ORDER BY time)) - (MIN(time) OVER (PARTITION BY order_id ORDER BY time))) AS minutes_to_deliver
        FROM courier_actions
        WHERE order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')) AS a
WHERE minutes_to_deliver != 0
GROUP BY date
ORDER BY date

-- Расчет на основе данных в таблице orders для каждого часа в сутках следующих показателей:

-- Число успешных (доставленных) заказов.
-- Число отменённых заказов.
-- Долю отменённых заказов в общем числе заказов (cancel rate).

SELECT hour::int, successful_orders, canceled_orders, ROUND(canceled_orders/all_orders::decimal,3) AS cancel_rate
FROM (SELECT DATE_PART('hour', creation_time) AS hour,
        COUNT(order_id) FILTER (WHERE order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')) AS successful_orders,
        COUNT(order_id) FILTER (WHERE order_id IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS canceled_orders,
        COUNT(order_id) AS all_orders
        FROM orders
        GROUP BY 1)    AS a


