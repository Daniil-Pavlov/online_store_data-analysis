-- 2.1.1
SELECT date, revenue, SUM(revenue) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_revenue,
ROUND(revenue*100/(LAG(revenue) OVER (ORDER BY date))-100,2) AS revenue_change 

FROM (  SELECT creation_time::date AS date, SUM(price) AS revenue
        FROM (  SELECT creation_time, order_id, unnest(product_ids) AS product_id
                FROM orders
                WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS a
        LEFT JOIN products
        USING (product_id)
        GROUP BY creation_time::date) AS b
  
-- 2.1.2
SELECT time::DATE AS date,

ROUND(SUM(sm_ord) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) / COUNT(DISTINCT(user_id)),2)  AS arpu,
ROUND(SUM(sm_ord) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) / COUNT(DISTINCT(user_id)) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')),2) AS arppu,
ROUND(SUM(sm_ord) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) / COUNT(order_id) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')),2)AS aov
FROM user_actions
LEFT JOIN ( SELECT order_id,sum(price) AS sm_ord
            FROM (SELECT order_id,creation_time,unnest(product_ids) AS product_id FROM orders) AS ord
            LEFT JOIN products
            USING (product_id)
            GROUP BY order_id) AS sm
USING(order_id)
GROUP BY time::DATE
ORDER BY time::DATE

-- 2.1.3
WITH old AS (   SELECT time::DATE AS date, 
                SUM(sm_ord) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS sum_day,
                COUNT(user_id) FILTER (WHERE time = fst_dlv) AS pay_cl,
                COUNT(user_id) FILTER (WHERE time = fst_ord) AS ord_cl,
                COUNT(order_id) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS ord
                FROM (  SELECT time,user_id,order_id, MIN(time) OVER (PARTITION BY user_id ORDER BY time) AS fst_ord
                        FROM user_actions) AS fo 
                LEFT JOIN ( SELECT order_id, MIN(time) OVER (PARTITION BY user_id ORDER BY time) AS fst_dlv
                            FROM user_actions 
                            WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS fd
                USING(order_id)
                LEFT JOIN ( SELECT order_id,sum(price) AS sm_ord
                            FROM (SELECT order_id,creation_time,unnest(product_ids) AS product_id FROM orders) AS ord
                            LEFT JOIN products
                            USING (product_id)
                            GROUP BY order_id) AS sm
                USING(order_id)
                GROUP BY time::DATE
                ORDER BY time::DATE  )

SELECT date, 
ROUND(SUM(sum_day) OVER w /SUM(ord_cl) OVER w,2) AS running_arpu,
ROUND(SUM(sum_day) OVER w /SUM(pay_cl) OVER w,2) AS running_arppu, 
ROUND(SUM(sum_day) OVER w /SUM(ord) OVER w,2) AS running_aov 
FROM old
WINDOW w AS (ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  )

-- 2.1.4
WITH sm_ord AS (SELECT order_id,SUM(price) AS s_o
                FROM (SELECT creation_time,order_id,unnest(product_ids) AS product_id FROM orders) AS a
                LEFT JOIN products
                USING (product_id)
                GROUP BY order_id),
          x AS (SELECT to_char(time::date, 'Day')  AS weekday,
                DATE_PART('isodow', time::date) AS weekday_number,
                
                COUNT(DISTINCT(user_id)) AS all_us,
                COUNT(DISTINCT(user_id)) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS pay_us,
                COUNT(DISTINCT(order_id)) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS ord,
                SUM(s_o) FILTER (WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS sum_orders
                FROM user_actions
                
                LEFT JOIN sm_ord
                USING (order_id) 
                WHERE time::DATE >='2022-08-26' AND time::DATE <='2022-09-08' 
                GROUP BY 1,2
                ORDER BY 2)


SELECT weekday, weekday_number,
ROUND(sum_orders::decimal/all_us,2) AS arpu,
ROUND(sum_orders::decimal/pay_us,2) AS arppu,
ROUND(sum_orders::decimal/ord,2) AS  aov
FROM x

-- 2.1.5
WITH sum_products AS   (SELECT order_id,SUM (price) AS price_order
                        FROM (  SELECT order_id,unnest(product_ids) AS product_id
                                FROM orders
                                WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS pr_ord
                        LEFT JOIN products 
                        USING(product_id)
                        GROUP BY order_id)
SELECT  time::DATE AS date,
        SUM (price_order) AS revenue,
        SUM (price_order) FILTER (WHERE time::date = first_action::DATE) AS new_users_revenue,
        ROUND(SUM (price_order) FILTER (WHERE time::date = first_action::DATE)*100/SUM (price_order),2) AS new_users_revenue_share,
        ROUND((SUM (price_order)-SUM (price_order) FILTER (WHERE time::date = first_action::DATE))*100/SUM (price_order),2) AS old_users_revenue_share
FROM (  SELECT time ,user_id,order_id,price_order,action, MIN(time) OVER (PARTITION BY user_id ORDER BY time) AS first_action
        FROM user_actions
        LEFT JOIN sum_products
        USING(order_id)) AS n
GROUP BY time::DATE
ORDER BY time::DATE

-- 2.1.6
WITH sm_pr AS   (SELECT name AS product_name,SUM(price) AS revenue, ROUND(SUM(price)*100/SUM(SUM(price)) OVER (),2) AS share_in_revenue
                FROM (  SELECT order_id, unnest(product_ids) AS product_id
                        FROM orders
                        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')) AS price_products
                LEFT JOIN products
                USING(product_id)
                GROUP BY 1
                ORDER BY 2 DESC)

SELECT (CASE WHEN share_in_revenue < 0.5 THEN 'ДРУГОЕ' ELSE product_name END) AS product_name,SUM (revenue) AS revenue,SUM(share_in_revenue) AS share_in_revenue--product_name, revenue,ROUND(revenue*100/all_sum,2) AS share_in_revenue
FROM sm_pr
GROUP BY 1
ORDER BY 2 DESC

-- 2.1.7
WITH rev_tax AS (SELECT creation_time::DATE as date,SUM(price) AS revenue,
                SUM(CASE WHEN name IN ('сахар', 'сухарики', 'сушки', 'семечки', 
                'масло льняное', 'виноград', 'масло оливковое', 
                'арбуз', 'батон', 'йогурт', 'сливки', 'гречка', 
                'овсянка', 'макароны', 'баранина', 'апельсины', 
                'бублики', 'хлеб', 'горох', 'сметана', 'рыба копченая', 
                'мука', 'шпроты', 'сосиски', 'свинина', 'рис', 
                'масло кунжутное', 'сгущенка', 'ананас', 'говядина', 
                'соль', 'рыба вяленая', 'масло подсолнечное', 'яблоки', 
                'груши', 'лепешка', 'молоко', 'курица', 'лаваш', 'вафли', 'мандарины') THEN ROUND(price*10/110,2) ELSE ROUND(price*20/120,2) END) as tax
                FROM   (SELECT creation_time,order_id, unnest(product_ids) as product_id
                       FROM   orders
                       WHERE  order_id not in (SELECT order_id FROM   user_actions WHERE  action = 'cancel_order')) as price_products
                LEFT JOIN products 
                using(product_id)
                GROUP BY 1
                ORDER BY 1  ),
     cos AS (SELECT date,
            CASE WHEN DATE_PART('month', date) = 8 THEN sum_ord_ac*140+sum_ord_del*150+bonus_cnt*400+120000 ELSE sum_ord_del*150+sum_ord_ac*115+bonus_cnt*500+150000 END AS costs
            
            FROM (  SELECT date,
                    SUM(ord_del)  AS sum_ord_del,
                    SUM(ord_ac) AS sum_ord_ac,
                    SUM(bonus_cnt) AS bonus_cnt
                    FROM (  SELECT time::DATE AS date,courier_id, 
                            COUNT(order_id) FILTER (WHERE action = 'deliver_order') AS ord_del,
                            COUNT(order_id) FILTER (WHERE action = 'accept_order')AS ord_ac,
                            CASE WHEN COUNT(order_id) FILTER (WHERE action = 'deliver_order')> 4 THEN 1 ELSE 0 END  AS bonus_cnt
                            FROM courier_actions
                            WHERE order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order' )
                            GROUP BY 1,2) AS a
                    GROUP BY 1
                    ORDER BY 1) AS b)     

SELECT date, revenue, costs, tax,
revenue - costs - tax AS gross_profit, 
SUM(revenue) OVER w AS total_revenue,
SUM(costs) OVER w AS total_costs,
SUM(tax) OVER w AS total_tax,
SUM(revenue) OVER w - SUM(costs) OVER w - SUM(tax) OVER w AS total_gross_profit,
ROUND((revenue - costs - tax)*100/revenue,2) AS gross_profit_ratio,
ROUND ((SUM(revenue) OVER w - SUM(costs) OVER w - SUM(tax) OVER w )*100/SUM(revenue) OVER w,2) AS total_gross_profit_ratio
FROM rev_tax
JOIN cos
USING (date)
WINDOW w AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)









