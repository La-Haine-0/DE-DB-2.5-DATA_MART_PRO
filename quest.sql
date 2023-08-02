WITH
sales_total AS (
	SELECT product_id, sales_date, sales_cnt, (SELECT shop_id FROM shop WHERE shop_name = 'DNS')
	FROM shop_dns
	UNION SELECT product_id, sales_date, sales_cnt, (SELECT shop_id FROM shop WHERE shop_name = 'М.Видео')
		  FROM shop_mvideo
	UNION SELECT product_id, sales_date, sales_cnt, (SELECT shop_id FROM shop WHERE shop_name = 'Ситилинк')
		  FROM shop_citilink
),
sales_total_with_disc AS (
	SELECT st.shop_id, st.product_id, st.sales_cnt, st. sales_date, p.discount, pr.price,
		(date_trunc('month', sales_date::date) + interval '1 month' - interval '1 day')::date AS last_date_of_month,
		CASE WHEN p.discount IS NULL THEN False ELSE True END AS discount_exists,
		CASE WHEN st.sales_cnt * pr.price * (1.00 - p.discount) IS NULL THEN 0
			 ELSE st.sales_cnt * pr.price * (1.00 - p.discount)
		END AS promo_income
	FROM sales_total st
	LEFT JOIN promo p ON st.shop_id = p.shop_id AND st.product_id = p.product_id AND st.sales_date = p.promo_date
	JOIN products pr ON st.product_id = pr.product_id
),
sales_agg AS (
	SELECT shop_id, product_id, sales_cnt, sales_date, discount, last_date_of_month, discount_exists,
		SUM(promo_income) OVER w2 AS promo_income,
		SUM(sales_cnt) OVER w2 AS sum_sales,
		ROUND(AVG(sales_cnt) OVER w2, 1) AS "avg(sales/date)",
		MAX(sales_cnt) OVER w1 AS max_sales,
		DENSE_RANK() OVER w1 AS date_rank_max_sales,
		COUNT(discount) FILTER(WHERE discount IS NOT NULL) OVER w2 AS promo_len,
		CASE 
			WHEN (SUM(sales_cnt) FILTER(WHERE discount IS NOT NULL) OVER w2) IS NULL THEN 0
			ELSE (SUM(sales_cnt) FILTER(WHERE discount IS NOT NULL) OVER w2)
		END AS promo_sales_cnt
	FROM sales_total_with_disc
	WINDOW w1 AS (PARTITION BY shop_id, product_id, last_date_of_month ORDER BY sales_cnt DESC),
		w2 AS (PARTITION BY shop_id, product_id, last_date_of_month)
	ORDER BY shop_id, product_id, last_date_of_month
),
sales_result AS (
	SELECT s.shop_id, s.product_id, s.last_date_of_month, sum_sales,
		s."avg(sales/date)", s.max_sales, s.promo_len , s.promo_sales_cnt,
		sp.date_max_sales, sp.discount_exists, s.promo_income
	FROM sales_agg s
	LEFT JOIN (
		SELECT shop_id, product_id, last_date_of_month, sales_date AS date_max_sales, discount_exists
		FROM sales_agg
		WHERE date_rank_max_sales = 1
	) sp ON s.shop_id = sp.shop_id AND s.product_id = sp.product_id AND s.last_date_of_month = sp.last_date_of_month
	GROUP BY s.shop_id, s.product_id, s.last_date_of_month, s.sum_sales, 
		s."avg(sales/date)", s.max_sales, s.promo_len , s.promo_sales_cnt,
		sp.date_max_sales, sp.discount_exists, s.promo_income
	ORDER BY s.shop_id, s.product_id, s.last_date_of_month
)

SELECT sh.shop_name,
	   pr.product_name,
	   p.plan_date,
	   s.sum_sales AS sales_fact,
	   p.plan_cnt AS sales_plan,
	   ROUND((s.sum_sales::decimal / p.plan_cnt::decimal), 2) AS "sales_fact/sales_plan",
	   s.sum_sales * pr.price AS income_fact,
	   p.plan_cnt * pr.price AS income_plan,
	   ROUND(((s.sum_sales * pr.price) / (p.plan_cnt * pr.price)), 2) AS "income_fact/income_plan",
	   s."avg(sales/date)",
	   s.max_sales,
	   s.date_max_sales,
	   s.discount_exists AS date_max_sales_is_promo,
	   ROUND((s."avg(sales/date)"::decimal / s.max_sales::decimal), 2) AS "avg(sales/date) / max_sales",
	   s.promo_len,
	   s.promo_sales_cnt,
	   ROUND((s.promo_sales_cnt::decimal / s.sum_sales::decimal), 2) AS "promo_sales_cnt/fact_sales",
	   s.promo_income,
	   ROUND(s.promo_income / (s.sum_sales * pr.price), 2) AS "promo_income/fact_income"
FROM plan AS p
JOIN shop AS sh ON p.shop_id = sh.shop_id
JOIN sales_result AS s ON p.shop_id = s.shop_id AND p.product_id = s.product_id AND p.plan_date = s.last_date_of_month
JOIN products AS pr ON p.product_id = pr.product_id;