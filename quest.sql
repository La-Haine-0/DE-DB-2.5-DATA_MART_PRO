-- Запрос для получения витрины с новыми атрибутами
WITH sales_total AS (
    SELECT product_id, sales_date, sales_cnt, (SELECT shop_id FROM public.shop WHERE shop_name = 'DNS') AS shop_id
    FROM public.shop_dns
    UNION ALL
    SELECT product_id, sales_date, sales_cnt, (SELECT shop_id FROM public.shop WHERE shop_name = 'М.Видео') AS shop_id
    FROM public.shop_mvideo
    UNION ALL
    SELECT product_id, sales_date, sales_cnt, (SELECT shop_id FROM public.shop WHERE shop_name = 'Ситилинк') AS shop_id
    FROM public.shop_citilink
),
tmp_sales_fact AS (
    SELECT
        s.shop_id,
        p.product_id,
        SUM(st.sales_cnt) AS sales_fact
    FROM
        public.shop s
        CROSS JOIN public.products p
        LEFT JOIN sales_total st ON s.shop_id = st.shop_id AND p.product_id = st.product_id
    GROUP BY
        s.shop_id, p.product_id
),
tmp_sales_plan AS (
    SELECT
        shop_id,
        product_id,
        SUM(plan_cnt) AS sales_plan
    FROM
        public.plan
    GROUP BY
        shop_id, product_id
),
tmp_income_fact AS (
    SELECT
        s.shop_id,
        p.product_id,
        SUM(st.sales_cnt * p.price) AS income_fact
    FROM
        public.shop s
        CROSS JOIN public.products p
        LEFT JOIN sales_total st ON s.shop_id = st.shop_id AND p.product_id = st.product_id
    GROUP BY
        s.shop_id, p.product_id
),
tmp_income_plan AS (
    SELECT
        sp.shop_id,
        sp.product_id,
        SUM(sp.plan_cnt * p.price) AS income_plan
    FROM
        public.plan sp
        JOIN public.products p ON sp.product_id = p.product_id
    GROUP BY
        sp.shop_id, sp.product_id
),
tmp_promo_stats AS (
    SELECT
        s.shop_id,
        p.product_id,
        COUNT(DISTINCT pr.promo_date) AS promo_len,
        SUM(CASE WHEN pr.promo_date IS NOT NULL THEN COALESCE(st.sales_cnt, 0) ELSE 0 END) AS promo_sales_cnt,
        SUM(CASE WHEN pr.promo_date IS NOT NULL THEN COALESCE(st.sales_cnt, 0) * COALESCE(pr.discount, 0) ELSE 0 END) AS promo_income
    FROM
        public.shop s
        CROSS JOIN public.products p
        LEFT JOIN sales_total st ON s.shop_id = st.shop_id AND p.product_id = st.product_id
        LEFT JOIN public.promo pr ON s.shop_id = pr.shop_id AND p.product_id = pr.product_id
    GROUP BY
        s.shop_id, p.product_id
),
tmp_sales_stats AS (
    SELECT
        s.shop_id,
        p.product_id,
        COUNT(DISTINCT st.sales_date) AS days_with_sales,
        MAX(st.sales_cnt) AS max_sales,
        MAX(CASE WHEN st.sales_cnt = st.max_sales THEN st.sales_date END) AS date_max_sales,
        MAX(CASE WHEN st.sales_cnt = st.max_sales AND st.promo_date IS NOT NULL THEN true ELSE false END) AS date_max_sales_is_promo
    FROM
        public.shop s
        CROSS JOIN public.products p
        LEFT JOIN (
            SELECT
                st.shop_id,
                st.product_id,
                st.sales_date,
                st.sales_cnt,
                pr.promo_date
            FROM sales_total st
            LEFT JOIN public.promo pr ON st.shop_id = pr.shop_id AND st.product_id = pr.product_id AND st.sales_date = pr.promo_date
        ) st ON s.shop_id = st.shop_id AND p.product_id = st.product_id
    GROUP BY
        s.shop_id, p.product_id
)
SELECT
    s.shop_name,
    p.product_name,
    COALESCE(sf.sales_fact, 0) AS sales_fact,
    COALESCE(sp.sales_plan, 0) AS sales_plan,
    CASE
        WHEN COALESCE(sp.sales_plan, 0) > 0 THEN COALESCE(sf.sales_fact, 0) / COALESCE(sp.sales_plan, 0)
        ELSE 0
    END AS sales_fact_to_plan,
    COALESCE(if.income_fact, 0) AS income_fact,
    COALESCE(ip.income_plan, 0) AS income_plan,
    CASE
        WHEN COALESCE(ip.income_plan, 0) > 0 THEN COALESCE(if.income_fact, 0) / COALESCE(ip.income_plan, 0)
        ELSE 0
    END AS income_fact_to_plan,
    COALESCE(sf.sales_fact / tmp_sales_stats.days_with_sales, 0) AS avg_sales_per_day,
    COALESCE(tmp_sales_stats.max_sales, 0) AS max_sales,
    COALESCE(tmp_sales_stats.date_max_sales, NULL) AS date_max_sales,
    COALESCE(tmp_sales_stats.date_max_sales_is_promo, false) AS date_max_sales_is_promo,
    CASE
        WHEN COALESCE(tmp_sales_stats.max_sales, 0) > 0 THEN COALESCE(sf.sales_fact / tmp_sales_stats.days_with_sales, 0) / COALESCE(tmp_sales_stats.max_sales, 0)
        ELSE 0
    END AS avg_sales_to_max_sales_ratio,
    COALESCE(tmp_promo_stats.promo_len, 0) AS promo_len,
    COALESCE(tmp_promo_stats.promo_sales_cnt, 0) AS promo_sales_cnt,
    CASE
        WHEN COALESCE(sf.sales_fact, 0) > 0 THEN COALESCE(tmp_promo_stats.promo_sales_cnt, 0) / COALESCE(sf.sales_fact, 0)
        ELSE 0
    END AS promo_sales_to_fact_sales_ratio,
    COALESCE(tmp_promo_stats.promo_income, 0) AS promo_income,
    CASE
        WHEN COALESCE(if.income_fact, 0) > 0 THEN COALESCE(tmp_promo_stats.promo_income, 0) / COALESCE(if.income_fact, 0)
        ELSE 0
    END AS promo_income_to_fact_income_ratio
FROM
    public.shop s
    CROSS JOIN public.products p
    LEFT JOIN tmp_sales_fact sf ON s.shop_id = sf.shop_id AND p.product_id = sf.product_id
    LEFT JOIN tmp_sales_plan sp ON s.shop_id = sp.shop_id AND p.product_id = sp.product_id
    LEFT JOIN tmp_income_fact if ON s.shop_id = if.shop_id AND p.product_id = if.product_id
    LEFT JOIN tmp_income_plan ip ON s.shop_id = ip.shop_id AND p.product_id = ip.product_id
    LEFT JOIN tmp_sales_stats ON s.shop_id = tmp_sales_stats.shop_id AND p.product_id = tmp_sales_stats.product_id
    LEFT JOIN tmp_promo_stats ON s.shop_id = tmp_promo_stats.shop_id AND p.product_id = tmp_promo_stats.product_id;