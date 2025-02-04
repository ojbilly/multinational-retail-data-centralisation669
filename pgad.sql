-- 1️⃣ Find the countries we operate in and the one with the most physical stores
SELECT country_code AS country,
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;
-- 2️⃣ Find the locations with the most stores
SELECT locality,
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;
-- 3️⃣ Find the months with the most sales
SELECT SUM(sales_amount) AS total_sales,
    EXTRACT(
        MONTH
        FROM sales_date
    ) AS month
FROM sales_table
GROUP BY month
ORDER BY total_sales DESC;
-- 4️⃣ Compare online vs offline sales
SELECT COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE
        WHEN location = 'Web' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM sales_table
GROUP BY location;
-- 5️⃣ Determine revenue per store type
SELECT store_type,
    SUM(sales_amount) AS total_sales,
    (
        SUM(sales_amount) * 100.0 / (
            SELECT SUM(sales_amount)
            FROM sales_table
        )
    ) AS sales_made_percentage
FROM sales_table
    JOIN dim_store_details ON sales_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER BY total_sales DESC;
-- 6️⃣ Find the best-performing months in each year
SELECT SUM(sales_amount) AS total_sales,
    EXTRACT(
        YEAR
        FROM sales_date
    ) AS year,
    EXTRACT(
        MONTH
        FROM sales_date
    ) AS month
FROM sales_table
GROUP BY year,
    month
ORDER BY total_sales DESC;
-- 7️⃣ Find total staff numbers per country
SELECT country_code,
    SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;
-- 8️⃣ Find the most revenue-generating store types in Germany
SELECT SUM(sales_amount) AS total_sales,
    store_type,
    country_code
FROM sales_table
    JOIN dim_store_details ON sales_table.store_code = dim_store_details.store_code
WHERE country_code = 'DE'
GROUP BY store_type,
    country_code
ORDER BY total_sales DESC;
-- 9️⃣ Calculate average time between sales per year
SELECT EXTRACT(
        YEAR
        FROM sales_date
    ) AS year,
    AVG(
        LEAD(sales_date) OVER (
            PARTITION BY EXTRACT(
                YEAR
                FROM sales_date
            )
            ORDER BY sales_date
        ) - sales_date
    ) AS actual_time_taken
FROM sales_table
GROUP BY year
ORDER BY year;