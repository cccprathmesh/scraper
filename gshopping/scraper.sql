-- Active: 1781005187826@@2.24.198.101@3306@google_shopping
SET statement_timeout = '5min';

SELECT
    table_schema,
    column_name,
    data_type
FROM information_schema.columns
WHERE
    table_name = 'osb_products'
ORDER BY ordinal_position;

SELECT
    table_schema,
    column_name,
    data_type
FROM information_schema.columns
WHERE
    table_name = 'google_shopping_sellers'
ORDER BY ordinal_position;

SELECT
    table_schema,
    column_name,
    data_type
FROM information_schema.columns
WHERE
    table_name = 'google_shopping_results'
ORDER BY ordinal_position;

SELECT pg_terminate_backend (pid)
FROM pg_stat_activity
WHERE
    pid <> pg_backend_pid ()
    AND state = 'active';

SELECT
    pid,
    usename AS user,
    datname AS database,
    state,
    wait_event_type,
    wait_event,
    NOW() - query_start AS duration,
    query
FROM pg_stat_activity
WHERE
    state != 'idle'
ORDER BY query_start;

SELECT scraping_status, status, count(*) as total
FROM public.osb_products
GROUP BY
    scraping_status,
    status
ORDER BY total DESC
LIMIT 10000

SELECT
    scraping_status,
    status,
    claimed_by,
    count(*) as total
FROM public.osb_products
WHERE
    status = 1
GROUP BY
    scraping_status,
    status,
    claimed_by
ORDER BY total DESC
LIMIT 10000

UPDATE osb_products SET status = '2' WHERE status = 1;

SELECT *
FROM osb_products
WHERE
    status = 1
    AND scraping_status = 'claimed'
    AND product_id NOT IN(
        SELECT DISTINCT
            product_id
        FROM google_shopping_sellers
    );

SELECT * FROM osb_products WHERE product_id IN ('725134');
-- SELECT * FROM google_shopping_sellers WHERE product_id IN ('725134');
SELECT DISTINCT product_id FROM google_shopping_sellers;

SELECT MIN(updated_at), MAX(updated_at)
FROM osb_products
WHERE
    status = 1
    AND scraping_status = 'claimed'
GROUP BY
    status;

SELECT *
FROM google_shopping_results
WHERE
    status = 'selection_error'
    AND product_id IN (
        SELECT product_id
        FROM osb_products
        WHERE
            status = 1
    );
-- If not showing then reset google_seller_page_url and check with default url as we have prepared.

SELECT *
FROM google_shopping_results
WHERE
    status = 'product_not_clickable'
    AND product_id IN (
        SELECT product_id
        FROM osb_products
        WHERE
            status = 1
    );
-- reset google_seller_page_url
-- Remove 1stopbedrooms if still not found then remove color and part then check

SELECT *
FROM google_shopping_results
WHERE
    status = 'no_products'
    AND product_id IN (
        SELECT product_id
        FROM osb_products
        WHERE
            status = 1
    );
-- reset google_seller_page_url
-- Remove 1stopbedrooms if still not found then remove color and part then check
SELECT *
FROM google_shopping_results
WHERE
    status = 'captcha_failed'
    AND product_id IN (
        SELECT product_id
        FROM osb_products
        WHERE
            status = 1
    );
-- reset google_seller_page_url
-- Remove 1stopbedrooms if still not found then remove color and part then check

-- all should be in error

SELECT status, count(*) FROM google_shopping_results GROUP BY status;

UPDATE google_shopping_results
SET
    google_seller_page_url = null
WHERE
    status = 'selection_error';

UPDATE osb_products
SET
    scraping_status = 'pending',
    claimed_at = null,
    claimed_by = null
WHERE
    scraping_status = 'claimed';

WITH
    cte AS (
        SELECT product_id
        FROM osb_products
        WHERE
            scraping_status = 'completed'
            AND status = 1
        ORDER BY product_id
        LIMIT 5
    )
UPDATE osb_products
SET
    scraping_status = 'pending',
    claimed_at = NULL,
    claimed_by = NULL
WHERE
    product_id IN (
        SELECT product_id
        FROM cte
    );

UPDATE osb_products
SET
    scraping_status = 'pending',
    claimed_at = null,
    claimed_by = null
WHERE
    scraping_status IN ('pending', 'error', 'claimed');

UPDATE osb_products
SET
    scraping_status = 'pending',
    claimed_at = null,
    claimed_by = null
WHERE
    product_id IN (
        SELECT product_id
        FROM (
                SELECT product_id
                FROM google_shopping_results
                WHERE
                    product_id IN (
                        SELECT product_id
                        FROM osb_products
                        WHERE
                            status = 1
                            AND scraping_status = 'completed'
                    )
                    AND google_seller_page_url = ''
            ) AS A
    );

SELECT * FROM google_shopping_sellers;

UPDATE osb_products
SET
    status = 2
WHERE
    product_id IN (
        SELECT product_id
        FROM google_shopping_results
        WHERE
            osb_url_match = 'Yes'
    );

UPDATE osb_products
SET
    status = 1
WHERE
    product_id IN (
        SELECT product_id
        FROM google_shopping_results
        WHERE
            osb_url_match = 'No'
    )
    AND status = 2;

UPDATE osb_products
SET
    status = 1,
    scraping_status = 'pending'
WHERE
    product_id IN (1)

SHOW PROCESSLIST;

UPDATE osb_products
SET
    retry_count = 0,
    error_message = NULL
WHERE
    error_message IN (
        'captcha_failed',
        'unprocessed_due_to_shutdown',
        'not_processed'
    )
    AND scraping_status = 'pending';

SELECT scraping_status, priority, COUNT(*) as cnt
FROM osb_products
WHERE
    status = 1
GROUP BY
    priority,
    scraping_status
ORDER BY priority ASC

UPDATE osb_products
SET
    scraping_status = 'pending',
    retry_count = 0,
    claimed_at = NULL,
    claimed_by = NULL
WHERE
    scraping_status = 'error'
    AND status = 1;

SELECT error_message, count(*)
FROM osb_products
WHERE
    status = 1
    AND scraping_status = 'error'
GROUP BY
    error_message
ORDER BY count(*) DESC

SELECT *
FROM osb_products
WHERE
    status = 1
    AND scraping_status = 'completed'
ORDER BY last_attempt DESC

SHOW PROCESSLIST;

SELECT
    product_id,
    google_seller_page_url,
    google_seller_page_full_url
FROM google_shopping_results
WHERE
    google_seller_page_url LIKE 'https://share.google%';

SELECT
    product_id,
    google_seller_page_url,
    google_seller_page_full_url,
    osb_url_match,
    updated_at,
    other_attributes
FROM google_shopping_results
WHERE
    product_id IN (
        SELECT product_id
        FROM osb_products
        WHERE
            is_osb_url_imported = 0
            AND status = 1
            AND scraping_status = 'completed'
    )
    AND google_seller_page_url != ''
    AND osb_url_match = 'Yes'
ORDER BY updated_at DESC;

SELECT *
FROM osb_products
WHERE
    status = 1
    AND product_id = 10742;

-- Add is_osb_url_imported column to osb_products table
ALTER TABLE osb_products
ADD COLUMN is_osb_url_imported BOOLEAN DEFAULT FALSE;

UPDATE osb_products SET is_osb_url_imported = 1;

SELECT
    product_id,
    google_seller_page_url,
    google_seller_page_full_url
FROM google_shopping_results
WHERE
    google_seller_page_full_url LIKE "%share.google%";

UPDATE osb_products
SET
    is_osb_url_imported = 1
WHERE
    product_id IN (
        SELECT product_id
        FROM (
                SELECT product_id
                    -- ,
                    -- google_seller_page_full_url as google_shopping_url
                FROM google_shopping_results
                WHERE
                    google_seller_page_full_url != ''
                    AND osb_url_match = 'Yes'
                    AND product_id IN (
                        SELECT product_id
                        FROM osb_products
                        WHERE
                            is_osb_url_imported = 0
                    )
                ORDER BY updated_at DESC
            ) AS A
    );

SELECT A.product_id, B.osb_url, A.google_seller_page_full_url as google_shopping_url
FROM
    google_shopping_results as A
    INNER JOIN osb_products as B ON A.product_id = B.product_id
WHERE
    A.google_seller_page_full_url != ''
    AND A.osb_url_match = 'No'

SELECT scraping_status,
    -- retry_count,
    COUNT(*) as cnt
FROM osb_products
WHERE
    status = 1
    AND scraping_status != 'completed'
GROUP BY
    -- retry_count,
    scraping_status

SELECT
    product_id,
    google_seller_page_full_url as google_shopping_url
FROM google_shopping_results
WHERE
    COALESCE(
        google_seller_page_full_url,
        ''
    ) != ''
    AND osb_url_match = 'Yes'
    AND product_id IN (
        SELECT product_id
        FROM osb_products
        WHERE
            is_osb_url_imported = 0
    )
ORDER BY updated_at DESC

UPDATE osb_products
SET
    is_osb_url_imported = 1
WHERE
    product_id IN (
        SELECT product_id (
                SELECT product_id,
                FROM google_shopping_results
                WHERE
                    COALESCE(
                        google_seller_page_full_url, ''
                    ) != ''
                    AND osb_url_match = 'Yes'
                    AND product_id IN (
                        SELECT product_id
                        FROM osb_products
                        WHERE
                            is_osb_url_imported = 0
                    )
            ) as A
    );
-- ORDER BY updated_at DESC

UPDATE osb_products
SET
    priority = 1
WHERE
    product_id IN (
        SELECT product_id
        FROM (
                SELECT product_id
                FROM osb_products
                WHERE
                    status = 1
                    AND scraping_status != 'completed'
            ) AS A
    );

UPDATE osb_products
SET
    status = 0,
    scraping_status = 'pending',
    priority = 0,
    retry_count = 0,
    claimed_at = NULL,
    claimed_by = NULL;