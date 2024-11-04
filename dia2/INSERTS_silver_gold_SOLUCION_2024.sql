-- INSERT QUERIES

-- SILVER

-- ORDERS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.ORDERS 
SELECT 
    ORDER_ID::varchar(50),
    SHIPPING_SERVICE::varchar(20),
    SHIPPING_COST::float,
    ADDRESS_ID::varchar(50),
    CREATED_AT::timestamp_ntz,
    IFNULL(promo_id,'N/A'),
    ESTIMATED_DELIVERY_AT::timestamp_ntz,
    ORDER_COST::float,
    USER_ID::varchar(50),
    ORDER_TOTAL::float,
    DELIVERED_AT::timestamp_ntz,
    TRACKING_ID::varchar(50),
    STATUS::varchar(20),
    ROUND(shipping_cost::float/order_total::float,2),
    TIMESTAMPDIFF(HOUR,created_at,delivered_at)
FROM curso.orders;

-- EVENTS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.EVENTS 
SELECT 
    EVENT_ID::varchar(50),
    PAGE_URL::varchar(200),
    EVENT_TYPE::varchar(50),
    USER_ID::varchar(50),
    PRODUCT_ID::varchar(50),
    SESSION_ID::varchar(50),
    CREATED_AT::timestamp_ntz,
    ORDER_ID::varchar(50),
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at ASC)
FROM curso.events;

-- PRODUCTS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.PRODUCTS 
SELECT 
    PRODUCT_ID::Varchar(50),
    PRICE::float,
    NAME::varchar(100),
    INVENTORY::number(38,0)
FROM curso.products;

-- ADDRESSES
INSERT INTO MY_DB.MY_SILVER_SCHEMA.ADDRESSES 
SELECT 
    ADDRESS_ID::varchar(50),
    ZIPCODE::number(38,0),
    COUNTRY::varchar(50),
    ADDRESS::varchar(150),
    STATE::varchar(50)
FROM curso.addresses;

-- USERS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.USERS 
SELECT 
    USER_ID::varchar(50),
    UPDATED_AT::timestamp_ntz,
    ADDRESS_ID::varchar(50),
    LAST_NAME::varchar(50),
    CREATED_AT::timestamp_ntz,
    PHONE_NUMBER::varchar(20),
    FIRST_NAME::varchar(50),
    EMAIL::varchar(100)
FROM curso.users;

-- ORDER_ITEMS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.ORDER_ITEMS 
SELECT 
    ORDER_ID::varchar(50),
    PRODUCT_ID::varchar(50),
    QUANTITY::number(38,0)
FROM curso.order_items;

-- PROMOS
INSERT INTO MY_DB.MY_SILVER_SCHEMA.PROMOS 
SELECT 
    PROMO_ID::varchar(50),
    DISCOUNT::float,
    STATUS::varchar(50)
FROM curso.promos;

-- GOLD

-- SESSION_DETAILS
INSERT INTO MY_DB.MY_GOLD_SCHEMA.session_details 
SELECT 
    session_id,
    CASE WHEN MAX(order_id) IS NULL THEN FALSE ELSE TRUE END,
    MAX(hit_number),
    TIMESTAMPDIFF(MINUTE,MIN(created_at),MAX(created_at))
FROM MY_DB.MY_SILVER_SCHEMA.events 
GROUP BY session_id;

-- GENERAL_STATE_ANALYSIS
INSERT INTO MY_DB.MY_GOLD_SCHEMA.GENERAL_STATE_ANALYSIS 
SELECT 
    a.state,
    SUM(o.order_cost),
    COUNT(*),
    COUNT(DISTINCT o.user_id),
    ROUND(SUM(o.shipping_cost)/SUM(o.order_total),2),
    mode() WITHIN GROUP (ORDER BY o.shipping_service)
FROM MY_DB.MY_SILVER_SCHEMA.ORDERS o 
LEFT JOIN MY_DB.MY_SILVER_SCHEMA.ADDRESSES a ON o.address_id = a.address_id 
GROUP BY a.state;