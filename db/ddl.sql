-- 데이터베이스 스키마 정의
-- user_category 테이블 스크립트에서 삭제해야 함

-- DROP TABLE IF EXISTS 
--     current_price_by_platform, 
--     game_category, 
--     game_dynamic, 
--     game_static, 
--     user_category,
--     platform, 
--     category 
-- CASCADE;

-- 1. category
DROP TABLE IF EXISTS category;
CREATE TABLE category (
    id INT PRIMARY KEY,
    category_name VARCHAR NOT NULL
);

-- 2. platform
DROP TABLE IF EXISTS platform;
CREATE TABLE platform (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL
);

-- 3. game_static
DROP TABLE IF EXISTS game_static;
CREATE TABLE game_static (
    id INT PRIMARY KEY,
    title VARCHAR NOT NULL,
    original_title VARCHAR,
    description TEXT,
    release_date VARCHAR,
    publisher VARCHAR,
    developer VARCHAR,
    thumbnail VARCHAR,
    price INT,
    is_singleplay BOOLEAN,
    is_multiplay BOOLEAN
);

-- 4. game_dynamic
DROP TABLE IF EXISTS game_dynamic;
CREATE TABLE game_dynamic (
    game_id INT PRIMARY KEY,
    rating INT,
    active_players INT,
    lowest_platform INT,
    lowest_price INT,
    history_lowest_price INT,
    on_sale BOOLEAN,
    total_reviews INT,
    updated_at TIMESTAMP DEFAULT now(),  -- 기본값 지정
    FOREIGN KEY (game_id) REFERENCES game_static(id),
    FOREIGN KEY (lowest_platform) REFERENCES platform(id)
);

-- 5. game_category
DROP TABLE IF EXISTS game_category;
CREATE TABLE game_category (
    category_id INT,
    game_id INT,
    PRIMARY KEY (category_id, game_id),
    FOREIGN KEY (category_id) REFERENCES category(id),
    FOREIGN KEY (game_id) REFERENCES game_static(id)
);

-- 6. current_price_by_platform
DROP TABLE IF EXISTS current_price_by_platform;
CREATE TABLE current_price_by_platform (
    id SERIAL PRIMARY KEY,
    game_id INT,
    platform_id INT,
    discount_rate INT,
    discount_price INT,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    deleted_at TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES game_static(id),
    FOREIGN KEY (platform_id) REFERENCES platform(id)
);

-- 7. user_category
DROP TABLE IF EXISTS user_category;
CREATE TABLE user_category (
    user_id INT,
    category_id INT,
    PRIMARY KEY (user_id, category_id),
    FOREIGN KEY (category_id) REFERENCES category(id)
);

-- 💡 트리거 함수: updated_at 자동 반영
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 트리거 연결: game_dynamic
CREATE TRIGGER update_game_dynamic_timestamp
BEFORE UPDATE ON game_dynamic
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- 트리거 연결: current_price_by_platform
CREATE TRIGGER update_current_price_timestamp
BEFORE UPDATE ON current_price_by_platform
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();