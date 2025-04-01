-- 데이터베이스 스키마 정의

-- 1. category
-- DROP TABLE IF EXISTS category CASCADE;
CREATE TABLE category (
    id BIGINT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

COMMENT ON COLUMN category.id IS '카테고리 고유 식별자';
COMMENT ON COLUMN category.category_name IS '카테고리 이름';

-- 2. platform
-- DROP TABLE IF EXISTS platform CASCADE;
CREATE TABLE platform (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL
);

COMMENT ON COLUMN platform.id IS '플랫폼 고유 식별자';
COMMENT ON COLUMN platform.name IS '플랫폼 이름';

-- 3. game_static
-- DROP TABLE IF EXISTS game_static CASCADE;
CREATE TABLE game_static (
    id BIGINT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    original_title VARCHAR(255),
    description TEXT,
    release_date VARCHAR(255),
    publisher VARCHAR(255),
    developer VARCHAR(255),
    thumbnail VARCHAR(255),
    price INT,
    is_singleplay BOOLEAN,
    is_multiplay BOOLEAN
);

COMMENT ON COLUMN game_static.id IS '게임 고유 식별자';
COMMENT ON COLUMN game_static.title IS '게임 제목';
COMMENT ON COLUMN game_static.original_title IS '게임 원제목';
COMMENT ON COLUMN game_static.description IS '게임 설명';
COMMENT ON COLUMN game_static.release_date IS '출시일';
COMMENT ON COLUMN game_static.publisher IS '배급사';
COMMENT ON COLUMN game_static.developer IS '개발사';
COMMENT ON COLUMN game_static.thumbnail IS '썸네일 URL';
COMMENT ON COLUMN game_static.price IS '가격';
COMMENT ON COLUMN game_static.is_singleplay IS '싱글플레이 여부';
COMMENT ON COLUMN game_static.is_multiplay IS '멀티플레이 여부';

-- 4. game_dynamic
-- DROP TABLE IF EXISTS game_dynamic CASCADE;
CREATE TABLE game_dynamic (
    game_id BIGINT PRIMARY KEY REFERENCES game_static(id),
    rating INT,
    active_players INT,
    lowest_platform INT REFERENCES platform(id),
    lowest_price INT,
    history_lowest_price INT,
    on_sale BOOLEAN,
    total_reviews BIGINT,
    updated_at TIMESTAMP DEFAULT now()
);

COMMENT ON COLUMN game_dynamic.game_id IS '게임 고유 식별자 (game_static 참조)';
COMMENT ON COLUMN game_dynamic.rating IS '게임 평점';
COMMENT ON COLUMN game_dynamic.active_players IS '활성 플레이어 수';
COMMENT ON COLUMN game_dynamic.lowest_platform IS '최저가 플랫폼 ID';
COMMENT ON COLUMN game_dynamic.lowest_price IS '최저가';
COMMENT ON COLUMN game_dynamic.history_lowest_price IS '역대 최저가';
COMMENT ON COLUMN game_dynamic.on_sale IS '할인 여부';
COMMENT ON COLUMN game_dynamic.total_reviews IS '총 리뷰 수';
COMMENT ON COLUMN game_dynamic.updated_at IS '업데이트된 날짜';

-- 트리거: game_dynamic
CREATE TRIGGER update_game_dynamic_timestamp
BEFORE UPDATE ON game_dynamic
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- 5. game_category
-- DROP TABLE IF EXISTS game_category CASCADE;
CREATE TABLE game_category (
    category_id BIGINT REFERENCES category(id),
    game_id BIGINT REFERENCES game_static(id),
    PRIMARY KEY (category_id, game_id)
);

COMMENT ON COLUMN game_category.category_id IS '카테고리 ID (category 참조)';
COMMENT ON COLUMN game_category.game_id IS '게임 ID (game_static 참조)';

-- 6. current_price_by_platform
-- DROP TABLE IF EXISTS current_price_by_platform CASCADE;
CREATE TABLE current_price_by_platform (
    id SERIAL PRIMARY KEY,
    game_id INT REFERENCES game_static(id),
    platform_id INT REFERENCES platform(id),
    discount_rate INT,
    discount_price INT,
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    updated_at TIMESTAMP DEFAULT now() NOT NULL,
    deleted_at TIMESTAMP
);

COMMENT ON COLUMN current_price_by_platform.id IS '현재 가격 고유 식별자';
COMMENT ON COLUMN current_price_by_platform.game_id IS '게임 ID (game_static 참조)';
COMMENT ON COLUMN current_price_by_platform.platform_id IS '플랫폼 ID (platform 참조)';
COMMENT ON COLUMN current_price_by_platform.discount_rate IS '할인율';
COMMENT ON COLUMN current_price_by_platform.discount_price IS '할인가';
COMMENT ON COLUMN current_price_by_platform.created_at IS '생성된 날짜';
COMMENT ON COLUMN current_price_by_platform.updated_at IS '수정된 날짜';
COMMENT ON COLUMN current_price_by_platform.deleted_at IS '삭제된 날짜';

-- 트리거: current_price_by_platform
CREATE TRIGGER update_current_price_timestamp
BEFORE UPDATE ON current_price_by_platform
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

-- 7. users
-- DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    kakao_id VARCHAR(255),
    nickname VARCHAR(12),
    discord_link VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN users.id IS '사용자 고유 식별자';
COMMENT ON COLUMN users.kakao_id IS '카카오 ID';
COMMENT ON COLUMN users.nickname IS '사용자 닉네임';
COMMENT ON COLUMN users.discord_link IS '디스코드 링크';
COMMENT ON COLUMN users.created_at IS '사용자 생성 날짜';
COMMENT ON COLUMN users.deleted_at IS '사용자 삭제 날짜';
COMMENT ON COLUMN users.updated_at IS '사용자 수정 날짜';

-- 8. user_category
-- DROP TABLE IF EXISTS user_category CASCADE;
CREATE TABLE user_category (
    user_id BIGINT NOT NULL REFERENCES users(id),
    category_id BIGINT NOT NULL REFERENCES category(id),
    PRIMARY KEY (user_id, category_id)
);

COMMENT ON COLUMN user_category.user_id IS '사용자 ID (users 참조)';
COMMENT ON COLUMN user_category.category_id IS '카테고리 ID (category 참조)';

-- 9. comment
-- DROP TABLE IF EXISTS comment CASCADE;
CREATE TABLE comment (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    game_id BIGINT NOT NULL REFERENCES game_static(id),
    content VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    updated_at TIMESTAMP DEFAULT now() NOT NULL,
    deleted_at TIMESTAMP
);

COMMENT ON COLUMN comment.id IS '댓글의 고유 식별자';
COMMENT ON COLUMN comment.user_id IS '댓글 작성자의 ID';
COMMENT ON COLUMN comment.game_id IS '댓글이 달린 게임의 ID';
COMMENT ON COLUMN comment.content IS '댓글 내용 (100자 제한)';
COMMENT ON COLUMN comment.created_at IS '댓글이 생성된 날짜';
COMMENT ON COLUMN comment.updated_at IS '댓글이 수정된 날짜';
COMMENT ON COLUMN comment.deleted_at IS '댓글이 삭제된 날짜';

-- 10. video
-- DROP TABLE IF EXISTS video CASCADE;
CREATE TABLE video (
    id SERIAL PRIMARY KEY,
    game_id INT NOT NULL REFERENCES game_static(id),
    video_id VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    thumbnail TEXT NOT NULL,
    views BIGINT NOT NULL,
    upload_date TIMESTAMP NOT NULL,
    channel_profile_image TEXT NOT NULL,
    channel_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    updated_at TIMESTAMP DEFAULT now(),
    deleted_at TIMESTAMP
);

COMMENT ON COLUMN video.id IS '관련 영상 고유 식별자';
COMMENT ON COLUMN video.game_id IS '게임의 고유 식별자 (game_static 테이블 참조)';
COMMENT ON COLUMN video.video_id IS '유튜브 영상 고유 식별자';
COMMENT ON COLUMN video.title IS '관련 영상 제목';
COMMENT ON COLUMN video.thumbnail IS '관련 영상 썸네일 주소';
COMMENT ON COLUMN video.views IS '관련 영상 조회수';
COMMENT ON COLUMN video.upload_date IS '관련 영상 업로드 날짜';
COMMENT ON COLUMN video.channel_profile_image IS '관련 영상 업로드 채널 프로필 사진';
COMMENT ON COLUMN video.channel_name IS '관련 영상 업로드 채널 이름';
COMMENT ON COLUMN video.created_at IS '데이터베이스에 등록된 날짜';
COMMENT ON COLUMN video.updated_at IS '데이터베이스에서 수정된 최종 날짜';
COMMENT ON COLUMN video.deleted_at IS '서비스에서 더이상 사용하지 않을 날짜';