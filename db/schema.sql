
-- RAD-2 Cloud Database Schema
-- 1. Raspberry Pi 기기 정보

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    pair_code_hash TEXT,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW()
);


-- 2. Radar Event 정보

CREATE TABLE IF NOT EXISTS events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    device_id TEXT NOT NULL
        REFERENCES devices(device_id),

    edge_event_id BIGINT NOT NULL,

    kind TEXT NOT NULL
        CHECK (kind IN ('camera', 'final')),

    person TEXT NOT NULL,

    t TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,

    situation TEXT NOT NULL,

    prob DOUBLE PRECISION
        CHECK (prob >= 0 AND prob <= 1),

    abnormal BOOLEAN NOT NULL DEFAULT FALSE,

    photo BOOLEAN NOT NULL DEFAULT FALSE,
    photo_key TEXT,

    label TEXT DEFAULT NULL,

    details JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    UNIQUE (device_id, edge_event_id)
);


-- 3. 이벤트 조회용 Index

CREATE INDEX IF NOT EXISTS idx_events_device_time
ON events (device_id, t DESC);


-- 4. Flutter App 인증 Token
CREATE TABLE IF NOT EXISTS app_tokens (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    device_id TEXT NOT NULL
        REFERENCES devices(device_id)
        ON DELETE CASCADE,

    token_hash TEXT NOT NULL UNIQUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);


-- 5. 휴대폰별 사진 수신 확인
CREATE TABLE IF NOT EXISTS photo_acks (
    app_token_id BIGINT NOT NULL
        REFERENCES app_tokens(id)
        ON DELETE CASCADE,

    event_id BIGINT NOT NULL
        REFERENCES events(id)
        ON DELETE CASCADE,

    acked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (app_token_id, event_id)
);


-- 6. Raspberry Pi별 알림 및 Camera 설정
CREATE TABLE IF NOT EXISTS settings (
    device_id TEXT PRIMARY KEY
        REFERENCES devices(device_id)
        ON DELETE CASCADE,

    notify JSONB NOT NULL DEFAULT
        '["장시간 체류", "배회", "반복 접근"]'::jsonb,

    camera_on BOOLEAN NOT NULL DEFAULT TRUE,

    camera_prob DOUBLE PRECISION NOT NULL DEFAULT 0.6
        CHECK (camera_prob >= 0 AND camera_prob <= 1),

    camera_hold_s DOUBLE PRECISION NOT NULL DEFAULT 1.0
        CHECK (camera_hold_s >= 0 AND camera_hold_s <= 10),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (jsonb_typeof(notify) = 'array')
);


-- 7. Firebase Cloud Messaging Token
CREATE TABLE IF NOT EXISTS push_tokens (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    app_token_id BIGINT NOT NULL
        REFERENCES app_tokens(id)
        ON DELETE CASCADE,

    fcm_token TEXT NOT NULL UNIQUE,

    platform TEXT NOT NULL
        CHECK (platform IN ('android', 'ios')),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- 8. 조회 성능용 Index
CREATE INDEX IF NOT EXISTS idx_app_tokens_device
ON app_tokens(device_id);

CREATE INDEX IF NOT EXISTS idx_photo_acks_event
ON photo_acks(event_id);

CREATE INDEX IF NOT EXISTS idx_push_tokens_app
ON push_tokens(app_token_id);

-- M6: Event별 휴대폰 Push 발송 기록
CREATE TABLE IF NOT EXISTS push_deliveries (
    event_id BIGINT NOT NULL
        REFERENCES events(id) ON DELETE CASCADE,

    app_token_id BIGINT NOT NULL
        REFERENCES app_tokens(id) ON DELETE CASCADE,

    sent_at TIMESTAMPTZ,

    PRIMARY KEY (event_id, app_token_id)
);