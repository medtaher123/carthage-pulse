-- 1. Create the extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS enriched_events (
    event_id VARCHAR,
    event_type VARCHAR,
    posted_in_subreddit VARCHAR,
    author VARCHAR,
    url VARCHAR,
    title VARCHAR,
    content TEXT,
    timestamp TIMESTAMP NOT NULL,
    has_media BOOLEAN,
    media_urls TEXT[],
    score INTEGER,
    upvote_ratio FLOAT,
    num_comments INTEGER,
    is_crosspost BOOLEAN,
    original_subreddit VARCHAR,
    languages TEXT[],
    translation TEXT,
    sentiment_score FLOAT,
    intent VARCHAR,
    topics TEXT[],
    entities TEXT,
    PRIMARY KEY (event_id, timestamp) -- Combined PK
);


CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR,
    event_type VARCHAR,
    posted_in_subreddit VARCHAR,
    author VARCHAR,
    url VARCHAR,
    title VARCHAR,
    content TEXT,
    timestamp TIMESTAMP NOT NULL,
    has_media BOOLEAN,
    media_urls TEXT[],
    score INTEGER,
    upvote_ratio FLOAT,
    num_comments INTEGER,
    is_crosspost BOOLEAN,
    original_subreddit VARCHAR,
    PRIMARY KEY (event_id, timestamp)
);

CREATE TABLE IF NOT EXISTS trending_words_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    word TEXT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, word)
);

CREATE TABLE IF NOT EXISTS trending_topics_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    topic TEXT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, topic)
);


-- 4. Convert to Hypertables
-- This automatically partitions your data into 7-day chunks by default
SELECT create_hypertable('enriched_events', 'timestamp');
SELECT create_hypertable('raw_events', 'timestamp');
SELECT create_hypertable('trending_words_metrics', 'window_start');
SELECT create_hypertable('trending_topics_metrics', 'window_start');



-- Automatically delete raw events older than 30 days
-- SELECT add_retention_policy('raw_events', INTERVAL '30 days');

-- Keep enriched events for 90 days
-- SELECT add_retention_policy('enriched_events', INTERVAL '90 days');