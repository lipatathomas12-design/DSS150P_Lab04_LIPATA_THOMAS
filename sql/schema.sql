CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS audit;


CREATE TABLE IF NOT EXISTS raw.bgg_games (
    source_sha256      text        NOT NULL,
    source_row_number  integer     NOT NULL,
    ingested_at        timestamptz NOT NULL,
    ingest_run_id      text        NOT NULL,
    id text, name text, year_published text, min_players text, max_players text,
    play_time text, min_age text, users_rated text, rating_average text, bgg_rank text,
    complexity_average text, owned_users text, mechanics text, domains text,
    PRIMARY KEY (source_sha256, source_row_number)
);

CREATE TABLE IF NOT EXISTS staging.bgg_games (
    id bigint PRIMARY KEY,
    source_row_number integer,
    name text, year_published double precision, min_players double precision,
    max_players double precision, play_time double precision, min_age double precision,
    users_rated double precision, rating_average double precision, bgg_rank double precision,
    complexity_average double precision, owned_users double precision,
    mechanics text, domains text,
    staged_at timestamptz NOT NULL, run_id text NOT NULL
);

-- curated: cleaned, final types
CREATE TABLE IF NOT EXISTS curated.bgg_games (
    id bigint PRIMARY KEY,
    name text NOT NULL, year_published integer NOT NULL, min_players integer NOT NULL,
    max_players integer NOT NULL, play_time integer NOT NULL, min_age integer NOT NULL,
    users_rated integer NOT NULL, rating_average numeric(4,2) NOT NULL, bgg_rank integer NOT NULL,
    complexity_average numeric(4,2) NOT NULL, owned_users integer NOT NULL,
    mechanics text NOT NULL, domains text NOT NULL,
    curated_at timestamptz NOT NULL, run_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS ml.predictions (
    game_id bigint PRIMARY KEY,
    y_true smallint, y_pred smallint,
    p_low double precision, p_medium double precision, p_high double precision,
    model_name text, run_id text, predicted_at timestamptz
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    pipeline_run_id text PRIMARY KEY,
    started_at timestamptz, finished_at timestamptz, status text, error text,
    raw_rows integer, staging_rows integer, curated_rows integer,
    train_rows integer, test_rows integer, prediction_rows integer,
    summary jsonb
);

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    pipeline_run_id text, stage text, check_name text, passed boolean, detail text,
    PRIMARY KEY (pipeline_run_id, stage, check_name)
);