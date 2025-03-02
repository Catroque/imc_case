
CREATE TABLE ingestion_job_execution 
(
    id           SERIAL PRIMARY KEY,
    execution_id VARCHAR(64) NOT NULL,
    job_name     VARCHAR(64) NOT NULL,
    tickers      VARCHAR(1000) NOT NULL,
    interval     INTEGER NOT NULL,
    overlap      INTEGER NOT NULL,
    bucket_name  VARCHAR(128) NOT NULL,
    start_time   TIMESTAMP NOT NULL,
    end_time     TIMESTAMP,
    filename     VARCHAR(128),
    records      INTEGER,
    status       VARCHAR(64) NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE CONSTRAINT unique_execution_id UNIQUE (execution_id);
