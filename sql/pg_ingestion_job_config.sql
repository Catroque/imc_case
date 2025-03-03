
CREATE TABLE ingestion_job_config 
(
    job_name    VARCHAR(64) PRIMARY KEY,
    tickers     VARCHAR(1000) NOT NULL,
    period      INTEGER,
    interval    INTEGER,
    overlap     INTEGER,
    bucket_name VARCHAR(128) NOT NULL,
    updated_by  VARCHAR(64)  NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE CONSTRAINT unique_job_name UNIQUE (job_name);
