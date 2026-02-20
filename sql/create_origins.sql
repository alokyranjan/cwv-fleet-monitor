-- DDL reference for the origins table (dealer site registry)
-- Actual creation is handled by scripts/setup_dataset.py

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.origins` (
    origin          STRING NOT NULL,
    dealer_name     STRING,
    dealer_group    STRING,
    oem_brand       STRING,
    region          STRING,
    state           STRING,
    platform_version STRING,
    is_active       BOOLEAN,
    tags            STRING,
    added_at        TIMESTAMP,
    updated_at      TIMESTAMP
);
