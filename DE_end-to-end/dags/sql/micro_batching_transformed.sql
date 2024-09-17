CREATE TABLE IF NOT EXISTS {{ params.table_name_transformed }} (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    age INT,
    gender VARCHAR(50),
    ad_position VARCHAR(255),
    browsing_history TEXT,
    activity_time TIMESTAMP,
    ip_address VARCHAR(45),
    log TEXT,
    redirect_from VARCHAR(255),
    redirect_to VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    age_group VARCHAR(255),
    hours INT,
    time_of_day VARCHAR(255),
    device_type VARCHAR(255),
    os_family VARCHAR(255),
    browser_family VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS df_year_month (
    years INT,
    month INT,
    current_period_amount BIGINT NOT NULL,
    previous_period_amount BIGINT NOT NULL,
    increase BIGINT NOT NULL,
    clicks_increase BOOLEAN NOT NULL,
    year_sum BIGINT,
    percentage_per_year DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS df_country (
    country VARCHAR(255),
    amount BIGINT NOT NULL,
    rank INT NOT NULL
);

CREATE TABLE IF NOT EXISTS df_city (
    city VARCHAR(255),
    amount BIGINT NOT NULL,
    rank INT NOT NULL
);

CREATE TABLE IF NOT EXISTS df_age_group (
    age_group VARCHAR(255),
    time_of_day VARCHAR(255),
    amount BIGINT NOT NULL
);