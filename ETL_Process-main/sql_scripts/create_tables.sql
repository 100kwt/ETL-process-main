CREATE TABLE stg_clients (
    client_id VARCHAR(128),
    last_name VARCHAR(128),
    first_name VARCHAR(128),
    patronymic VARCHAR(128),
    date_of_birth DATE,
    passport_num VARCHAR(128),
    passport_valid_to DATE,
    phone VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

CREATE TABLE stg_accounts (
    account VARCHAR(128),
    valid_to DATE,
    client VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

CREATE TABLE stg_cards (
    card_num VARCHAR(128),
    account VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

CREATE TABLE dwh_fact_transactions (
    transaction_id VARCHAR(128),
    transaction_date TIMESTAMP,
    amount DECIMAL(15, 2),
    card_num VARCHAR(128),
    oper_type VARCHAR(128),
    oper_result VARCHAR(128),
    terminal VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

CREATE TABLE dwh_fact_passport_blacklist (
    date DATE,
    passport VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

CREATE TABLE dwh_dim_terminals (
    terminal_id VARCHAR(128),
    terminal_type VARCHAR(128),
    terminal_city VARCHAR(128),
    terminal_address VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

CREATE TABLE rep_fraud (
    event_dt TIMESTAMP,
    passport VARCHAR(128),
    fio VARCHAR(128),
    phone VARCHAR(128),
    event_type VARCHAR(128),
    report_dt DATE
);

CREATE TABLE meta_fraud_analysis (
    analysis_id SERIAL PRIMARY KEY,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_fraud_cases INTEGER,
    most_common_fraud VARCHAR(255),
    top_fraudsters TEXT,
    most_frequent_time VARCHAR(50),
    source_files TEXT
);
