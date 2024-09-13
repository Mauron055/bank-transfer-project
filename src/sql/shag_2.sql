CREATE TABLE STV2024050734__STAGING.transactions (
    operation_id VARCHAR(60) NOT NULL,
    account_number_from INTEGER,
    account_number_to INTEGER,
    currency_code VARCHAR(3) NOT NULL,
    country VARCHAR(30),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INTEGER,
    transaction_dt TIMESTAMP WITHOUT TIME ZONE,
    
    -- Проекции по датам
    transaction_dt_year INTEGER,
    transaction_dt_month INTEGER,
    transaction_dt_day INTEGER,
    
    -- Хеш-ключ для сегментирования
    transaction_hash INTEGER
);
-- таблица для currencies
CREATE TABLE STV2024050734__STAGING.currencies (
    date_update TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    currency_code_with VARCHAR(3),
    currency_with_div DECIMAL(5, 3),
    
    -- Проекции по датам
    date_update_year INTEGER,
    date_update_month INTEGER,
    date_update_day INTEGER,
    
    -- Хеш-ключ для сегментирования
    currency_hash INTEGER
);
-- таблица для витрины
CREATE TABLE STV2024050734__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from VARCHAR(3) NOT NULL,
    amount_total DECIMAL(18, 2),
    cnt_transactions INTEGER,
    avg_transactions_per_account DECIMAL(18, 2),
    cnt_accounts_make_transactions INTEGER,
    
    -- Хеш-ключ для сегментирования
    global_metrics_hash INTEGER
);
-- заполняем проекции по датам в таблицах
UPDATE STV2024050734__STAGING.transactions
    SET transaction_dt_year = EXTRACT(YEAR FROM transaction_dt),
        transaction_dt_month = EXTRACT(MONTH FROM transaction_dt),
        transaction_dt_day = EXTRACT(DAY FROM transaction_dt);
UPDATE STV2024050734__STAGING.currencies
    SET date_update_year = EXTRACT(YEAR FROM date_update),
        date_update_month = EXTRACT(MONTH FROM date_update),
        date_update_day = EXTRACT(DAY FROM date_update);
ALTER TABLE STV2024050734__DWH.global_metrics
       ADD COLUMN date_update_year INTEGER;

   ALTER TABLE STV2024050734__DWH.global_metrics
       ADD COLUMN date_update_month INTEGER;

   ALTER TABLE STV2024050734__DWH.global_metrics
       ADD COLUMN date_update_day INTEGER;
UPDATE STV2024050734__DWH.global_metrics
   SET date_update_year = EXTRACT(YEAR FROM date_update),
       date_update_month = EXTRACT(MONTH FROM date_update),
       date_update_day = EXTRACT(DAY FROM date_update);
-- добавляем хеш-функции
UPDATE STV2024050734__STAGING.transactions 
SET transaction_hash = hash(
    CAST(transaction_dt_year AS VARCHAR) || 
    CAST(transaction_dt_month AS VARCHAR) || 
    CAST(transaction_dt_day AS VARCHAR) || 
    operation_id
);
UPDATE STV2024050734__DWH.global_metrics 
SET global_metrics_hash = hash(
    CAST(date_update_year AS VARCHAR) || 
    CAST(date_update_month AS VARCHAR) || 
    CAST(date_update_day AS VARCHAR) || 
    currency_from
);
