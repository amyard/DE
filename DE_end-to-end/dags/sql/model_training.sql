CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    customer_id UUID UNIQUE NOT NULL,
    customer_satisfaction_speed INTEGER NOT NULL,
    customer_satisfaction_product INTEGER NOT NULL,
    customer_satisfaction_service INTEGER NOT NULL,
    product_type VARCHAR(50) NOT NULL,
    avg_amount_captured DECIMAL(16, 2) NOT NULL
);