USERS_INSERT_QUERY = """
                        INSERT INTO users (first_name, last_name, email, gender, dob, phone_number, address)
                        VALUES %s
                        ON CONFLICT (email) DO NOTHING;
                    """

LOGS_INSERT_QUERY = """
                INSERT INTO logs (login_date, logout_date, user_email, device)
                VALUES %s;
            """

ORDERS_INSERT_QUERY = """
                INSERT INTO orders (customer_email, order_date, delivery_address, product_name, product_category,  product_brand,  product_description,  quantity,  unit_price,  total_price)
                VALUES %s;
            """