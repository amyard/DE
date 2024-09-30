import random
from faker import Faker
from datetime import datetime, timedelta


class FakerGenerator():
    def __init__(self, amount_of_users_data, amount_of_logs_data, amount_of_orders_data):
        self.__faker = Faker()
        self.__european_countries: list[str] = ['Germany', 'France', 'Italy', 'Spain', 'Netherlands', 'Belgium', 'Austria', 'Switzerland', 'Sweden', 'Norway']
        self.__devices: list[str] = ['Mobile', 'Tablet', 'Desktop', 'Laptop']

        self.logs_data: list[list] = []
        self.users_data: list[list] = []
        self.orders_data: list[list] = []
        self.user_emails: set = set()

        self.logs_columns: list = ["login_date", "logout_date", "user_email", "device"]
        self.users_columns: list = ["first_name", "last_name", "email", "gender", "dob", "phone_number", "address"]
        self.orders_columns: list = ["customer_email", "order_date", "delivery_address", "product_name", "product_category", "product_brand", "product_description", "quantity", "unit_price", "total_price"]

        self.amount_of_users_data = amount_of_users_data
        self.amount_of_logs_data = amount_of_logs_data
        self.amount_of_orders_data = amount_of_orders_data

        Faker.seed(0)

    def __generate_user_data(self) -> None:
        while len(self.users_data) < self.amount_of_users_data:
            first_name: str = self.__faker.first_name()
            last_name: str = self.__faker.last_name()
            email: str = self.__faker.email()
            if email in self.user_emails:
                continue
            self.user_emails.add(email)
            gender: str = random.choice(['Male', 'Female', 'Non-binary', 'Alien'])
            dob: datetime = self.__faker.date_time_between(start_date='-80y', end_date='-18y')
            phone_number: str = self.__faker.phone_number()
            address: str = f"{self.__faker.street_address()}, {self.__faker.secondary_address() + ',' if random.choice([True, False]) else ''} {self.__faker.city()}, {random.choice(self.__european_countries)}"
            self.users_data.append([first_name, last_name, email, gender, dob.strftime('%Y-%m-%d %H:%M:%S'), phone_number, address])

    def __generate_logs_data(self) -> None:
        for _ in range(self.amount_of_logs_data):
            user_email: str = random.choice(list(self.user_emails)) if random.random() < 0.99 else self.__faker.email()
            login_date: datetime = self.__faker.date_time_between(start_date='-1y', end_date='now')
            logout_date: datetime = login_date + timedelta(hours=random.randint(1, 8))
            device: str = random.choice(self.__devices)
            self.logs_data.append([login_date.strftime('%Y-%m-%d %H:%M:%S'), logout_date.strftime('%Y-%m-%d %H:%M:%S'), user_email, device])

    def __generate_orders_data(self) -> None:
        # Generate 300 unique product names
        product_names: list[str] = []
        while len(product_names) < 300:
            product_name: str = self.__faker.unique.word()
            product_names.append(product_name)

        # Generate orders data
        temp_d: dict = dict()

        for _ in range(self.amount_of_orders_data):
            customer_email: str = random.choice(list(self.user_emails))
            order_date: datetime = self.__faker.date_time_between(start_date='-1y', end_date='now')
            u_key = f"{customer_email}_{order_date.strftime('%Y-%m-%d')}"
            u_value = f"{self.__faker.street_address()}, {self.__faker.secondary_address() if random.choice([True, False]) else ''}, {self.__faker.city()}, {random.choice(self.__european_countries)}"
            delivery_address: str = temp_d.setdefault(u_key, u_value)
            product_name: str = random.choice(product_names)
            product_category: str = self.__faker.word()
            product_brand: str = self.__faker.company()
            product_description: str = self.__faker.sentence()
            quantity: int = random.randint(1, 10)
            unit_price: float = round(random.uniform(5.0, 500.0), 2)
            total_price: float = round(quantity * unit_price, 2)
            self.orders_data.append(
                [customer_email, order_date.strftime('%Y-%m-%d %H:%M:%S'), delivery_address, product_name, product_category,
                 product_brand, product_description, quantity, unit_price, total_price])

    def generate(self):
        self.__generate_user_data()
        self.__generate_logs_data()
        self.__generate_orders_data()
