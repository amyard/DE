import random
import faker
import csv
import ipaddress

fake = faker.Faker()

redirect_from_websites = [fake.domain_name() for _ in range(50)]
redirect_to_websites = [fake.domain_name() for _ in range(150)]
categories = [
    'Education', 'Entertainment', 'News', 'Shopping', 'Social Media', 'Sports',
    'Gaming', 'Health', 'Finance', 'Technology', 'Travel', 'Automotive', 'Home Improvement',
    'Fashion', 'Food & Drink', 'Real Estate', 'Art & Culture', 'Music', 'Movies', 'Books'
]
fieldnames = ['username', 'age', 'gender', 'ad_position', 'browsing_history', 'activity_time', 'ip_address', 'log',
                  'redirect_from', 'redirect_to']

def generate_log():
    # Randomly select browser
    browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    browser_version = random.randint(50, 100)
    browser = f"{random.choice(browsers)}/{browser_version}.0"

    # Randomly select OS and its version
    operating_systems = {
        'Windows': [f'Windows NT {random.choice([6.1, 6.2, 10.0])}', 'Win64; x64'],
        'Linux': [f'Linux; Android {random.choice([8, 9, 10])}', random.choice(['Mobile', 'Tablet'])],
        'Macintosh': [f'Macintosh; Intel Mac OS X 10_{random.randint(10, 15)}', '']
    }
    os_choice = random.choice(list(operating_systems.keys()))
    os_details = operating_systems[os_choice]

    # Randomly select device type
    devices = ['Mobile', 'Tablet', 'Laptop']
    device = random.choice(devices)

    # Constructing user-agent string
    log = f"Mozilla/5.0 ({os_details[0]}; {device}) AppleWebKit/537.36 (KHTML, like Gecko) {browser} {os_details[1]} Safari/537.36"

    return log

def generate_age():
    return random.choices(
        [random.randint(18, 100), random.choice([None, '']), random.randint(100, 220)],
        weights=[95, 4, 1],  # Probabilities: 95%, 4%, 1%
        k=1
    )[0]

def create_mock_data():
    # Usernames
    username = fake.user_name()

    # Age (can be None or empty)
    age = generate_age()

    # Gender (not just male/female)
    gender = random.choice(['male', 'female', 'non-binary', 'prefer not to say', 'genderqueer', 'genderfluid'])

    # Ad positions
    ad_position = random.choice(['top-banner', 'sidebar', 'inline', 'footer', 'popup'])

    # Browsing history categories (20 categories)
    browsing_history = random.sample(categories, random.randint(1, 5))  # Randomly pick 1-5 categories

    # Date and time of activity
    activity_time = fake.date_time_this_year()

    # Valid IP Address
    ip_address = str(ipaddress.IPv4Address(random.randint(0, (1 << 32) - 1)))

    # Logs (generated using the generate_log method)
    log = generate_log()

    # Redirect from (50 unique websites)
    redirect_from = random.choice(redirect_from_websites)

    # Redirect to (150 unique websites)
    redirect_to = random.choice(redirect_to_websites)

    # Compiling data into a dictionary
    data = {
        'username': username,
        'age': age,
        'gender': gender,
        'ad_position': ad_position,
        'browsing_history': browsing_history,
        'activity_time': activity_time.strftime("%Y-%m-%d %H:%M:%S"),
        'ip_address': ip_address,
        'log': log,
        'redirect_from': redirect_from,
        'redirect_to': redirect_to
    }

    return data

# for local usage
def generate_and_save_data_to_csv(filename='user_data.csv'):
    # Open CSV file for writing
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Generate and write 50 data rows
        for _ in range(50):
            data = create_mock_data()
            writer.writerow(data)

    print(f"Data saved to {filename}")

if __name__ == '__main__':
    generate_and_save_data_to_csv()