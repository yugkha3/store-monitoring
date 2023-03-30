import psycopg2
import csv

# Establishing a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="store-monitoring",
    user="postgres",
    password="2603"
)

# Creating a cursor object
cursor = conn.cursor()

# Creating the store_status table that will store store status data
cursor.execute(
    '''
        CREATE TABLE store_status (
            store_id BIGINT NOT NULL,
            status VARCHAR(10) NOT NULL CHECK (status IN ('active', 'inactive')),
            timestamp_utc TIMESTAMP NOT NULL
        );
    '''
)

print("store_status table created")

# Adding store_status data from the CSV file to the store_status table
with open('./csvs/store-status.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)

    for row in reader:
        store_id, status, timestamp_utc = row
        cursor.execute(
            "INSERT INTO store_status (store_id, status, timestamp_utc) VALUES (%s, %s, %s)", (store_id, status, timestamp_utc)
        )

    print("store_status data added")

# Creating the store_hours table that will store the business hours of all stores
cursor.execute(
    '''
        CREATE TABLE store_hours (
            store_id BIGINT NOT NULL,
            day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
            start_local_time TIME NOT NULL,
            end_local_time TIME NOT NULL
        );
    '''
)

print("store_hours table created")

# Adding store_hours data from the CSV file to the store_hours table
with open('./csvs/menu-hours.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)

    for row in reader:
        store_id, day, start_time_local, end_time_local = row
        cursor.execute(
            'INSERT INTO store_hours (store_id, day_of_week, start_local_time, end_local_time) VALUES (%s, %s, %s, %s)', (store_id, day, start_time_local, end_time_local)
        )

    print("store_hours data added")

# Creating the store_timezones table that will store the timezone of each store
cursor.execute(
    '''
        CREATE TABLE store_timezones (
            store_id BIGINT NOT NULL,
            timezone_str VARCHAR(255) NOT NULL,
            PRIMARY KEY (store_id)
        );
    '''
)

print("store_timezones table created")

# Adding the store timezones from the CSV file to the store_timezones table
with open('./csvs/store-timezones.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)

    for row in reader:
        store_id, timezone_str = row
        cursor.execute(
            'INSERT INTO store_timezones (store_id, timezone_str) VALUES (%s, %s)', (store_id, timezone_str)
        )

    print("store_timezones data added")

# Committing the changes to the database and closing the connection
conn.commit()
conn.close()

print("Database setup completed successfully.")
