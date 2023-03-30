import psycopg2
import pytz
import csv
import io
from datetime import datetime, timedelta


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="store-monitoring",
        user="postgres",
        password="2603"
    )


def generate_report():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT max(timestamp_utc) FROM store_status;")
    current_timestamp = cursor.fetchone()[0]
    current_timestamp = pytz.UTC.localize(current_timestamp)

    cursor.execute("SELECT DISTINCT store_id FROM store_status;")
    store_ids = [row[0] for row in cursor.fetchall()]

    """
    store_ids = [
        8419537941919820732,
        8139926242460185114,
        9055649751952768824,
        1050565545391667097,
        3483930781272060942,
        1740222068509982431,
        8778651700128892847,
        2190599106298424769,
        8175229951397380687,
        7463734423358941330,
        6160914795278193489
    ]
    """

    num_stores = len(store_ids)
    print(f"Total number of distinct stores: {num_stores}")    
    
    report_data = []

    for store_id in store_ids:
        print('Generating report for store_id: ', store_id)
        cursor.execute(
            "SELECT timezone_str FROM store_timezones WHERE store_id = %s;", (store_id,))
        timezone_str = cursor.fetchone()

        if timezone_str is None:
            timezone_str = 'America/Chicago'
        else:
            timezone_str = timezone_str[0]

        store_timezone = pytz.timezone(timezone_str)

        # Get local timestamps for different intervals
        local_now = current_timestamp.astimezone(store_timezone)
        local_hour_ago = local_now - timedelta(hours=1)
        local_day_ago = local_now - timedelta(days=1)
        local_week_ago = local_now - timedelta(weeks=1)

        # Get UTC timestamps for different intervals
        utc_hour_ago = local_hour_ago.astimezone(pytz.UTC)
        utc_day_ago = local_day_ago.astimezone(pytz.UTC)
        utc_week_ago = local_week_ago.astimezone(pytz.UTC)

        # Get the store's business hours
        cursor.execute(
            "SELECT day_of_week, start_local_time, end_local_time FROM store_hours WHERE store_id = %s;", (store_id,))
        store_hours = cursor.fetchall()

        if not store_hours:
            store_hours = [(i, datetime.strptime('00:00:00', '%H:%M:%S').time(), datetime.strptime('23:59:59', '%H:%M:%S').time()) for i in range(7)]

        def is_within_business_hours(timestamp):
            local_timestamp = timestamp.astimezone(store_timezone)
            day_of_week = local_timestamp.weekday()
            local_time = local_timestamp.time()

            for hours in store_hours:
                if hours[0] == day_of_week and hours[1] <= local_time <= hours[2]:
                    return True
            return False

        # Get the store's status records for the last hour, day, and week
        cursor.execute(
            "SELECT timestamp_utc, status FROM store_status WHERE store_id = %s AND timestamp_utc BETWEEN %s AND %s;",
            (store_id, utc_week_ago, current_timestamp)
        )

        records = cursor.fetchall()

        # Calculate uptime and downtime for different intervals
        def calculate_uptime_downtime(interval_start, interval_end):
            duration = timedelta()
            uptime = timedelta()
            downtime = timedelta()

            last_timestamp = interval_start
            last_status = None

            for record in records:
                timestamp_utc, status = record
                timestamp_utc = pytz.UTC.localize(timestamp_utc)

                if interval_start <= timestamp_utc < interval_end:
                    if is_within_business_hours(timestamp_utc):
                        if last_status is not None:
                            duration += timestamp_utc - last_timestamp
                            if last_status == 'active':
                                uptime += duration
                            else:
                                downtime += duration
                            duration = timedelta()
                        last_status = status
                        last_timestamp = timestamp_utc

            if last_status is not None:
                duration += interval_end - last_timestamp
                if last_status == 'active':
                    uptime += duration
                else:
                    downtime += duration
            return uptime, downtime
        uptime_last_hour, downtime_last_hour = calculate_uptime_downtime(utc_hour_ago, current_timestamp)
        uptime_last_day, downtime_last_day = calculate_uptime_downtime(utc_day_ago, current_timestamp)
        uptime_last_week, downtime_last_week = calculate_uptime_downtime(utc_week_ago, current_timestamp)

        # Append store data to the report
        report_data.append([
            store_id,
            uptime_last_hour.seconds // 60,  # Convert to minutes
            uptime_last_day.total_seconds() // 3600,  # Convert to hours
            uptime_last_week.total_seconds() // 3600,  # Convert to hours
            downtime_last_hour.seconds // 60,  # Convert to minutes
            downtime_last_day.total_seconds() // 3600,  # Convert to hours
            downtime_last_week.total_seconds() // 3600  # Convert to hours
        ])

    # Generate CSV from the report data
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "store_id",
        "uptime_last_hour(in minutes)",
        "uptime_last_day(in hours)",
        "update_last_week(in hours)",
        "downtime_last_hour(in minutes)",
        "downtime_last_day(in hours)",
        "downtime_last_week(in hours)"
    ])

    for row in report_data:
        writer.writerow(row)

    return output.getvalue()