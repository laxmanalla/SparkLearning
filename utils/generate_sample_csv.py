import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_data(num_records=5000):
    # Set the time window
    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=30)

    # Generate random data within the time window
    rec_device_ids = np.random.randint(1, 4, size=num_records)
    channels = np.random.choice([8, 10, 12, 14], size=num_records)
    timestamps = pd.date_range(start=start_time, end=end_time, periods=num_records)
    values = np.random.uniform(20, 50, size=num_records)

    # Create a DataFrame
    df = pd.DataFrame({
        'rec_device_id': rec_device_ids,
        'channel': channels,
        'timestamp': timestamps,
        'value': values
    })

    # Save the DataFrame to a CSV file
    df.to_csv('sample_data.csv', index=False)

    return df

# Generate and print the sample data
sample_data = generate_sample_data()

sample_data.to_csv('data/channel_data.csv', index=False)
