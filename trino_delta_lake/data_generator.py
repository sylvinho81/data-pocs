import os
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pyarrow.dataset as ds
import random
import boto3
from botocore.client import Config

# Initialize Faker
fake = Faker()

# MinIO configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minio123')
BUCKET_NAME = 'delta-lake'

# Initialize MinIO client
s3_client = boto3.client('s3',
                        endpoint_url=f'http://{MINIO_ENDPOINT}',
                        aws_access_key_id=MINIO_ACCESS_KEY,
                        aws_secret_access_key=MINIO_SECRET_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

# Create bucket if it doesn't exist
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
except:
    s3_client.create_bucket(Bucket=BUCKET_NAME)

def generate_clients(num_clients):
    clients = []
    for i in range(num_clients):
        client = {
            'id': i + 1,
            'name': fake.company(),
            'number_employments': random.randint(50, 1000),  # Increased minimum employments
            'created_at': fake.date_time_between(start_date='-2y'),
            'updated_at': fake.date_time_between(start_date='-1y')
        }
        clients.append(client)
    return pd.DataFrame(clients)

def generate_users(num_users, client_ids):
    users = []
    # Ensure more even distribution of users across clients
    for i in range(num_users):
        created_at = fake.date_time_between(start_date='-2y')
        # Using modulo to ensure even distribution
        client_id = client_ids[i % len(client_ids)]
        users.append({
            'id': i + 1,
            'name': fake.name(),
            'age': random.randint(18, 65),
            'client_id': client_id,
            'salary': round(random.uniform(30000, 150000), 2),
            'created_at': created_at,
            'updated_at': fake.date_time_between(start_date=created_at)
        })
    return pd.DataFrame(users)

def write_to_delta(df, table_name, partition_cols=None):
    # Convert pandas DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Storage options for delta-rs
    storage_options = {
        'AWS_ACCESS_KEY_ID': MINIO_ACCESS_KEY,
        'AWS_SECRET_ACCESS_KEY': MINIO_SECRET_KEY,
        'AWS_REGION': 'us-east-1',
        'AWS_ENDPOINT_URL': f'http://{MINIO_ENDPOINT}',
        'AWS_ALLOW_HTTP': 'true',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true'
    }
    
    # Write to Delta Lake
    write_deltalake(
        f's3://{BUCKET_NAME}/{table_name}',
        table,
        partition_by=partition_cols,
        mode='overwrite',
        storage_options=storage_options
    )

def main():
    # Generate 1000 clients
    print("Generating clients data...")
    clients_df = generate_clients(1000)
    
    # Generate 200000 users (average 200 users per client)
    print("Generating users data...")
    users_df = generate_users(200000, clients_df['id'].tolist())
    
    # Write clients table
    print("Writing clients table...")
    write_to_delta(clients_df, 'clients')
    
    # Write users table with partitioning
    print("Writing users table...")
    write_to_delta(users_df, 'users', partition_cols=['client_id'])
    
    print("Data generation and ingestion completed!")

if __name__ == "__main__":
    main() 