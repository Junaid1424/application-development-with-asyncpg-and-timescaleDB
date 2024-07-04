#!/usr/bin/env python
# coding: utf-8

# In[2]:


import asyncpg
import asyncio
from datetime import datetime
CONNECTION = "postgres://tsdbadmin:yhseuyz1ayazztqh@c59hzbidi4.nmrpvcfuxt.tsdb.cloud.timescale.com:36330/tsdb?sslmode=require"


async def main():
    conn = await asyncpg.connect(CONNECTION)
    extensions = await conn.fetch("select extname, extversion from pg_extension")
    for extension in extensions:
        print(extension)
    await conn.close()

await main()


# In[7]:


CONNECTION = "postgres://tsdbadmin:yhseuyz1ayazztqh@c59hzbidi4.nmrpvcfuxt.tsdb.cloud.timescale.com:36330/tsdb?sslmode=require"

async def create_table():
    conn = await asyncpg.connect(CONNECTION)
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS finance (
        bucket TIMESTAMPTZ NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        PRIMARY KEY (bucket)
    );
    """
    await conn.execute(create_table_query)
    print("Table created successfully.")
    
    await conn.close()

async def insert_data(data):
    conn = await asyncpg.connect(CONNECTION)
    
    insert_query = """
    INSERT INTO finance (bucket, open, high, low, close)
    VALUES ($1, $2, $3, $4, $5)
    """
    await conn.executemany(insert_query, data)
    print("Data inserted successfully.")
    
    await conn.close()

async def read_data():
    conn = await asyncpg.connect(CONNECTION)
    
    select_query = "SELECT * FROM finance ORDER BY bucket DESC"
    rows = await conn.fetch(select_query)
    
    for row in rows:
        print(dict(row))
    
    await conn.close()
    return rows

async def update_data(bucket, new_values):
    conn = await asyncpg.connect(CONNECTION)
    
    update_query = """
    UPDATE finance
    SET open = $2, high = $3, low = $4, close = $5
    WHERE bucket = $1
    """
    await conn.execute(update_query, bucket, *new_values)
    print("Data updated successfully.")
    
    await conn.close()

async def delete_data(bucket):
    conn = await asyncpg.connect(CONNECTION)
    
    delete_query = "DELETE FROM finance WHERE bucket = $1"
    await conn.execute(delete_query, bucket)
    print("Data deleted successfully.")
    
    await conn.close()

async def main():
    await create_table()
    
    sample_data = [
        (datetime.strptime('2024-07-03 06:25:00+00:00', '%Y-%m-%d %H:%M:%S%z'), 61057.0, 61058.8, 61057.0, 61058.8),
        (datetime.strptime('2024-07-03 06:24:00+00:00', '%Y-%m-%d %H:%M:%S%z'), 61061.4, 61061.4, 61061.4, 61061.4),
        (datetime.strptime('2024-07-03 06:23:00+00:00', '%Y-%m-%d %H:%M:%S%z'), 61072.6, 61072.7, 61061.4, 61061.4),
        (datetime.strptime('2024-07-03 06:22:00+00:00', '%Y-%m-%d %H:%M:%S%z'), 61026.4, 61069.5, 61026.4, 61069.5),
        (datetime.strptime('2024-07-03 06:21:00+00:00', '%Y-%m-%d %H:%M:%S%z'), 61060.4, 61060.4, 61033.4, 61033.4)
    ]
    
    await insert_data(sample_data)
    await read_data()
    
    update_bucket = datetime.strptime('2024-07-03 06:25:00+00:00', '%Y-%m-%d %H:%M:%S%z')
    new_values = (62000.0, 62010.0, 61990.0, 62005.0)
    await update_data(update_bucket, new_values)
    
    await read_data()
    
    delete_bucket = datetime.strptime('2024-07-03 06:25:00+00:00', '%Y-%m-%d %H:%M:%S%z')
    await delete_data(delete_bucket)
    
    await read_data()

await main()


# In[ ]:




