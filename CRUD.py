#!/usr/bin/env python
# coding: utf-8

import asyncpg
import asyncio
from datetime import datetime

CONNECTION = "paste your connection file"

async def create_hypertable():
    conn = await asyncpg.connect(CONNECTION)
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ticks (
        time TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        price DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        PRIMARY KEY (time, symbol)
    );
    """
    await conn.execute(create_table_query)
    
    create_hypertable_query = "SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);"
    await conn.execute(create_hypertable_query)
    
    create_continuous_aggregate_query = """
    CREATE MATERIALIZED VIEW one_min_candle
    WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 minute', time) AS bucket,
        symbol,
        first(price, time) AS open,
        max(price) AS high,
        min(price) AS low,
        last(price, time) AS close
    FROM
        ticks
    GROUP BY
        bucket, symbol
    WITH NO DATA;
    """
    await conn.execute(create_continuous_aggregate_query)
    
    print("Hypertable and continuous aggregate created successfully.")
    await conn.close()

async def insert_ticks(data):
    conn = await asyncpg.connect(CONNECTION)
    
    insert_query = """
    INSERT INTO ticks (time, symbol, price, volume)
    VALUES ($1, $2, $3, $4)
    """
    await conn.executemany(insert_query, data)
    print("Tick data inserted successfully.")
    
    await conn.close()

async def read_ticks():
    conn = await asyncpg.connect(CONNECTION)
    
    select_query = "SELECT * FROM ticks ORDER BY time DESC"
    rows = await conn.fetch(select_query)
    
    for row in rows:
        print(dict(row))
    
    await conn.close()
    return rows

async def read_aggregates():
    conn = await asyncpg.connect(CONNECTION)
    
    select_query = "SELECT * FROM one_min_candle ORDER BY bucket DESC"
    rows = await conn.fetch(select_query)
    
    for row in rows:
        print(dict(row))
    
    await conn.close()
    return rows

async def update_tick(time, symbol, new_values):
    conn = await asyncpg.connect(CONNECTION)
    
    update_query = """
    UPDATE ticks
    SET price = $3, volume = $4
    WHERE time = $1 AND symbol = $2
    """
    await conn.execute(update_query, time, symbol, new_values[0], new_values[1])
    print("Tick data updated successfully.")
    
    await conn.close()

async def delete_tick(time, symbol):
    conn = await asyncpg.connect(CONNECTION)
    
    delete_query = "DELETE FROM ticks WHERE time = $1 AND symbol = $2"
    await conn.execute(delete_query, time, symbol)
    print("Tick data deleted successfully.")
    
    await conn.close()

async def main():
    # Create hypertable and continuous aggregate
    await create_hypertable()
    
    # Insert sample data
    sample_ticks = [
        (datetime.strptime('2024-07-03 06:25:00+00:00', '%Y-%m-%d %H:%M:%S%z'), 'BTC', 61057.0, 0.5),
        (datetime.strptime('2024-07-03 06:24:30+00:00', '%Y-%m-%d %H:%M:%S%z'), 'BTC', 61058.8, 0.2),
        (datetime.strptime('2024-07-03 06:23:45+00:00', '%Y-%m-%d %H:%M:%S%z'), 'BTC', 61061.4, 0.1),
        (datetime.strptime('2024-07-03 06:22:30+00:00', '%Y-%m-%d %H:%M:%S%z'), 'BTC', 61072.7, 0.3),
        (datetime.strptime('2024-07-03 06:21:15+00:00', '%Y-%m-%d %H:%M:%S%z'), 'BTC', 61026.4, 0.4)
    ]
    
    await insert_ticks(sample_ticks)
    
    # Read raw tick data
    print("Reading raw tick data:")
    await read_ticks()
    
    # Read aggregated candlestick data
    print("Reading aggregated candlestick data:")
    await read_aggregates()

    # Update a tick data
    update_time = datetime.strptime('2024-07-03 06:25:00+00:00', '%Y-%m-%d %H:%M:%S%z')
    new_values = (62000.0, 0.6)
    await update_tick(update_time, 'BTC', new_values)
    print("After update:")
    await read_ticks()
    await read_aggregates()

    # Delete a tick data
    delete_time = datetime.strptime('2024-07-03 06:25:00+00:00', '%Y-%m-%d %H:%M:%S%z')
    await delete_tick(delete_time, 'BTC')
    print("After delete:")
    await read_ticks()
    await read_aggregates()

await main()
