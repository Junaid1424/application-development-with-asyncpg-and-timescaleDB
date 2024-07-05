#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#pip install asyncpg


# In[4]:


import asyncpg
import pandas as pd
import plotly.graph_objects as go
import asyncio
import time
from IPython.display import display, clear_output


# In[1]:

# Database credentials
db_url = "paste your credentials"

async def fetch_data():
    # Connect to the database
    conn = await asyncpg.connect(db_url)

    # Define the query to fetch data for the past 1 week
    query = """
    SELECT bucket, open, high, low, close
    FROM one_min_candle
    WHERE symbol = 'BTC/USD'
    AND bucket >= NOW() - INTERVAL '1 week'
    ORDER BY bucket DESC;
    """

    # Execute the query and fetch the results
    rows = await conn.fetch(query)

    # Convert the results to a pandas DataFrame
    data = pd.DataFrame(rows, columns=['bucket', 'open', 'high', 'low', 'close'])

    #save the DataFrame to a CSV file
    data.to_csv('data.csv', index=False)

    # Close the connection
    await conn.close()
    
    # Return the DataFrame to display it in the notebook
    return data

# Using await directly in Jupyter to run the async function and display the DataFrame
display(await fetch_data())


# In[5]:


async def update_plot(interval=60):
    while True:
        data = await fetch_data()
        data['bucket'] = pd.to_datetime(data['bucket'])

        fig = go.Figure(data=[go.Candlestick(x=data['bucket'],
                                             open=data['open'],
                                             high=data['high'],
                                             low=data['low'],
                                             close=data['close'])])

        fig.update_layout(
            title='Stock Market Data',
            yaxis_title='Price',
            xaxis_title='Date',
            xaxis_rangeslider_visible=False
        )

        clear_output(wait=True)
        display(fig)
        time.sleep(interval)


# In[ ]:


# Run the update_plot function with a specified interval (e.g., 60 seconds)
await update_plot(interval=60)

