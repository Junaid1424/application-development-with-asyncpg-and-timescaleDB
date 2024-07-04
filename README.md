# How to Build Applications with Asyncpg and PostgreSQL

This repository provides code samples and a detailed guide on integrating Asyncpg with TimescaleDB, focusing on real-time data visualization and CRUD operations. The guide complements our blog post: "How to Build Applications with Asyncpg and PostgreSQL".
Preparation and Requirements

Before starting, ensure you meet the following requirements to successfully run the code examples provided:
# Python Environment

    Python version: Ensure Python 3.6 or later is installed on your machine.
    Jupyter Notebook: Required to run the .ipynb files without errors.

# PostgreSQL and TimescaleDB

    PostgreSQL: Make sure PostgreSQL is installed.
    TimescaleDB: Install the TimescaleDB extension on your PostgreSQL instance. Follow the installation guide in the official TimescaleDB documentation.

# Repository Structure

    connection.py: Establishes a connection to TimescaleDB and fetches data for real-time visualization.
    CRUD.py: Demonstrates CRUD operations using Asyncpg and TimescaleDB. This script handles database creation, data insertion, and query execution.
    Notebooks/: Contains Jupyter notebooks that guide you through the usage of connection.py and CRUD.py scripts with interactive code cells.
# Usage

    Real-time Data Visualization
    Use connection.py to connect to TimescaleDB and visualize data in real time.

    CRUD Operations
    CRUD.py contains functions to create a database, insert data, and perform various database operations using Asyncpg.

# Modifications for Non-Jupyter Environments

If you plan to run the scripts outside of Jupyter (e.g., as standalone Python scripts), you will need to adjust the await syntax used in Asyncpg calls. Replace inline await statements with asynchronous function calls wrapped in asyncio.run().
