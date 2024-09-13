import sqlite3
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Function to connect to SQLite database
def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

# Function to create the stock data table
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS harshfinance_data (
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER
        )
    ''')
    conn.commit()

# Function to insert stock data into the database
def insert_data(conn, data):
    cursor = conn.cursor()
    for index, row in data.iterrows():
        index_str = index.strftime('%Y-%m-%d')
        cursor.execute('''
            INSERT INTO harshfinance_data (date, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (index_str, row['Open'], row['High'], row['Low'], row['Close'], row['Adj Close'], row['Volume']))
    conn.commit()

# Function to retrieve data from the database
def get_data(conn):
    query = "SELECT * FROM harshfinance_data"
    df = pd.read_sql_query(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df

# Function to calculate Value at Risk (VaR)
def calculate_var(returns, confidence_level=0.95):
    return returns.quantile(1 - confidence_level)

# Connect to SQLite database and create table
conn = create_connection('harshfinance_data.db')
create_table(conn)

# Download historical data
data = yf.download('^GSPC', start='2022-04-02', end='2024-07-05')

# Insert data into the database
insert_data(conn, data)

# Retrieve data from the database
df = get_data(conn)

# Close the database connection
conn.close()

# Calculate moving averages
df['MA50'] = df['close'].rolling(window=50).mean()
df['MA200'] = df['close'].rolling(window=200).mean()

# Calculate RSI
def calculate_rsi(data, window=14):
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

df['RSI'] = calculate_rsi(df)

# Generate Buy/Sell signals
df['Signal'] = 0
df['Signal'][df['MA50'] > df['MA200']] = 1
df['Signal'][df['MA50'] < df['MA200']] = -1

# Calculate returns and strategy returns
df['Return'] = df['close'].pct_change()
df['Strategy Return'] = df['Signal'].shift(1) * df['Return']

# Calculate cumulative returns
df['Cumulative Return'] = (1 + df['Return']).cumprod() - 1
df['Cumulative Strategy Return'] = (1 + df['Strategy Return']).cumprod() - 1

# Calculate and print Value at Risk (VaR)
var = calculate_var(df['Strategy Return'].dropna())
print(f'Value at Risk (VaR) at 95% confidence level: {var:.4f}')

# Plot historical prices and moving averages
plt.figure(figsize=(14, 7))
plt.plot(df['close'], label='Close Price')
plt.plot(df['MA50'], label='50-day MA')
plt.plot(df['MA200'], label='200-day MA')
plt.title('S&P 500 Historical Prices with Moving Averages')
plt.legend()
plt.show()

# Plot RSI
plt.figure(figsize=(14, 5))
plt.plot(df['RSI'], label='RSI')
plt.axhline(70, color='r', linestyle='--', label='Overbought')
plt.axhline(30, color='g', linestyle='--', label='Oversold')
plt.title('Relative Strength Index (RSI)')
plt.legend()
plt.show()

plt.figure(figsize=(14, 7))
plt.plot(df['close'], label='Close Price')
plt.plot(df.loc[df['Signal'] == 1].index, df['close'][df['Signal'] == 1], '^', markersize=10, color='g', label='Buy Signal')
plt.plot(df.loc[df['Signal'] == -1].index, df['close'][df['Signal'] == -1], 'v', markersize=10, color='r', label='Sell Signal')
plt.title('Buy and Sell Signals')
plt.legend()
plt.show()

plt.figure(figsize=(14, 7))
plt.plot(df['Cumulative Return'], label='Cumulative Return')
plt.plot(df['Cumulative Strategy Return'], label='Cumulative Strategy Return')
plt.title('Cumulative Returns for Strategy')
plt.legend()
plt.show()
