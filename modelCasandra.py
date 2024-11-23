#!/usr/bin/env python3
import logging
from datetime import datetime

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance DECIMAL,
        name TEXT STATIC,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity DECIMAL,
        PRIMARY KEY ((account),symbol)
    )
"""

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

################################################################
CREATE_TRADES_BY_ACCOUNT_TD_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_td (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), type, trade_id)
    ) WITH CLUSTERING ORDER BY (type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_STD_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_std (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account),symbol, type, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_SD_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_sd (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, trade_id DESC)
"""
################################################################
#   Q1
SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

################################################################
#   Q2
SELECT_USER_POSITIONS = """
    SELECT account, symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""

#   Q3.1
SELECT_TRADES_ACCOUNT_D = """
    SELECT account, toTimestamp(trade_id) as trade_id, amount, price, shares, symbol, type
    FROM  trades_by_a_d
    WHERE account = ?
"""

#   Q3.2
SELECT_TRADES_ACCOUNT_D_DR = """
    SELECT account, toTimestamp(trade_id) as trade_id, amount, price, shares, symbol, type
    FROM  trades_by_a_d
    WHERE account = ? AND trade_id >= minTimeuuid(?) AND trade_id < maxTimeuuid(?)
"""
#   Q3.3
SELECT_TRADES_ACCOUNT_TD = """
    SELECT account, toTimestamp(trade_id) as trade_id, amount, price, shares, symbol, type
    FROM trades_by_a_td
    WHERE account = ? AND type = ? AND trade_id >= minTimeuuid(?) AND trade_id < maxTimeuuid(?)
"""
#   Q3.4
SELECT_TRADES_ACCOUNT_STD = """
    SELECT account, toTimestamp(trade_id) as trade_id, amount, price, shares, symbol, type
    FROM trades_by_a_std
    WHERE account = ? AND symbol = ? AND type = ? AND trade_id >= minTimeuuid(?) AND trade_id < maxTimeuuid(?)
"""
#   Q3.5
SELECT_TRADES_ACCOUNT_SD = """
    SELECT account, toTimestamp(trade_id) as trade_id, amount, price, shares, symbol, type
    FROM trades_by_a_sd
    WHERE account = ? AND symbol = ? AND trade_id >= minTimeuuid(?) AND trade_id < maxTimeuuid(?)
"""

#################################################################
def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_TD_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_STD_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_SD_TABLE)

#   Q1
def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")

################################################################
#   Q2
def get_user_positions(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = []
    for row in rows:
        accounts.append(row.account_number)
    for account in accounts:
        stmt = session.prepare(SELECT_USER_POSITIONS)
        rows = session.execute(stmt, [account])
        print(f"\n=== Positions for Account: {account} ===")
        for row in rows:
            print(f"- {row.symbol}: {row.quantity}")

#   Q3.1
def get_trade_history(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = []
    for row in rows:
        accounts.append(row.account_number)
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_D)
        rows = session.execute(stmt, [account])
        print(f"\n=== Trades for Account: {account} ===")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_id: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

#   Q3.2
def get_trade_history_dr(session, username, date_1, date_2):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime(date_1, "%Y-%m-%d")
    end_date = datetime.strptime(date_2, "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_D_DR)
        rows = session.execute(stmt, [account, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === Between the dates: {date_1} and {date_2}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

#   Q3.3
def get_trade_history_t_dr(session, username, type, date_1, date_2):
    log.info(f"Retrieving trades for {username} of type {type} between {date_1} and {date_2}")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime(date_1, "%Y-%m-%d")
    end_date = datetime.strptime(date_2, "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_TD)
        rows = session.execute(stmt, [account, type, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === With the type: {type} Between the dates: {date_1} and {date_2}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

def get_trade_history_t(session, username, type):
    log.info(f"Retrieving trades for {username} of type {type}")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime("2000-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("3000-12-12", "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_TD)
        rows = session.execute(stmt, [account, type, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === With the type: {type}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

#   Q3.4
def get_trade_history_st_dr(session, username, symbol, type, date_1, date_2):
    log.info(f"Retrieving trades for {username} of symbol {symbol} with type {type} between {date_1} and {date_2}")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime(date_1, "%Y-%m-%d")
    end_date = datetime.strptime(date_2, "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_STD)
        rows = session.execute(stmt, [account, symbol, type, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === With the symbol {symbol} type: {type} Between the dates: {date_1} and {date_2}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

def get_trade_history_st(session, username, symbol, type):
    log.info(f"Retrieving trades for {username} of symbol {symbol} with type {type}")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime("2000-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("3000-12-12", "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_STD)
        rows = session.execute(stmt, [account, symbol, type, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === With the symbol {symbol} type: {type}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

#   Q3.5
def get_trade_history_s_dr(session, username, symbol, date_1, date_2):
    log.info(f"Retrieving trades for {username} of symbol {symbol} between {date_1} and {date_2}")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime(date_1, "%Y-%m-%d")
    end_date = datetime.strptime(date_2, "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_SD)
        rows = session.execute(stmt, [account, symbol, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === With the symbol {symbol} Between the dates: {date_1} and {date_2}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

def get_trade_history_s(session, username, symbol):
    log.info(f"Retrieving trades for {username} of symbol {symbol}")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = [row.account_number for row in rows]
    start_date = datetime.strptime("2000-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("3000-12-12", "%Y-%m-%d")
    for account in accounts:
        stmt = session.prepare(SELECT_TRADES_ACCOUNT_SD)
        rows = session.execute(stmt, [account, symbol, start_date, end_date])
        print(f"\n=== Trades for Account: {account} === With the symbol {symbol}")
        for row in rows:
            trade_date = row.trade_id.strftime("%Y-%m-%d")
            print(f"- trade_date: {trade_date}\t type: {row.type}\t symbol: {row.symbol}\t shares: {row.shares}\t price: {row.price}       amount: {row.amount}")

################################################################
