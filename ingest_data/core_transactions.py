import psycopg2
import random
from datetime import datetime, timedelta

# Connection parameters for the databases.
# core_users holds the accounts and users data.
CORE_USERS_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "core_users",
    "user": "postgres",
    "password": "postgres"
}

# core_transactions holds the transactions tables and the view.
CORE_TRANS_CONN = {
    "host": "localhost",
    "port": 5433,
    "dbname": "core_transactions",
    "user": "postgres",
    "password": "postgres"
}

def random_date():
    """Return a random datetime within the past 100 days."""
    now = datetime.now()
    delta = timedelta(days=random.randint(0, 100),
                      hours=random.randint(0, 23),
                      minutes=random.randint(0, 59))
    return now - delta

def get_accounts(conn, account_type):
    """
    Retrieve accounts of a given type from the core_users DB.
    Returns a list of tuples (account_id, user_id, account_type).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id, user_id, account_type FROM accounts WHERE account_type = %s", (account_type,))
        return cur.fetchall()

def insert_transaction(conn, table, user_id, amount, merchant, account_type, ts, status):
    """
    Insert a transaction record into a specified transactions table.
    """
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table} (user_id, amount, merchant, account_type, timestamp, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(query, (user_id, amount, merchant, account_type, ts, status))
    conn.commit()

def insert_internal_transaction(conn, sender_id, receiver_id, amount, account_type, ts, status):
    """
    Insert a record into transactions_internal.
    """
    with conn.cursor() as cur:
        query = """
            INSERT INTO transactions_internal (sender_id, receiver_id, amount, account_type, timestamp, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(query, (sender_id, receiver_id, amount, account_type, ts, status))
    conn.commit()

def create_operations_view(conn):
    """
    Drop any existing operations view and create a new view that unifies the
    three transactions tables. For internal transactions, two rows are created:
      - one for the sender (with negative amount)
      - one for the receiver (with positive amount)
    """
    with conn.cursor() as cur:
        cur.execute("DROP VIEW IF EXISTS operations;")
        view_query = """
        CREATE VIEW operations AS
        SELECT row_number() OVER () AS id, user_id, amount, transaction_type, line_id, timestamp
        FROM (
            SELECT id, user_id, amount, 'mastercard' AS transaction_type, id AS line_id, timestamp 
            FROM transactions_mastercard
            UNION ALL
            SELECT id, user_id, amount, 'paypal' AS transaction_type, id AS line_id, timestamp 
            FROM transactions_paypal
            UNION ALL
            SELECT id, sender_id AS user_id, -amount AS amount, 'internal_send' AS transaction_type, id AS line_id, timestamp 
            FROM transactions_internal
            UNION ALL
            SELECT id, receiver_id AS user_id, amount AS amount, 'internal_receive' AS transaction_type, id AS line_id, timestamp 
            FROM transactions_internal
        ) t;
        """
        cur.execute(view_query)
    conn.commit()

def main():
    # Connect to core_users (for accounts and users data)
    users_conn = psycopg2.connect(**CORE_USERS_CONN)
    # Connect to core_transactions (for transactions and view)
    trans_conn = psycopg2.connect(**CORE_TRANS_CONN)

    merchants = ["Amazon", "Walmart", "BestBuy", "Target", "Starbucks"]

    # === Transactions for Mastercard (for credit_card and prepago accounts) ===
    # Fetch accounts from core_users
    credit_accounts = get_accounts(users_conn, 'credit_card')
    prepago_accounts = get_accounts(users_conn, 'prepago')
    mastercard_accounts = credit_accounts + prepago_accounts

    # Insert one transaction per eligible account into transactions_mastercard
    for account in mastercard_accounts:
        _, user_id, acc_type = account
        amount = round(random.uniform(10, 1000), 2)
        merchant = random.choice(merchants)
        ts = random_date()
        status = random.choice(["approved", "declined"])
        insert_transaction(trans_conn, "transactions_mastercard", user_id, amount, merchant, acc_type, ts, status)

    # === Transactions for Paypal ===
    paypal_accounts = get_accounts(users_conn, 'paypal')
    for account in paypal_accounts:
        _, user_id, acc_type = account
        amount = round(random.uniform(5, 500), 2)
        merchant = random.choice(merchants)
        ts = random_date()
        status = random.choice(["approved", "declined"])
        insert_transaction(trans_conn, "transactions_paypal", user_id, amount, merchant, acc_type, ts, status)

    # === Internal Transactions (e.g., transfers using savings accounts) ===
    savings_accounts = get_accounts(users_conn, 'savings')
    # Extract the user_ids for savings accounts (there should be 100 as per your distribution)
    savings_user_ids = [acct[1] for acct in savings_accounts]

    # Create 50 internal transactions by randomly selecting distinct sender and receiver
    for _ in range(50):
        sender = random.choice(savings_user_ids)
        receiver = random.choice(savings_user_ids)
        while receiver == sender:
            receiver = random.choice(savings_user_ids)
        amount = round(random.uniform(1, 300), 2)
        ts = random_date()
        status = random.choice(["completed", "pending"])
        # Here, the account_type is assumed to be "savings" for the transfer
        insert_internal_transaction(trans_conn, sender, receiver, amount, "savings", ts, status)

    # === Create or Replace the operations view ===
    create_operations_view(trans_conn)
    
    print("Transactions ingested and operations view created successfully.")

    # Close both connections.
    users_conn.close()
    trans_conn.close()

if __name__ == '__main__':
    main()