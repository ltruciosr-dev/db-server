import psycopg2
import random
from datetime import datetime, timedelta

# List of merchants to choose from
MERCHANTS = ["Amazon", "Walmart", "BestBuy", "Target", "Starbucks"]

def random_date_between(start, end):
    """Return a random date between start and end (both datetime.date objects)."""
    delta_days = (end - start).days
    random_days = random.randint(0, delta_days)
    return start + timedelta(days=random_days)

def main():
    # Connection parameters for DB_CORE_USERS (where accounts live)
    conn_users = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="core_users",
        user="postgres",
        password="postgres"
    )
    cur_users = conn_users.cursor()
    
    # Connection parameters for DB_ML_METAS (where campaigns will be inserted)
    conn_metas = psycopg2.connect(
        host="localhost",
        port=5434,  # as defined in your docker-compose
        dbname="ml_metas",
        user="postgres",
        password="postgres"
    )
    cur_metas = conn_metas.cursor()
    
    # ------------------------------------------------------------------
    # Retrieve all credit_card accounts with their activation dates.
    # We'll assume the accounts table contains a non-null activated_at column.
    # ------------------------------------------------------------------
    cur_users.execute("""
        SELECT user_id, activated_at
        FROM accounts
        WHERE account_type = 'credit_card';
    """)
    accounts = cur_users.fetchall()  # list of tuples (user_id, activated_at)
    
    # Create a dictionary mapping user_id to activation_date (as a date)
    credit_card_users = {user_id: activated_at.date() for user_id, activated_at in accounts}
    print(f"Found {len(credit_card_users)} credit_card users.")
    
    # ------------------------------------------------------------------
    # For a clean slate in DB_ML_METAS, truncate the campaigns and user_campaigns tables.
    # ------------------------------------------------------------------
    cur_metas.execute("TRUNCATE TABLE user_campaigns, campaigns RESTART IDENTITY CASCADE;")
    conn_metas.commit()
    
    # Set up the 2-year window for campaign start dates.
    today = datetime.now().date()
    two_years_ago = today - timedelta(days=365 * 2)
    
    total_campaigns = 50
    for i in range(total_campaigns):
        campaign_name = f"Campaign {i+1}"
        # Random goal and cashback percentage.
        goal = round(random.uniform(1000, 50000), 2)
        cashback_percentage = round(random.uniform(1, 20), 2)
        
        # Choose a campaign start_date randomly from the past 2 years.
        campaign_start = random_date_between(two_years_ago, today)
        # Campaign duration: random between 14 and 90 days.
        duration_days = random.randint(14, 90)
        campaign_end = campaign_start + timedelta(days=duration_days)
        
        # Insert the campaign record.
        cur_metas.execute(
            """
            INSERT INTO campaigns (name, goal, cashback_percentage, start_date, end_date)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (campaign_name, goal, cashback_percentage, campaign_start, campaign_end)
        )
        campaign_id = cur_metas.fetchone()[0]
        conn_metas.commit()
        
        # For each credit_card user, assign the campaign if the account was activated before campaign start.
        for user_id, activated_date in credit_card_users.items():
            if activated_date < campaign_start:
                # Choose one random merchant for the campaign assignment.
                merchant_list = [random.choice(MERCHANTS)]
                cur_metas.execute(
                    """
                    INSERT INTO user_campaigns (user_id, campaign_id, merchant_list, start_date, end_date)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (user_id, campaign_id, merchant_list, campaign_start, campaign_end)
                )
        conn_metas.commit()
    
    print(f"Inserted {total_campaigns} campaigns and assigned eligible credit_card users to each campaign.")
    
    # Close all cursors and connections.
    cur_users.close()
    conn_users.close()
    cur_metas.close()
    conn_metas.close()

if __name__ == '__main__':
    main()