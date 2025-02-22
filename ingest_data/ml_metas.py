import psycopg2
import random
from datetime import datetime, timedelta

def random_active_dates():
    """
    Generate dates for an active campaign:
      - start_date: between today and 10 days ago
      - end_date: between tomorrow and 10 days in the future
    """
    start_date = datetime.now().date() - timedelta(days=random.randint(0, 10))
    end_date = datetime.now().date() + timedelta(days=random.randint(1, 10))
    return start_date, end_date

def random_expired_dates():
    """
    Generate dates for an expired campaign:
      - start_date: between 20 and 100 days ago
      - end_date: between start_date and yesterday
    """
    days_ago = random.randint(20, 100)
    start_date = datetime.now().date() - timedelta(days=days_ago)
    # Ensure the campaign ended before today
    end_offset = random.randint(1, days_ago - 1)
    end_date = start_date + timedelta(days=end_offset)
    if end_date >= datetime.now().date():
        end_date = datetime.now().date() - timedelta(days=1)
    return start_date, end_date

def random_upcoming_dates():
    """
    Generate dates for an upcoming campaign:
      - start_date: between tomorrow and 100 days in the future
      - end_date: start_date plus 1 to 30 days
    """
    start_date = datetime.now().date() + timedelta(days=random.randint(1, 100))
    end_date = start_date + timedelta(days=random.randint(1, 30))
    return start_date, end_date

def main():
    # Connection to DB_CORE_USERS (for fetching credit_card users)
    conn_users = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="core_users",
        user="postgres",
        password="postgres"
    )
    cur_users = conn_users.cursor()

    # Connection to DB_ML_METAS (for inserting campaigns and user_campaigns)
    conn_metas = psycopg2.connect(
        host="localhost",
        port=5434,  # as per docker-compose, DB_ML_METAS is exposed on port 5434
        dbname="ml_metas",
        user="postgres",
        password="postgres"
    )
    cur_metas = conn_metas.cursor()

    # -------------------------------------------------------------------
    # Fetch all users with a credit_card account from DB_CORE_USERS.
    # -------------------------------------------------------------------
    cur_users.execute("""
        SELECT DISTINCT user_id 
        FROM accounts 
        WHERE account_type = 'credit_card';
    """)
    credit_card_users = cur_users.fetchall()  # list of tuples (user_id,)
    credit_card_user_ids = [row[0] for row in credit_card_users]
    print(f"Found {len(credit_card_user_ids)} users with credit_card accounts.")

    # -------------------------------------------------------------------
    # Truncate campaigns and user_campaigns tables in DB_ML_METAS for a clean slate.
    # -------------------------------------------------------------------
    cur_metas.execute("TRUNCATE TABLE user_campaigns, campaigns RESTART IDENTITY CASCADE;")
    conn_metas.commit()

    merchants = ["Amazon", "Walmart", "BestBuy", "Target", "Starbucks"]

    total_campaigns = 200
    active_campaign_count = 5  # exactly 5 campaigns are active now

    for i in range(total_campaigns):
        campaign_name = f"Campaign {i+1}"
        goal = round(random.uniform(1000, 100000), 2)
        cashback_percentage = round(random.uniform(1, 20), 2)

        # For the first few campaigns, force them to be active.
        if i < active_campaign_count:
            start_date, end_date = random_active_dates()
        else:
            # Randomly choose between an expired or upcoming campaign (50/50 chance)
            if random.random() < 0.5:
                start_date, end_date = random_expired_dates()
            else:
                start_date, end_date = random_upcoming_dates()

        # Insert campaign into DB_ML_METAS.
        cur_metas.execute(
            """
            INSERT INTO campaigns (name, goal, cashback_percentage, start_date, end_date)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (campaign_name, goal, cashback_percentage, start_date, end_date)
        )
        campaign_id = cur_metas.fetchone()[0]

        # For each credit_card user, assign the campaign.
        for user_id in credit_card_user_ids:
            # Randomly select one merchant for this campaign assignment.
            merchant_list = [random.choice(merchants)]
            cur_metas.execute(
                """
                INSERT INTO user_campaigns (user_id, campaign_id, merchant_list, start_date, end_date)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (user_id, campaign_id, merchant_list, start_date, end_date)
            )
        # Commit after processing each campaign.
        conn_metas.commit()

    print("200 campaigns created (with 5 active) and assigned to all credit_card users.")

    # Close all cursors and connections.
    cur_users.close()
    conn_users.close()
    cur_metas.close()
    conn_metas.close()

if __name__ == '__main__':
    main()