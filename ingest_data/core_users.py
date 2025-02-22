import psycopg2
import random
import string
from datetime import datetime, timedelta

# Lists of common Chilean first names and surnames.
male_first_names = [
    "Juan", "Carlos", "Pedro", "Miguel", "Andrés",
    "Jorge", "Ricardo", "Francisco", "Sebastián", "Diego"
]
female_first_names = [
    "María", "Camila", "Sofía", "Isabella", "Valentina",
    "Fernanda", "Catalina", "Gabriela", "Antonella", "Julieta"
]
last_names = [
    "González", "Rodríguez", "Pérez", "Martínez", "Sánchez",
    "Ramírez", "Torres", "Flores", "Díaz", "Reyes"
]

# Common email domains.
email_domains = ["gmail.com", "hotmail.cl", "yahoo.com", "outlook.com"]

# Chilean regions and cities (simplified).
chile_locations = [
    ("Región Metropolitana", "Santiago"),
    ("Región de Valparaíso", "Valparaíso"),
    ("Región del Biobío", "Concepción"),
    ("Región de Coquimbo", "La Serena"),
    ("Región de Antofagasta", "Antofagasta"),
    ("Región de La Araucanía", "Temuco"),
    ("Región de O'Higgins", "Rancagua"),
    ("Región de Los Lagos", "Puerto Montt"),
    ("Región de Magallanes", "Punta Arenas"),
    ("Región de Tarapacá", "Iquique")
]

def random_activation_date():
    """Return a datetime up to 100 days ago."""
    days_ago = random.randint(0, 100)
    return datetime.now() - timedelta(days=days_ago)

def random_phone():
    """Generate a realistic Chilean mobile number in format: +56 9 XXXX XXXX"""
    number = ''.join(random.choices(string.digits, k=8))
    return f"+56 9 {number[:4]} {number[4:]}"

def generate_full_name(gender):
    """Generate a realistic full name based on gender."""
    if gender == "Male":
        first = random.choice(male_first_names)
    elif gender == "Female":
        first = random.choice(female_first_names)
    else:  # For 'Other', pick from both lists
        first = random.choice(male_first_names + female_first_names)
    last = random.choice(last_names)
    return f"{first} {last}"

def generate_email(full_name):
    """Generate an email from the full name with a random domain."""
    # Remove accents and lower the text (for simplicity, we only replace a few common ones)
    normalized = (full_name
                  .replace("á", "a")
                  .replace("é", "e")
                  .replace("í", "i")
                  .replace("ó", "o")
                  .replace("ú", "u")
                  .replace("Á", "A")
                  .replace("É", "E")
                  .replace("Í", "I")
                  .replace("Ó", "O")
                  .replace("Ú", "U"))
    username = ".".join(normalized.split()).lower()
    # Append a random two-digit number to help ensure uniqueness
    suffix = random.randint(10, 99)
    domain = random.choice(email_domains)
    return f"{username}{suffix}@{domain}"

def main():
    # Generate user data for 100 Chilean citizens.
    users_data = []
    for _ in range(100):
        # Randomly select a gender.
        gender = random.choice(["Male", "Female", "Other"])
        full_name = generate_full_name(gender)
        email = generate_email(full_name)
        phone = random_phone()
        age = random.randint(18, 80)
        income_level = random.choice(["Low", "Medium", "High"])
        region, city = random.choice(chile_locations)
        
        users_data.append({
            "name": full_name,
            "email": email,
            "phone": phone,
            "gender": gender,
            "age": age,
            "income_level": income_level,
            "region": region,
            "city": city
        })

    # Connection parameters – adjust these as needed.
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="core_users",  # Adjust if your DB name is different.
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    # -------------------------------
    # TRUNCATE existing data in dependent order.
    # -------------------------------
    cur.execute("""
        TRUNCATE TABLE card_info, accounts, user_status, onboarding, demographics, users 
        RESTART IDENTITY CASCADE;
    """)
    conn.commit()

    # -------------------------------
    # Insert Users and their Demographics
    # -------------------------------
    for idx, user in enumerate(users_data, start=1):
        # Insert into users table.
        cur.execute(
            "INSERT INTO users (name, email, phone) VALUES (%s, %s, %s);",
            (user["name"], user["email"], user["phone"])
        )
        # Insert into demographics table.
        cur.execute(
            """
            INSERT INTO demographics (user_id, age, gender, income_level, country, state, city)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (idx, user["age"], user["gender"], user["income_level"], "Chile", user["region"], user["city"])
        )
    conn.commit()

    # -------------------------------
    # Insert Onboarding for each User
    # -------------------------------
    for user_id in range(1, 101):
        step = random.randint(1, 5)
        status = 'completed' if random.random() > 0.5 else 'in_progress'
        completed_at = datetime.now() if random.random() > 0.5 else None
        cur.execute(
            """
            INSERT INTO onboarding (user_id, step, status, completed_at)
            VALUES (%s, %s, %s, %s);
            """,
            (user_id, step, status, completed_at)
        )
    conn.commit()

    # -------------------------------
    # Insert User Status for each User
    # -------------------------------
    for user_id in range(1, 101):
        status = 'active'
        days_ago = random.randint(0, 30)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        last_active_at = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        cur.execute(
            """
            INSERT INTO user_status (user_id, status, last_active_at)
            VALUES (%s, %s, %s);
            """,
            (user_id, status, last_active_at)
        )
    conn.commit()

    # -------------------------------
    # Insert Accounts with required distribution
    # -------------------------------
    # Insert 50 accounts of type 'credit_card'
    for _ in range(50):
        user_id = random.randint(1, 100)
        account_type = 'credit_card'
        balance = round(random.uniform(0, 100000), 2)
        currency = 'CLP'
        activated_at = random_activation_date()
        cur.execute(
            """
            INSERT INTO accounts (user_id, account_type, balance, currency, activated_at)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (user_id, account_type, balance, currency, activated_at)
        )
    conn.commit()

    # Insert 100 accounts of type 'prepago'
    for _ in range(100):
        user_id = random.randint(1, 100)
        account_type = 'prepago'
        balance = round(random.uniform(0, 100000), 2)
        currency = 'CLP'
        activated_at = random_activation_date()
        cur.execute(
            """
            INSERT INTO accounts (user_id, account_type, balance, currency, activated_at)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (user_id, account_type, balance, currency, activated_at)
        )
    conn.commit()

    # Insert 100 accounts of type 'savings'
    for _ in range(100):
        user_id = random.randint(1, 100)
        account_type = 'savings'
        balance = round(random.uniform(0, 100000), 2)
        currency = 'CLP'
        activated_at = random_activation_date()
        cur.execute(
            """
            INSERT INTO accounts (user_id, account_type, balance, currency, activated_at)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (user_id, account_type, balance, currency, activated_at)
        )
    conn.commit()

    # Insert 10 accounts of type 'paypal'
    for _ in range(10):
        user_id = random.randint(1, 100)
        account_type = 'paypal'
        balance = round(random.uniform(0, 100000), 2)
        currency = 'CLP'
        activated_at = random_activation_date()
        cur.execute(
            """
            INSERT INTO accounts (user_id, account_type, balance, currency, activated_at)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (user_id, account_type, balance, currency, activated_at)
        )
    conn.commit()

    # -------------------------------
    # Insert Card_Info for accounts of type 'prepago' and 'credit_card'
    # Mask the card number with '***'
    # -------------------------------
    cur.execute("""
        SELECT id FROM accounts 
        WHERE account_type IN ('prepago', 'credit_card');
    """)
    eligible_accounts = cur.fetchall()  # List of tuples (id,)

    for (account_id,) in eligible_accounts:
        card_number = '***'
        # Generate an expiration date between 1 and 5 years in the future.
        years_to_add = random.randint(1, 5)
        expiration_date = (datetime.now() + timedelta(days=365 * years_to_add)).date()
        status = 'active'
        cur.execute(
            """
            INSERT INTO card_info (account_id, card_number, expiration_date, status)
            VALUES (%s, %s, %s, %s);
            """,
            (account_id, card_number, expiration_date, status)
        )
    conn.commit()

    # Close cursor and connection.
    cur.close()
    conn.close()
    print("Data ingestion complete.")

if __name__ == '__main__':
    main()