import os
import time
import random
import logging
import psycopg2

from contextlib import contextmanager
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from faker import Faker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BankDataGenerator:
    def __init__(self):
        self.faker = Faker()
        self.conn = None
        self.cur = None

        self.db_config = {
            'dbname': os.getenv('POSTGRES_DB', 'bank_system'),
            'user': os.getenv('POSTGRES_USER', 'bank_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'bank_password'),
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }

        self.interval = float(os.getenv('GENERATOR_INTERVAL', '1.0'))
        self.max_users = int(os.getenv('GENERATOR_MAX_USERS', '500'))
        self.max_accounts = int(os.getenv('GENERATOR_MAX_ACCOUNTS', '5000'))

        self.users = []
        self.accounts = []
        self.transaction_types = []
        self.payment_systems = []

        self.stats = {
            'users_created': 0,
            'accounts_created': 0,
            'transactions_created': 0,
            'scheduled_transfers_created': 0
        }

    @contextmanager
    def get_cursor(self):
        try:
            if not self.conn or self.conn.closed:
                self.conn = psycopg2.connect(**self.db_config)
                self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

            yield self.cur
            self.conn.commit()

        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()

    def initialize_cache(self):
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT user_id FROM users")
                self.users = [row['user_id'] for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT bank_account_id, owner_id, balance, currency, account_number
                    FROM bank_accounts
                    WHERE status = 'active'
                    """
                )
                self.accounts = cur.fetchall()

                cur.execute("SELECT type_id, name FROM transaction_type")
                self.transaction_types = cur.fetchall()

                cur.execute(
                    "SELECT payment_system_id, payment_system FROM payment_system"
                )
                self.payment_systems = cur.fetchall()

                logger.info(
                    f"Cache initialized: {len(self.users)} users, {len(self.accounts)} accounts"
                )

        except Exception as e:
            logger.error(f"Failed to initialize cache: {e}")

    def create_user(self):
        if len(self.users) >= self.max_users:
            return None

        try:
            with self.get_cursor() as cur:
                first_name = self.faker.first_name()
                last_name = self.faker.last_name()
                email = self.faker.unique.email()
                phone = self.faker.unique.phone_number()[:20]

                cur.execute("""
                    INSERT INTO users (first_name, last_name, email, phone)
                    VALUES (%s, %s, %s, %s)
                    RETURNING user_id
                """, (first_name, last_name, email, phone))

                user_id = cur.fetchone()['user_id']
                self.users.append(user_id)
                self.stats['users_created'] += 1

                logger.info(
                    f"Created new user: {first_name} {last_name} (ID: {user_id})")
                return user_id

        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            return None

    def create_account(self):
        if len(self.accounts) >= self.max_accounts or not self.users:
            return None

        try:
            with self.get_cursor() as cur:
                owner_id = random.choice(self.users)
                payment_system = random.choice(self.payment_systems)

                cur.execute(
                    """
                        SELECT last_number FROM payment_system
                        WHERE payment_system_id = %s
                        FOR UPDATE
                    """, (payment_system['payment_system_id'],)
                )

                last_number = cur.fetchone()['last_number']
                account_number = f"{payment_system['payment_system']}{last_number:06d}"

                cur.execute(
                    """
                        UPDATE payment_system
                        SET last_number = last_number + 1
                        WHERE payment_system_id = %s
                    """, (payment_system['payment_system_id'],)
                )

                balance = round(random.uniform(100, 10000), 2)
                currency = random.choice(['USD', 'EUR', 'RUB', 'GBP'])

                cur.execute(
                    """
                        INSERT INTO bank_accounts
                        (account_number, balance, payment_system, currency, owner_id)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING bank_account_id, account_number, balance, currency
                    """, (account_number, balance, payment_system['payment_system'], currency, owner_id)
                )

                account = cur.fetchone()
                self.accounts.append(account)

                cur.execute(
                    """
                        INSERT INTO user_bank_accounts (bank_account_id, user_id)
                        VALUES (%s, %s)
                    """, (account['bank_account_id'], owner_id)
                )

                if random.random() < 0.2:
                    goal_amount = round(balance * random.uniform(2, 5), 2)
                    goal_name = random.choice(
                        ['House', 'Car', 'Vacation', 'Education', 'Emergency Fund']
                    )
                    interest_rate = round(random.uniform(1.5, 7.5), 2)
                    next_interest_date = datetime.now() + timedelta(days=30)

                    cur.execute(
                        """
                            INSERT INTO saving_accounts
                            (bank_account_id, goal_amount, goal_name, min_balance, interest_rate, next_interest_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (account['bank_account_id'], goal_amount, goal_name, balance * 0.5,
                              interest_rate, next_interest_date)
                    )

                self.stats['accounts_created'] += 1
                logger.info(
                    f"Created new account: {account_number} (Balance: {balance} {currency})")
                return account

        except Exception as e:
            logger.error(f"Failed to create account: {e}")
            return None

    def generate_transaction(self):
        if len(self.accounts) < 2:
            return

        try:
            with self.get_cursor() as cur:
                if random.random() < 0.3:
                    sender_account = None
                else:
                    sender_account = random.choice(self.accounts)

                receiver_account = random.choice(self.accounts)

                while sender_account and sender_account['bank_account_id'] == receiver_account['bank_account_id']:
                    receiver_account = random.choice(self.accounts)

                max_amount = float(sender_account['balance']) if sender_account else round(random.uniform(10, 1000), 2)
                amount = round(random.uniform(1, max_amount), 2) + 100

                transaction_type = random.choice(self.transaction_types)

                if not sender_account:
                    transaction_type = next(
                        (t for t in self.transaction_types if t['name'] == 'deposit'), transaction_type)

                descriptions = {
                    'transfer': ['Money transfer', 'Funds transfer', 'Payment'],
                    'deposit': ['ATM deposit', 'Bank deposit', 'Cash deposit'],
                    'withdrawal': ['ATM withdrawal', 'Cash withdrawal'],
                    'payment': ['Online payment', 'Store payment', 'Service payment'],
                    'refund': ['Purchase refund', 'Service refund']
                }

                description = random.choice(descriptions.get(transaction_type['name'], [
                                            'Transaction'])) + f" #{random.randint(1000, 9999)}"

                converted_amount = amount
                if sender_account and sender_account.get('currency') != receiver_account.get('currency'):
                    rates = {'USD': 1.0, 'EUR': 0.85, 'RUB': 75.0, 'GBP': 0.73}
                    from_rate = rates.get(sender_account.get('currency', 'USD'), 1.0)
                    to_rate = rates.get(receiver_account.get('currency', 'USD'), 1.0)
                    converted_amount = round(amount * from_rate / to_rate, 2)

                if sender_account:
                    if float(sender_account['balance']) >= amount:
                        cur.execute(
                            """
                            UPDATE bank_accounts
                            SET balance = balance - %s
                            WHERE bank_account_id = %s
                            RETURNING balance
                            """,
                            (amount, sender_account['bank_account_id'])
                        )
                        updated_sender_balance = cur.fetchone()['balance']

                        for acc in self.accounts:
                            if acc['bank_account_id'] == sender_account['bank_account_id']:
                                acc['balance'] = updated_sender_balance
                                break
                    else:
                        cur.execute(
                            """
                            INSERT INTO transactions
                            (sender_account_id, receiver_account_id, amount, converted_amount,
                            description, type_id, status)
                            VALUES (%s, %s, %s, %s, %s, %s, 'failed')
                            RETURNING transaction_id, created_at
                            """,
                            (
                                sender_account['bank_account_id'],
                                receiver_account['bank_account_id'],
                                amount,
                                converted_amount,
                                description,
                                transaction_type['type_id']
                            )
                        )
                        logger.warning(f"Insufficient funds for transaction from {sender_account['account_number']}")
                        return

                cur.execute(
                    """
                    UPDATE bank_accounts
                    SET balance = balance + %s
                    WHERE bank_account_id = %s
                    RETURNING balance
                    """,
                    (converted_amount, receiver_account['bank_account_id'])
                )
                updated_receiver_balance = cur.fetchone()['balance']

                for acc in self.accounts:
                    if acc['bank_account_id'] == receiver_account['bank_account_id']:
                        acc['balance'] = updated_receiver_balance
                        break

                cur.execute(
                    """
                    INSERT INTO transactions
                    (sender_account_id, receiver_account_id, amount, converted_amount,
                    description, type_id, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'completed')
                    RETURNING transaction_id, created_at
                    """,
                    (
                        sender_account['bank_account_id'] if sender_account else None,
                        receiver_account['bank_account_id'],
                        amount,
                        converted_amount,
                        description,
                        transaction_type['type_id']
                    )
                )

                cur.fetchone()
                self.stats['transactions_created'] += 1

                if random.random() < 0.05 and sender_account:
                    self.create_scheduled_transfer(
                        sender_account, receiver_account, amount
                    )

                logger.info(
                    f"Created transaction: {amount} {sender_account.get('currency', 'USD') if sender_account else 'USD'} "
                    f"from {sender_account.get('account_number', 'DEPOSIT') if sender_account else 'DEPOSIT'} "
                    f"to {receiver_account['account_number']} ({converted_amount} {receiver_account.get('currency', 'USD')})"
                )

        except Exception as e:
            logger.error(f"Failed to create transaction: {e}")

    def create_scheduled_transfer(self, sender_account, receiver_account, amount):
        try:
            with self.get_cursor() as cur:
                frequency = random.choice(['weekly', 'monthly'])
                start_date = datetime.now().date()

                if frequency == 'weekly':
                    next_date = start_date + timedelta(days=7)
                    end_date = start_date + \
                        timedelta(days=random.randint(30, 180))
                else:
                    next_date = start_date + timedelta(days=30)
                    end_date = start_date + \
                        timedelta(days=random.randint(90, 365))

                description = f"Scheduled {frequency} transfer #{random.randint(1000, 9999)}"

                cur.execute("""
                    INSERT INTO scheduled_transfers
                    (sender_account_id, receiver_account_id, amount, description,
                     frequency, start_date, next_occurrence_date, end_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING scheduled_transfer_id
                """, (
                    sender_account['bank_account_id'],
                    receiver_account['bank_account_id'],
                    amount,
                    description,
                    frequency,
                    start_date,
                    next_date,
                    end_date
                ))

                self.stats['scheduled_transfers_created'] += 1
                logger.info(f"Created scheduled {frequency} transfer")

        except Exception as e:
            logger.error(f"Failed to create scheduled transfer: {e}")

    def update_achievements(self):
        try:
            with self.get_cursor() as cur:
                cur.execute("""
                    SELECT u.user_id, COUNT(t.transaction_id) as tx_count,
                        SUM(CASE WHEN t.receiver_account_id = ba.bank_account_id THEN t.amount ELSE 0 END)
                                                                                        as total_received
                    FROM users u
                    LEFT JOIN bank_accounts ba ON u.user_id = ba.owner_id
                    LEFT JOIN transactions t ON ba.bank_account_id = t.receiver_account_id
                    GROUP BY u.user_id
                    HAVING COUNT(t.transaction_id) > 0
                """)

                users_stats = cur.fetchall()

                for user in users_stats:
                    if user['tx_count'] >= 1:
                        cur.execute("""
                            INSERT INTO user_achievements (user_id, achievement_id)
                            SELECT %s, achievement_id FROM achievements WHERE name = 'First Transaction'
                            ON CONFLICT (user_id, achievement_id) DO NOTHING
                        """, (user['user_id'],))

                    if user['tx_count'] >= 50:
                        cur.execute("""
                            INSERT INTO user_achievements (user_id, achievement_id)
                            SELECT %s, achievement_id FROM achievements WHERE name = 'Active User'
                            ON CONFLICT (user_id, achievement_id) DO NOTHING
                        """, (user['user_id'],))

                    if user['total_received'] >= 10000:
                        cur.execute("""
                            INSERT INTO user_achievements (user_id, achievement_id)
                            SELECT %s, achievement_id FROM achievements WHERE name = 'Savings Master'
                            ON CONFLICT (user_id, achievement_id) DO NOTHING
                        """, (user['user_id'],))

        except Exception as e:
            logger.error(f"Failed to update achievements: {e}")

    def print_stats(self):
        logger.info("=" * 50)
        logger.info("BANK DATA GENERATOR STATISTICS")
        logger.info("=" * 50)

        for key, value in self.stats.items():
            logger.info(f"{key.replace('_', ' ').title()}: {value}")

        logger.info(f"Active users in cache: {len(self.users)}")
        logger.info(f"Active accounts in cache: {len(self.accounts)}")
        logger.info("=" * 50)

    def run(self):
        logger.info("Starting Bank Data Generator...")

        self.initialize_cache()

        for _ in range(10):
            self.create_user()

        for _ in range(20):
            self.create_account()

        counter = 0
        try:
            while True:
                if counter % 10:
                    self.create_user()

                if counter % 20:
                    self.create_account()

                self.generate_transaction()
                self.update_achievements()

                if counter % 100:
                    self.print_stats()

                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Stopping generator...")
            self.print_stats()
        except Exception as e:
            logger.error(f"Generator error: {e}")


if __name__ == "__main__":
    generator = BankDataGenerator()
    generator.run()
