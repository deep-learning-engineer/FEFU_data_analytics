CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE achievements (
    achievement_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    condition TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_achievements (
    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
    achievement_id INT REFERENCES achievements(achievement_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, achievement_id)
);

CREATE TABLE payment_system (
    payment_system_id SERIAL PRIMARY KEY,
    payment_system VARCHAR(50) UNIQUE NOT NULL,
    last_number INT DEFAULT 0
);

CREATE TABLE bank_accounts (
    bank_account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    balance DECIMAL(15,2) DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(7) DEFAULT 'active' CHECK (status IN ('active', 'blocked', 'closed')),
    payment_system VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'USD',
    owner_id INT REFERENCES users(user_id) ON DELETE SET NULL
);

CREATE TABLE saving_accounts (
    bank_account_id INT PRIMARY KEY REFERENCES bank_accounts(bank_account_id) ON DELETE CASCADE,
    goal_amount DECIMAL(15,2) NOT NULL,
    goal_name VARCHAR(100) NOT NULL,
    min_balance DECIMAL(15,2) DEFAULT 0.0,
    interest_rate DECIMAL(5,2) DEFAULT 0.0,
    interest_period INT DEFAULT 30,
    next_interest_date DATE
);

CREATE TABLE user_bank_accounts (
    bank_account_id INT REFERENCES bank_accounts(bank_account_id) ON DELETE CASCADE,
    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
    payment_system_counter INT DEFAULT 0,
    PRIMARY KEY (bank_account_id, user_id)
);

CREATE TABLE transaction_type (
    type_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('pending', 'completed', 'failed', 'cancelled')),
    sender_account_id INT REFERENCES bank_accounts(bank_account_id),
    receiver_account_id INT REFERENCES bank_accounts(bank_account_id),
    amount DECIMAL(15,2) NOT NULL CHECK (amount > 0),
    converted_amount DECIMAL(15,2),
    description TEXT,
    type_id INT REFERENCES transaction_type(type_id)
);

CREATE TABLE bank_accounts_invitations (
    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
    bank_account_id INT REFERENCES bank_accounts(bank_account_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, bank_account_id)
);

CREATE TABLE scheduled_transfers (
    scheduled_transfer_id SERIAL PRIMARY KEY,
    sender_account_id INT REFERENCES bank_accounts(bank_account_id) ON DELETE CASCADE,
    receiver_account_id INT REFERENCES bank_accounts(bank_account_id),
    amount DECIMAL(15,2) NOT NULL CHECK (amount > 0),
    description TEXT,
    frequency VARCHAR(20) CHECK (frequency IN ('daily', 'weekly', 'monthly', 'yearly')),
    start_date DATE NOT NULL,
    next_occurrence_date DATE NOT NULL,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION prevent_transaction_deletion()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Удаление транзакций запрещено!';
    RETURN NULL;
END;
$$ language 'plpgsql';

CREATE TRIGGER prevent_transaction_delete BEFORE DELETE ON transactions
FOR EACH ROW EXECUTE FUNCTION prevent_transaction_deletion();

INSERT INTO transaction_type (name) VALUES 
('deposit'),
('withdrawal'),
('payment'),
('refund');

INSERT INTO payment_system (payment_system) VALUES 
('Visa'),
('MasterCard'),
('Mir'),
('UnionPay');

INSERT INTO achievements (name, condition) VALUES
('First Transaction', 'Совершить первую транзакцию'),
('Savings Master', 'Накопить более 10000'),
('Active User', 'Совершить 50 транзакций'),
('Referral King', 'Пригласить 5 друзей');