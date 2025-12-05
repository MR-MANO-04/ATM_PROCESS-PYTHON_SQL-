import sqlite3
import random
import os
import hashlib
from datetime import datetime

DB_PATH = "ATM_database.db"

def init_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS branches (
        Branch_No INTEGER PRIMARY KEY,
        Location TEXT NOT NULL,
        Cash INTEGER NOT NULL DEFAULT 0
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customer_info (
        Acc_No INTEGER PRIMARY KEY,
        Name TEXT NOT NULL,
        Address TEXT,
        Balance INTEGER NOT NULL DEFAULT 0
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        Txn_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Timestamp TEXT NOT NULL,
        Acc_No INTEGER NOT NULL,
        Branch_No INTEGER,
        Type TEXT NOT NULL,
        Amount INTEGER NOT NULL,
        To_Acc INTEGER,
        FOREIGN KEY (Acc_No) REFERENCES customer_info(Acc_No),
        FOREIGN KEY (Branch_No) REFERENCES branches(Branch_No)
    )
    """)
    conn.commit()
    cur.execute("PRAGMA table_info(customer_info)")
    cols = [r["name"] for r in cur.fetchall()]
    if "Branch_No" not in cols:
        cur.execute("ALTER TABLE customer_info ADD COLUMN Branch_No INTEGER")
        conn.commit()
    if "PinHash" not in cols:
        cur.execute("ALTER TABLE customer_info ADD COLUMN PinHash TEXT")
        conn.commit()
    count = cur.execute("SELECT COUNT(*) AS c FROM branches").fetchone()["c"]
    if count == 0:
        locations = [
            "Chennai - Central", "Chennai - Anna Nagar", "Bengaluru - MG Road",
            "Mumbai - Andheri", "Hyderabad - Begumpet", "Delhi - Connaught Place",
            "Kolkata - Park Street", "Avadi"
        ]
        for _ in range(5):
            while True:
                bno = random.randint(1000, 9999)
                if cur.execute("SELECT 1 FROM branches WHERE Branch_No = ?", (bno,)).fetchone() is None:
                    break
            loc = random.choice(locations)
            cash = random.randint(50000, 500000)
            cur.execute("INSERT INTO branches (Branch_No, Location, Cash) VALUES (?, ?, ?)", (bno, loc, cash))
        conn.commit()
    return conn, cur

def list_branches(cursor):
    rows = cursor.execute("SELECT * FROM branches ORDER BY Branch_No").fetchall()
    if not rows:
        print("No branches available.")
        return
    print("\nBranches:")
    for r in rows:
        print(f"{r['Branch_No']} - {r['Location']} (Cash: {r['Cash']})")

def get_branch(cursor, branch_no):
    return cursor.execute("SELECT * FROM branches WHERE Branch_No = ?", (branch_no,)).fetchone()

def get_account(cursor, acc_no):
    return cursor.execute("SELECT * FROM customer_info WHERE Acc_No = ?", (acc_no,)).fetchone()

def hash_pin(pin):
    return hashlib.sha256(pin.encode("utf-8")).hexdigest()

def prompt_set_pin():
    while True:
        pin = input("Set 4-digit PIN: ").strip()
        if not (pin.isdigit() and len(pin) == 4):
            print("PIN must be 4 digits.")
            continue
        pin2 = input("Confirm PIN: ").strip()
        if pin != pin2:
            print("PINs do not match. Try again.")
            continue
        return hash_pin(pin)

def verify_pin(cursor, acc_no):
    row = get_account(cursor, acc_no)
    if row is None:
        print("Account not found.")
        return False
    stored = row["PinHash"]
    if stored is None:
        print("PIN not set for this account.")
        return False
    pin = input("Enter PIN: ").strip()
    if hash_pin(pin) == stored:
        return True
    name_upper = row["Name"].upper() if row["Name"] else "USER"
    print(f"{name_upper}: Incorrect PIN.")
    return False

def record_transaction(cursor, conn, acc_no, branch_no, ttype, amount, to_acc=None):
    ts = datetime.now().isoformat(sep=" ", timespec="seconds")
    cursor.execute("INSERT INTO transactions (Timestamp, Acc_No, Branch_No, Type, Amount, To_Acc) VALUES (?, ?, ?, ?, ?, ?)",
                   (ts, acc_no, branch_no, ttype, amount, to_acc))
    conn.commit()

def create_account(cursor, conn):
    name = input("Enter the account holder name: ").strip()
    address = input("Enter location: ").strip()
    while True:
        try:
            bal = int(input("Initial deposit amount: ").strip())
            if bal < 0:
                print("Amount cannot be negative.")
                continue
            break
        except ValueError:
            print("Enter a valid amount.")
    while True:
        try:
            acc = int(input("Generate account number: ").strip())
        except ValueError:
            print("Enter a valid number.")
            continue
        exists = cursor.execute("SELECT 1 FROM customer_info WHERE Acc_No = ?", (acc,)).fetchone()
        if exists:
            print("Account already exists. Try different.")
            continue
        branches = cursor.execute("SELECT Branch_No FROM branches").fetchall()
        branch_no = random.choice(branches)["Branch_No"] if branches else None
        pinhash = prompt_set_pin()
        cursor.execute(
            "INSERT INTO customer_info (Acc_No, Name, Address, Balance, Branch_No, PinHash) VALUES (?, ?, ?, ?, ?, ?)",
            (acc, name, address, bal, branch_no, pinhash)
        )
        conn.commit()
        name_upper = name.upper()
        if branch_no:
            b = get_branch(cursor, branch_no)
            if b:
                print(f"{name_upper}: Account {acc} created and assigned to {b['Location']}.")
            else:
                print(f"{name_upper}: Account {acc} created.")
        else:
            print(f"{name_upper}: Account {acc} created.")
        if bal > 0:
            record_transaction(cursor, conn, acc, branch_no, "deposit", bal)
        return acc

def deposit(cursor, conn, acc_no, amount):
    row = get_account(cursor, acc_no)
    if row is None:
        print("Account not found.")
        return False
    name_upper = row["Name"].upper()
    new_balance = row["Balance"] + amount
    cursor.execute("UPDATE customer_info SET Balance = ? WHERE Acc_No = ?", (new_balance, acc_no))
    branch_info = None
    if "Branch_No" in row.keys() and row["Branch_No"] is not None:
        cursor.execute("UPDATE branches SET Cash = Cash + ? WHERE Branch_No = ?", (amount, row["Branch_No"]))
        conn.commit()
        branch_info = get_branch(cursor, row["Branch_No"])
    else:
        conn.commit()
    record_transaction(cursor, conn, acc_no, row["Branch_No"] if "Branch_No" in row.keys() else None, "deposit", amount)
    if branch_info:
        print(f"{name_upper}: Deposit of {amount} successful. New balance in your account is {new_balance}.")
        print(f"Branch: {branch_info['Location']} (Branch cash: {branch_info['Cash']})")
    else:
        print(f"{name_upper}: Deposit of {amount} successful. New balance in your account is {new_balance}.")
    return True

def withdraw(cursor, conn, acc_no, amount):
    row = get_account(cursor, acc_no)
    if row is None:
        print("Account not found.")
        return False
    name_upper = row["Name"].upper()
    if not verify_pin(cursor, acc_no):
        return False
    if amount > row["Balance"]:
        print(f"{name_upper}: Insufficient account balance.")
        return False
    branch_info = None
    if "Branch_No" in row.keys() and row["Branch_No"] is not None:
        b = get_branch(cursor, row["Branch_No"])
        if b is None:
            print(f"{name_upper}: Associated branch not found.")
            return False
        if amount > b["Cash"]:
            print(f"{name_upper}: Branch has insufficient cash.")
            return False
        branch_info = b
    new_balance = row["Balance"] - amount
    cursor.execute("UPDATE customer_info SET Balance = ? WHERE Acc_No = ?", (new_balance, acc_no))
    if branch_info:
        cursor.execute("UPDATE branches SET Cash = Cash - ? WHERE Branch_No = ?", (amount, branch_info["Branch_No"]))
        conn.commit()
        branch_info = get_branch(cursor, branch_info["Branch_No"])
    else:
        conn.commit()
    record_transaction(cursor, conn, acc_no, row["Branch_No"] if "Branch_No" in row.keys() else None, "withdraw", amount)
    if branch_info:
        print(f"{name_upper}: Withdrawal of {amount} successful. New balance in your account is {new_balance}.")
        print(f"Branch: {branch_info['Location']} (Branch cash: {branch_info['Cash']})")
    else:
        print(f"{name_upper}: Withdrawal of {amount} successful. New balance in your account is {new_balance}.")
    return True

def transfer(cursor, conn, from_acc, to_acc, amount):
    row_from = get_account(cursor, from_acc)
    row_to = get_account(cursor, to_acc)
    if row_from is None:
        print("Sender account not found.")
        return False
    if row_to is None:
        print("Recipient account not found.")
        return False
    name_from = row_from["Name"].upper()
    name_to = row_to["Name"]
    if not verify_pin(cursor, from_acc):
        return False
    if amount <= 0:
        print(f"{name_from}: Transfer amount must be positive.")
        return False
    if amount > row_from["Balance"]:
        print(f"{name_from}: Insufficient account balance.")
        return False
    new_balance_from = row_from["Balance"] - amount
    new_balance_to = row_to["Balance"] + amount
    cursor.execute("UPDATE customer_info SET Balance = ? WHERE Acc_No = ?", (new_balance_from, from_acc))
    cursor.execute("UPDATE customer_info SET Balance = ? WHERE Acc_No = ?", (new_balance_to, to_acc))
    conn.commit()
    record_transaction(cursor, conn, from_acc, row_from["Branch_No"] if "Branch_No" in row_from.keys() else None, "transfer", amount, to_acc)
    record_transaction(cursor, conn, to_acc, row_to["Branch_No"] if "Branch_No" in row_to.keys() else None, "transfer_in", amount, from_acc)
    print(f"{name_from}: Transfer of {amount} to {name_to} (Acc {to_acc}) successful. New balance in your account is {new_balance_from}.")
    return True

def show_account(cursor, acc_no):
    row = get_account(cursor, acc_no)
    if row is None:
        print("Account not found.")
        return
    name_upper = row["Name"].upper()
    print(f"{name_upper}: Account details")
    print(f"Account No: {row['Acc_No']}")
    print(f"Name: {row['Name']}")
    print(f"Address: {row['Address']}")
    print(f"Balance: {row['Balance']}")
    if "Branch_No" in row.keys() and row["Branch_No"] is not None:
        b = get_branch(cursor, row["Branch_No"])
        if b:
            print(f"Branch: {b['Location']} (Branch cash: {b['Cash']})")
        else:
            print(f"Branch: {row['Branch_No']} (details not found)")

def transactions_for_account(cursor, acc_no):
    rows = cursor.execute("SELECT * FROM transactions WHERE Acc_No = ? ORDER BY Txn_ID DESC", (acc_no,)).fetchall()
    if not rows:
        print("No transactions for this account.")
        return
    for r in rows:
        print(f"{r['Txn_ID']} | {r['Timestamp']} | {r['Type']} | Amount: {r['Amount']} | Branch: {r['Branch_No']} | To_Acc: {r['To_Acc']}")

def transactions_for_branch(cursor, branch_no):
    rows = cursor.execute("SELECT * FROM transactions WHERE Branch_No = ? ORDER BY Txn_ID DESC", (branch_no,)).fetchall()
    if not rows:
        print("No transactions for this branch.")
        return
    for r in rows:
        print(f"{r['Txn_ID']} | {r['Timestamp']} | Acc: {r['Acc_No']} | {r['Type']} | Amount: {r['Amount']} | To_Acc: {r['To_Acc']}")

def branch_transaction_count(cursor, branch_no):
    cnt = cursor.execute("SELECT COUNT(*) AS c FROM transactions WHERE Branch_No = ?", (branch_no,)).fetchone()["c"]
    print(f"Branch {branch_no} transaction count: {cnt}")
    return cnt

def top_users_by_transactions(cursor, branch_no=None, limit=5):
    if branch_no is None:
        rows = cursor.execute("""
            SELECT Acc_No, COUNT(*) AS cnt FROM transactions
            GROUP BY Acc_No ORDER BY cnt DESC LIMIT ?
        """, (limit,)).fetchall()
    else:
        rows = cursor.execute("""
            SELECT Acc_No, COUNT(*) AS cnt FROM transactions
            WHERE Branch_No = ?
            GROUP BY Acc_No ORDER BY cnt DESC LIMIT ?
        """, (branch_no, limit)).fetchall()
    if not rows:
        print("No transactions found.")
        return
    print("Top users by number of transactions:")
    for r in rows:
        acc = r["Acc_No"]
        cnt = r["cnt"]
        acct = get_account(cursor, acc)
        name = acct["Name"] if acct else "Unknown"
        print(f"{name} (Acc {acc}) - {cnt} transactions")

def transactions_summary(cursor):
    rows = cursor.execute("""
        SELECT Branch_No, Type, COUNT(*) AS cnt, SUM(Amount) AS total
        FROM transactions
        GROUP BY Branch_No, Type
        ORDER BY Branch_No
    """).fetchall()
    if not rows:
        print("No transactions to summarize.")
        return
    for r in rows:
        print(f"Branch {r['Branch_No']} | Type: {r['Type']} | Count: {r['cnt']} | Total Amount: {r['total']}")

def stats_menu(cursor):
    while True:
        print("\n--- Transaction Stats Menu ---")
        print("1. Show transactions for an account")
        print("2. Show transactions for a branch")
        print("3. Show transaction count for a branch")
        print("4. Top users by transaction count (global)")
        print("5. Top users by transaction count (branch)")
        print("6. Transactions summary (by branch & type)")
        print("7. Back")
        ch = input("Choose an option: ").strip()
        if ch == "1":
            try:
                acc = int(input("Account number: ").strip())
                transactions_for_account(cursor, acc)
            except ValueError:
                print("Invalid input.")
        elif ch == "2":
            try:
                b = int(input("Branch number: ").strip())
                transactions_for_branch(cursor, b)
            except ValueError:
                print("Invalid input.")
        elif ch == "3":
            try:
                b = int(input("Branch number: ").strip())
                branch_transaction_count(cursor, b)
            except ValueError:
                print("Invalid input.")
        elif ch == "4":
            try:
                lim = int(input("Limit (default 5): ").strip() or "5")
                top_users_by_transactions(cursor, None, lim)
            except ValueError:
                print("Invalid input.")
        elif ch == "5":
            try:
                b = int(input("Branch number: ").strip())
                lim = int(input("Limit (default 5): ").strip() or "5")
                top_users_by_transactions(cursor, b, lim)
            except ValueError:
                print("Invalid input.")
        elif ch == "6":
            transactions_summary(cursor)
        elif ch == "7":
            break
        else:
            print("Invalid option.")

def main():
    conn, cursor = init_db()
    try:
        while True:
            print("\n--- ATM Menu ---")
            print("1. Create account")
            print("2. Show account")
            print("3. Deposit")
            print("4. Withdraw")
            print("5. Transfer")
            print("6. List branches")
            print("7. Transaction stats")
            print("8. Exit")
            choice = input("Choose an option: ").strip()
            if choice == "1":
                create_account(cursor, conn)
            elif choice == "2":
                try:
                    acc_no = int(input("Enter account number: ").strip())
                    show_account(cursor, acc_no)
                except ValueError:
                    print("Invalid number.")
            elif choice == "3":
                try:
                    acc_no = int(input("Account number: ").strip())
                    amt = int(input("Deposit amount: ").strip())
                    if amt > 0:
                        deposit(cursor, conn, acc_no, amt)
                    else:
                        print("Amount must be positive.")
                except ValueError:
                    print("Invalid input.")
            elif choice == "4":
                try:
                    acc_no = int(input("Account number: ").strip())
                    amt = int(input("Withdraw amount: ").strip())
                    if amt > 0:
                        withdraw(cursor, conn, acc_no, amt)
                    else:
                        print("Amount must be positive.")
                except ValueError:
                    print("Invalid input.")
            elif choice == "5":
                try:
                    from_acc = int(input("Your account number: ").strip())
                    to_acc = int(input("Recipient account number: ").strip())
                    amt = int(input("Transfer amount: ").strip())
                    if amt > 0:
                        transfer(cursor, conn, from_acc, to_acc, amt)
                    else:
                        print("Amount must be positive.")
                except ValueError:
                    print("Invalid input.")
            elif choice == "6":
                list_branches(cursor)
            elif choice == "7":
                stats_menu(cursor)
            elif choice == "8":
                print("Thank you")
                break
            else:
                print("Invalid option.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
