from py_scripts.load_data import load_data
from py_scripts.process_data import process_data, analyze_fraud_data
from py_scripts.generate_report import generate_report, save_fraud_analysis
from sqlalchemy import create_engine
import glob
import pandas as pd
import os
from datetime import datetime

def load_to_dwh(data, table_name, schema="public"):
    data.columns = [col.lower() for col in data.columns]

    engine = create_engine(f'postgresql+psycopg2://var:123@localhost:5432/varr')
    data.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)

def move_to_archive(file_path):
    archive_path = os.path.join('archive', os.path.basename(file_path) + '.backup')
    os.rename(file_path, archive_path)

def add_required_fields(df):
    df.columns = [col.lower() for col in df.columns]
    if 'amount' in df.columns:
        df['amount'] = df['amount'].str.replace(',', '.').astype(float)
    now = datetime.now()
    if 'CREATE_DT' not in df.columns:
        df['CREATE_DT'] = now
    if 'UPDATE_DT' not in df.columns:
        df['UPDATE_DT'] = now
    return df

def main():
    cards, accounts, clients = load_data()

    transactions_files = sorted(glob.glob("data/transactions_*.txt"))
    passport_blacklist_files = sorted(glob.glob("data/passport_blacklist_*.xlsx"))
    terminals_files = sorted(glob.glob("data/terminals_*.xlsx"))

    for transaction_file, passport_blacklist_file, terminals_file in zip(transactions_files, passport_blacklist_files, terminals_files):
        print(f"Обрабатываю файлы: {transaction_file}, {passport_blacklist_file}, {terminals_file}")

        transactions = pd.read_csv(transaction_file, sep=';')
        transactions.columns = [col.upper() for col in transactions.columns]
        transactions['TRANSACTION_DATE'] = pd.to_datetime(transactions['TRANSACTION_DATE'], format='%Y-%m-%d %H:%M:%S')
        transactions = add_required_fields(transactions)
        load_to_dwh(transactions, "dwh_fact_transactions")
        move_to_archive(transaction_file)

        passport_blacklist = pd.read_excel(passport_blacklist_file)
        passport_blacklist = add_required_fields(passport_blacklist)
        load_to_dwh(passport_blacklist, "dwh_fact_passport_blacklist")
        move_to_archive(passport_blacklist_file)

        terminals = pd.read_excel(terminals_file)
        terminals = add_required_fields(terminals)
        load_to_dwh(terminals, "dwh_dim_terminals")
        move_to_archive(terminals_file)

        fraud_data = process_data(transactions, passport_blacklist, terminals, cards, accounts, clients)

        generate_report(fraud_data)

        analysis_result = analyze_fraud_data(fraud_data)
        save_fraud_analysis(analysis_result, [transaction_file, passport_blacklist_file, terminals_file])

if __name__ == "__main__":
    main()
