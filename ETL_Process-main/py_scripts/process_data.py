from datetime import datetime, timedelta
import pandas as pd


def analyze_fraud_data(fraud_data):
    analysis = {}

    fraud_data.columns = [col.lower() for col in fraud_data.columns]

    analysis["total_fraud_cases"] = len(fraud_data)

    # Самый частый тип мошенничества
    most_common_fraud = fraud_data["event_type"].value_counts().idxmax()
    analysis["most_common_fraud"] = most_common_fraud

    # Топ-3 самых активных мошенников
    top_fraudsters = fraud_data["passport"].value_counts().head(3).index.tolist()
    analysis["top_fraudsters"] = top_fraudsters

    # В какое время суток чаще всего происходят мошеннические операции
    fraud_data["hour"] = fraud_data["event_dt"].dt.hour
    time_periods = {
        "Ночь (00:00-06:00)": fraud_data[(fraud_data["hour"] >= 0) & (fraud_data["hour"] < 6)].shape[0],
        "Утро (06:00-12:00)": fraud_data[(fraud_data["hour"] >= 6) & (fraud_data["hour"] < 12)].shape[0],
        "День (12:00-18:00)": fraud_data[(fraud_data["hour"] >= 12) & (fraud_data["hour"] < 18)].shape[0],
        "Вечер (18:00-00:00)": fraud_data[(fraud_data["hour"] >= 18) & (fraud_data["hour"] < 24)].shape[0],
    }
    analysis["most_frequent_time"] = max(time_periods, key=time_periods.get)

    return analysis


def process_data(transactions, passport_blacklist, terminals, cards, accounts, clients):
    """
    Обнаружение мошеннических операций по 4-м условиям и формирование отчёта.
    """
    fraud_cases = []

    passport_blacklist['date'] = pd.to_datetime(passport_blacklist['date'])
    clients['date_of_birth'] = pd.to_datetime(clients['date_of_birth'])
    clients['passport_valid_to'] = pd.to_datetime(clients['passport_valid_to'], errors='coerce')
    accounts['valid_to'] = pd.to_datetime(accounts['valid_to'], errors='coerce')
    transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'], errors='coerce')
    terminals['create_dt'] = pd.to_datetime(terminals['create_dt'], errors='coerce')


    transactions.columns = [col.lower() for col in transactions.columns]
    passport_blacklist.columns = [col.lower() for col in passport_blacklist.columns]
    terminals.columns = [col.lower() for col in terminals.columns]
    cards.columns = [col.lower() for col in cards.columns]
    accounts.columns = [col.lower() for col in accounts.columns]
    clients.columns = [col.lower() for col in clients.columns]


    cards = cards.rename(columns={'card_num': 'card_num', 'account': 'account'})
    merged_cards = pd.merge(cards, accounts[['account', 'valid_to']], on='account', how='left')
    transactions = pd.merge(transactions, merged_cards[['card_num', 'account', 'valid_to']], on='card_num', how='left')


    accounts_clients = accounts.merge(clients, left_on='client', right_on='client_id', how='left')
    merged_cards = pd.merge(cards, accounts_clients[
        ['account', 'client_id', 'last_name', 'first_name', 'patronymic', 'passport_num', 'passport_valid_to',
         'phone']], on='account', how='left')
    transactions = pd.merge(transactions, merged_cards[
        ['card_num', 'client_id', 'last_name', 'first_name', 'patronymic', 'passport_num', 'passport_valid_to',
         'phone']], on='card_num', how='left')

    # Добавляем информацию о терминале (город) в операции
    terminals = terminals.rename(columns={'terminal_id': 'terminal'})
    transactions = pd.merge(transactions, terminals[['terminal', 'terminal_city']], on='terminal', how='left')

    # 1. Операция при просроченном или заблокированном паспорте.
    for idx, tr in transactions.iterrows():
        tr_date = tr['transaction_date']
        passport_valid_to = tr['passport_valid_to']
        is_expired = pd.notnull(passport_valid_to) and passport_valid_to < tr_date

        passport_in_blacklist = False
        if pd.notnull(tr['passport_num']):
            passport_in_blacklist = passport_blacklist['passport'].astype(str).str.strip().eq(str(
                tr['passport_num']).strip()).any()

        if is_expired or passport_in_blacklist:
            fraud_cases.append({
                "event_dt": tr_date,
                "passport": tr['passport_num'],
                "fio": f"{tr['last_name']} {tr['first_name']} {tr['patronymic']}",
                "phone": tr['phone'],
                "event_type": "Паспорт просрочен или заблокирован",
                "report_dt": datetime.now()
            })

    # 2. Операция при недействующем договоре.
    for idx, tr in transactions.iterrows():
        valid_to = tr['valid_to']
        tr_date = tr['transaction_date']
        if pd.notnull(valid_to) and tr_date > valid_to:
            fraud_cases.append({
                "event_dt": tr_date,
                "passport": tr['passport_num'],
                "fio": f"{tr['last_name']} {tr['first_name']} {tr['patronymic']}",
                "phone": tr['phone'],
                "event_type": "Операция по недействующему договору",
                "report_dt": datetime.now()
            })

    # 3. Операции в разных городах в течение одного часа.
    transactions_sorted = transactions.sort_values(by=['client_id', 'transaction_date'])
    for client_id, group in transactions_sorted.groupby('client_id'):
        group = group.reset_index(drop=True)
        for i in range(len(group)):
            base_time = group.loc[i, 'transaction_date']
            base_city = group.loc[i, 'terminal_city']
            mask = (group['transaction_date'] > base_time) & (
                        group['transaction_date'] <= base_time + timedelta(hours=1)) \
                   & (group['terminal_city']!=base_city)
            if mask.any():
                row = group.loc[i]
                fraud_cases.append({
                    "event_dt": row['transaction_date'],
                    "passport": row['passport_num'],
                    "fio": f"{row['last_name']} {row['first_name']} {row['patronymic']}",
                    "phone": row['phone'],
                    "event_type": "Операции в разных городах в течение 1 часа",
                    "report_dt": datetime.now()
                })

    # 4. Попытка подбора суммы.
    transactions_sorted = transactions.sort_values(by=['client_id', 'transaction_date'])
    for client_id, group in transactions_sorted.groupby('client_id'):
        group = group.reset_index(drop=True)
        n = len(group)
        i = 0
        while i < n:
            chain = [group.loc[i]]
            j = i + 1
            while j < n:
                if group.loc[j, 'transaction_date'] - chain[0]['transaction_date'] > timedelta(minutes=20):
                    break
                if group.loc[j, 'amount'] < chain[-1]['amount']:
                    chain.append(group.loc[j])
                else:
                    break
                j += 1

            if len(chain) >= 4:
                results = [op['oper_result'] for op in chain]
                if all(r=='FAIL' for r in results[:-1]) and results[-1]=='SUCCESS':
                    op = chain[-1]
                    fraud_cases.append({
                        "event_dt": op['transaction_date'],
                        "passport": op['passport_num'],
                        "fio": f"{op['last_name']} {op['first_name']} {op['patronymic']}",
                        "phone": op['phone'],
                        "event_type": "Подбор суммы",
                        "report_dt": datetime.now()
                    })
                    i = j
                    continue
            i += 1

    return pd.DataFrame(fraud_cases)
