import pandas as pd
from py_scripts.scripts import add_log


# Создание представления с необходимой для отчета информацией
def create_v_report_info_day(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE VIEW if not exists v_report_info_day as
        SELECT DISTINCT
            t1.trans_id,
            t1.trans_date,
            t1.oper_result,
            t1.card_num,
            t1.amt,
            t1.opertype,
            t3.client,
            t3.valid_to,
            t3.account,
            t4.passport_num,
            t4.passport_valid_to,
            t4.last_name||' '||t4.first_name||' '||t4.patronymic as fio,
            t4.phone,
            t5.terminal_city
        FROM DWH_FACT_transactions t1
        LEFT JOIN DWH_DIM_CARDS_HIST t2
        ON t1.card_num = t2.card_num
        LEFT JOIN DWH_DIM_ACCOUNTS_HIST t3
        ON t2.account = t3.account
        LEFT JOIN DWH_DIM_CLIENTS_HIST t4
        ON t3.client = t4.client_id
        LEFT JOIN DWH_DIM_terminals_HIST t5 on t1.terminal = t5.terminal_id
        WHERE t1.trans_date between 
            (select max(date(trans_date)) FROM DWH_FACT_transactions) 
            and
            (SELECT date(max(date(trans_date)), '+1 day') FROM DWH_FACT_transactions)
        ''')
# /Создание представления с необходимой для отчета информацией


# Совершение операции при просроченном или заблокированном паспорте
def passport_error(conn):
    passport_fraud=pd.read_sql('''
        SELECT
            min(trans_date) as event_dt,
            passport_num as passport,
            fio,
            phone,
            'PASSPORT' as event_type
        FROM v_report_info_day
        WHERE trans_date > (date(passport_valid_to, '+1 day'))
        or passport_num in (
            SELECT
                passport_num
            FROM DWH_FACT_passport_blacklist)
        and oper_result = 'SUCCESS'
        GROUP BY passport_num;
        ''', conn)
    passport_fraud.to_sql('REP_FRAUD', con=conn, if_exists='append', index=False)
    if len(passport_fraud) > 0:
        add_log('!! PASSPORT FRAUD !!', f'Passport fraud found: {len(passport_fraud)}')
# /Совершение операции при просроченном или заблокированном паспорте


# Совершение операции при недействующем договоре
def account_not_valid(conn):
    account_fraud=pd.read_sql('''
        SELECT
            min(trans_date) as event_dt,
            passport_num as passport,
            fio,
            phone,
            'ACCOUNT' as event_type
        FROM v_report_info_day
        WHERE trans_date > date(valid_to, '+1 day')
        and oper_result = 'SUCCESS'
        GROUP BY passport_num
    ''', conn)
    account_fraud.to_sql('REP_FRAUD', con=conn, if_exists='append', index=False)
    if len(account_fraud) > 0:
        add_log('!! ACCOUNT FRAUD !!', f'Account fraud found: {len(account_fraud)}')
# /Совершение операции при недействующем договоре


# Совершение операций в разных городах в течение одного часа
def one_hour(conn):
    cities_fraud=pd.read_sql('''
        SELECT
            min(trans_date) as event_dt,
            passport_num as passport,
            fio,
            phone,
            'CITIES' as event_type
        FROM 
            (
            SELECT
                trans_date,
                terminal_city != lag(terminal_city) over (partition by account order by trans_date) as different_city,
                strftime('%s', trans_date) - lag(strftime('%s', trans_date)) over (partition by account) as timedelta,
                passport_num,
                fio,
                phone
            FROM v_report_info_day
            )
        WHERE different_city = 1 and timedelta < 3600
        GROUP BY passport_num;
    ''', conn)
    cities_fraud.to_sql('REP_FRAUD', con=conn, if_exists='append', index=False)
    if len(cities_fraud) > 0:
        add_log('!! CITIES FRAUD !!', f'Cities fraud found: {len(cities_fraud)}')
# /Совершение операций в разных городах в течение одного часа



# Попытка подбора суммы
def amt_brute_force(conn):
    bruteforce_fraud=pd.read_sql('''
        SELECT
            trans_date as event_dt,
            passport_num as passport,
            fio,
            phone,
            'BRUTE_FORCE' as event_type
        FROM 
            (SELECT
                trans_date,
                passport_num,
                fio,
                phone,
                opertype,

                lag(amt,3) over (partition by account order by trans_date) as oper_1,
                lag(amt,2) over (partition by account order by trans_date) as oper_2,
                lag(amt) over (partition by account order by trans_date) as oper_3,
                amt as oper_4,

                lag(oper_result,3) over (partition by account order by trans_date) as res_1,
                lag(oper_result,2) over (partition by account order by trans_date) as res_2,
                lag(oper_result) over (partition by account order by trans_date) as res_3,
                oper_result as res_4,

                lag(strftime('%s', trans_date),3) over (partition by account order by trans_date) as t1,
                strftime('%s', trans_date) as t2

            FROM v_report_info_day)

        WHERE oper_1 > oper_2 and oper_2 > oper_3 and oper_3 > oper_4
        and res_1 = 'REJECT' and res_2 = 'REJECT' and res_3 = 'REJECT' and res_4 = 'SUCCESS'
        and opertype in ('WITHDRAW', 'PAYMENT')
        and t2-t1<1200
    ''', conn)
    bruteforce_fraud.to_sql('REP_FRAUD', con=conn, if_exists='append', index=False)
    if len(bruteforce_fraud) > 0:
        add_log('!! BRUTEFORCE FRAUD !!', f'Bruteforce fraud found: {len(bruteforce_fraud)}')
# /Попытка подбора суммы