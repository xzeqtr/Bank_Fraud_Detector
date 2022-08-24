import sqlite3
from sqlite3 import Error
import pandas as pd
import sys
import os
import re


# Проверка наличия файла с базой
def check_db_file(path='database.db'):
    if os.path.isfile(path):
        return True
# /Проверка наличия файла с базой

# Выполнение SQL комманд из файла
def sql_script_exec(filename):
    with open(filename, 'r', encoding='utf-8') as sql_file:
        sql_script = sql_file.read()
    conn = create_connection()
    cursor = conn.cursor()
    cursor.executescript(sql_script)
    conn.commit()
    conn.close()
# /Выполнение SQL комманд из файла


# Создание подключения к базе
def create_connection(path='database.db'):
    connection = None
    try:
        connection = sqlite3.connect(path)
    except Error as e:
        print(f'The error "{e}" occurred')
        sys.exit()
    return connection
# /Создание подключения к базе


# Получаем даты файлов с выгрузками в порядке возрастания:
# Преобразуем дату в формат, который прост для сортировки
# по возрастанию (из DDMMYYYY в YYYYMMDD), сортируем файлы и
# возвращаемся к формату DDMMYYYY. Начало списка - минимальная дата.
def get_file_dates():
    if (lst := os.listdir('./data')) == []:
        return None
    final = []
    for el in lst:
        if el.endswith('txt'):
            match = re.findall('.*_(\d{8})\.', el)[0]
            final.append(match[4:8]+match[2:4]+match[:2])
    final = sorted(final)
    for i in range(0, len(final)):
        final[i] = final[i][6:8] + final[i][4:6] + final[i][:4]
    return final
# /Получаем даты файлов с выгрузками в порядке возрастания


# Добавление записи в META_log
def add_log(event_type, description):
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO META_log(
        event_dt,
        event_type,
        event_desription)
        values(datetime('now'), ?, ?)
        ''', [event_type, description])
    conn.commit()
    conn.close()
# /Добавление записи в META_log


# Загрузка данных из файлов
def load_raw_data_to_sql():
    if (dates_list := get_file_dates()) is None:
        add_log('NO_FILES', 'No files, nothing to update')
        return None
    else:
        add_log('EXTRACT', 'Loading files on date ' + dates_list[0])

        # Определяем имена файлов для чтения
        passports_filename = f'data/passport_blacklist_{dates_list[0]}.xlsx'
        terminals_filename = f'data/terminals_{dates_list[0]}.xlsx'
        transactions_filename = f'data/transactions_{dates_list[0]}.txt'

        # Считываем данные из файлов
        passport_blacklist = pd.read_excel(passports_filename, index_col=None)
        terminals = pd.read_excel(terminals_filename, index_col=None)
        # Добавим стобец с датой загруженного файла (формат даты YYYY-MM-DD)
        terminals['file_date'] = dates_list[0][4:8] +\
                           '-' + dates_list[0][2:4] +\
                           '-' + dates_list[0][:2]
        transactions = pd.read_csv(transactions_filename, index_col=None, sep=';')

        # Загрузка данных в промежуточные таблицы
        conn = create_connection()
        passports_added = passport_blacklist.to_sql('STG_passport_blacklist', con=conn, if_exists='replace', index=False)
        terminals_added = terminals.to_sql('STG_terminals', con=conn, if_exists='replace', index=False)
        transactions_added = transactions.to_sql('STG_transactions', con=conn, if_exists='replace', index=False)
        conn.close()

        # Логируем количество загруженных строк
        add_log('EXTRACT', f'Loaded {passports_added} passports')
        add_log('EXTRACT', f'Loaded {terminals_added} terminals')
        add_log('EXTRACT', f'Loaded {transactions_added} transactions')

        # Переименовываем и перемещаем файлы в архив
        dst_path = 'archive/'
        if not os.path.isdir(dst_path):
            os.makedirs(dst_path)
        os.rename(passports_filename, dst_path + passports_filename.split('/')[1] + '.backup')
        os.rename(terminals_filename, dst_path + terminals_filename.split('/')[1] + '.backup')
        os.rename(transactions_filename, dst_path + transactions_filename.split('/')[1] + '.backup')
        add_log('BACKUP', passports_filename.split('/')[1] + 'backuped')
        add_log('BACKUP', terminals_filename.split('/')[1] + 'backuped')
        add_log('BACKUP', transactions_filename.split('/')[1] + 'backuped')

        return 1
# /Загрузка данных из файлов


# Загрузка данных из промежуточных таблиц в хранилище
def transform_raw_data():
    conn = create_connection()
    cursor = conn.cursor()

    # Загрузка терминалов
    # Создание представления с актуальными терминалами
    cursor.execute(''' 
        CREATE VIEW if not exists v_DWH_DIM_terminals_HIST as
            SELECT
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
                --effective_from,
                --effective_to,
                --deleted_flg
            FROM DWH_DIM_terminals_HIST
            where current_timestamp between effective_from and effective_to
            and deleted_flg = 0
    ''')
    # Создание промежуточных таблиц для новых, измененных и удаленных данных
    cursor.execute('''
        CREATE TABLE if not exists STG_new_terminals as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address,
            t1.file_date
        FROM STG_terminals t1
        LEFT JOIN v_DWH_DIM_terminals_HIST t2
        ON t1.terminal_id = t2.terminal_id
        WHERE t2.terminal_id is Null
        ''')

    cursor.execute('''
    CREATE TABLE if not exists STG_change_terminals as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address,
            t1.file_date
        FROM STG_terminals t1
        INNER JOIN v_DWH_DIM_terminals_HIST t2
        ON t1.terminal_id = t2.terminal_id
        AND 
            (
            t1.terminal_id <> t2.terminal_id or
            t1.terminal_type <> t2.terminal_type or
            t1.terminal_city <> t2.terminal_city or
            t1.terminal_address <> t2.terminal_address
            );
    ''')

    cursor.execute('''
    CREATE TABLE if not exists STG_deleted_terminals as
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address,
            t2.file_date
        FROM v_DWH_DIM_terminals_HIST t1
        LEFT JOIN STG_terminals t2
        ON t1.terminal_id = t2.terminal_id
        WHERE t2.terminal_id is Null;
    ''')

    # Добавляем новые данные
    cursor.execute('''
        INSERT INTO DWH_DIM_TERMINALS_HIST
            (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
            )
            SELECT
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                file_date
            FROM STG_new_terminals;
        ''')

    # добавляем измененные данные
    # 1) изменить effective_to для старой версии (update)
    # 2) добавляем новую версию (insert)
    cursor.execute(''' 
        UPDATE DWH_DIM_terminals_HIST
        SET effective_to = (SELECT datetime((SELECT max(file_date) 
                                            FROM STG_terminals), '-1 second'))
        WHERE terminal_id in (SELECT terminal_id FROM STG_change_terminals)
        and effective_to = datetime('9999-12-31 23:59:59');
    ''')

    cursor.execute(''' 
        INSERT INTO DWH_DIM_terminals_HIST
            (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
            )
        SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            file_date
        FROM STG_change_terminals;
    ''')


    # применить удаление данных
    # 1) закрыть effective_to у старой версии удаленных данных
    # 2) добавить новые строки с deleted_flg = 1

    cursor.execute(''' 
        UPDATE DWH_DIM_terminals_HIST
        SET effective_to = (SELECT datetime((SELECT max(file_date) 
                                            FROM STG_terminals), '-1 second'))
        WHERE terminal_id in (SELECT terminal_id FROM STG_deleted_terminals)
        AND effective_to = datetime('9999-12-31 23:59:59');
    ''')

    cursor.execute(''' 
        INSERT INTO DWH_DIM_terminals_HIST
            (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from,
            deleted_flg
            ) 
            SELECT
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                (SELECT date((SELECT max(file_date) 
                              FROM STG_terminals))),
                1
            FROM STG_deleted_terminals;
    ''')
    # /Загрузка терминалов

    # Загрузка в хранилище только новых паспортов
    cursor.execute('''
    CREATE TABLE if not exists STG_new_passports as
    SELECT
        t1.passport,
        t1.date
    FROM STG_passport_blacklist t1
    LEFT JOIN DWH_FACT_passport_blacklist t2
    ON t1.passport = t2.passport_num
    WHERE t2.passport_num is Null;
    ''')

    cursor.execute('''
        INSERT INTO DWH_FACT_passport_blacklist
            (
            passport_num,
            entry_dt
            )
            SELECT
                passport,
                date
            FROM STG_new_passports;
    ''')
    # /Загрузка в хранилище только новых паспортов

    # Добавление транзакций в хранилище
    cursor.execute('''
        INSERT INTO DWH_FACT_transactions
            (
            trans_id,
            trans_date,
            card_num,
            opertype,
            amt,
            oper_result,
            terminal
            )
            SELECT
                transaction_id,
                transaction_date,
                card_num,
                oper_type,
                amount,
                oper_result,
                terminal
            FROM STG_transactions;
        ''')
    # /Добавление транзакций в хранилище

    conn.commit()
    conn.close()

    add_log('TRANSFORM', 'Data transfered from STG to DWH')
# /Загрузка данных из STG таблиц в хранилище
