# Python 3.10.6
# Для запуска необходимо:
# - файлы data.zip, ddl_dml.sql в папке со скриптом
# - папки py_scripts, sql_scripts в папке со скриптом
# - pandas и зависимости, openpyxl - из req.txt
# папка archive при ее отсутствии будет создана
from py_scripts.scripts import *
from py_scripts.fraud_check import *
import sys
import zipfile 

if __name__ == "__main__":
# Файл базы будет создан сам, если он не найден.
# В него будут загружены первоначальные данные из ddl_dml.sql,
# а также будут созданы необходимые таблицы и
# преобразованы первоначальные таблицы.
# Файлы из архива за три дня будут помещены в папку data.
    if not check_db_file():
        sql_script_exec('ddl_dml.sql')
        sql_script_exec('sql_scripts/create_tables.sql')
        sql_script_exec('sql_scripts/transform_initial_tables.sql')
        add_log('CREATE_DB', 'No DB file. Creating DB')
        try:
            with zipfile.ZipFile('data.zip', 'r') as zip_ref:
                zip_ref.extractall('data')
        except:
            pass
    add_log('-_'*10, '-_'*16)
    add_log('--BEGIN--', 'Script was executed')

    # Загрузка сырых данных
    if (load_data := load_raw_data_to_sql()) is None:
        add_log('--FINISH--', 'Nothing to be done')
        sys.exit()

    # Преобразование и загрузка в хранилище
    transform_raw_data()
    sql_script_exec('sql_scripts/drop_STG_tables.sql')

    conn = create_connection()

    create_v_report_info_day(conn)

    # Запуск сценариев проверки мошеннических операций
    account_not_valid(conn)
    passport_error(conn)
    one_hour(conn)
    amt_brute_force(conn)

    conn.close()

    add_log('--FINISH--', 'Finished successfully')