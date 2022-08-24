-- Т.к. update_dt везде Null, то задачу можно упростить.
-- В противном случае необходимо добавить пару функций
-- как в случае загрузки изменений к терминалам

INSERT INTO DWH_DIM_ACCOUNTS_HIST
(
account, 
valid_to,
client,
effective_from
)
SELECT
    account, 
    valid_to,
    client,
    create_dt
FROM accounts;

INSERT INTO DWH_DIM_CARDS_HIST
(
card_num, 
account,
effective_from
)
SELECT
    card_num, 
    account,
    create_dt
FROM cards;

INSERT INTO DWH_DIM_CLIENTS_HIST
(
client_id,
last_name,
first_name,
patronymic,
date_of_birth,
passport_num,
passport_valid_to,
phone,
effective_from
)
SELECT
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt
FROM clients;

DROP TABLE if exists accounts;
DROP TABLE if exists cards;
DROP TABLE if exists clients;