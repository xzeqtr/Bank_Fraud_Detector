CREATE TABLE if not exists DWH_DIM_CARDS_HIST
    (
    card_num varchar(128),
    account varchar(128),
    effective_from datetime,
    effective_to datetime default (datetime('9999-12-31 23:59:59')),
    deleted_flg integer default 0
    );

CREATE TABLE if not exists DWH_DIM_ACCOUNTS_HIST
	(
	account varchar(128), 
	valid_to date, 
	client varchar(128),
    effective_from datetime,
    effective_to datetime default (datetime('9999-12-31 23:59:59')),
    deleted_flg integer default 0
	);

CREATE TABLE if not exists DWH_DIM_CLIENTS_HIST
	(
    client_id varchar(128), 
    last_name varchar(128), 
    first_name varchar(128), 
    patronymic varchar(128), 
    date_of_birth date, 
    passport_num varchar(128), 
    passport_valid_to date, 
    phone varchar(128),
    effective_from datetime,
    effective_to datetime default (datetime('9999-12-31 23:59:59')),
    deleted_flg integer default 0
	);

CREATE TABLE if not exists DWH_DIM_terminals_HIST
	(
	terminal_id varchar(10),
	terminal_type varchar(10),
	terminal_city varchar(128),
	terminal_address varchar(128),
	effective_from datetime,
	effective_to datetime default (datetime('9999-12-31 23:59:59')),
	deleted_flg integer default 0
	);

CREATE TABLE if not exists DWH_FACT_passport_blacklist
	(
	passport_num varchar(11),
	entry_dt date
	);

CREATE TABLE if not exists DWH_FACT_transactions
	(
	trans_id varchar(20),
	trans_date date,
	card_num varchar(19),
	opertype varchar(10),
	amt decimal(12,2),
	oper_result varchar(10),
	terminal varchar(10)
	);

CREATE TABLE if not exists REP_FRAUD
	(
	event_dt date,
	passport varchar(11),
	fio varchar(128), 
	phone varchar(20),
	event_type varchar(128),
	report_dt datetime default current_timestamp
	);

CREATE TABLE if not exists META_log
	(
	event_dt date,
	event_type varchar(30),
	event_desription varchar(128)
	);





