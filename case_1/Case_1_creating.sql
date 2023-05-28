
-- Таблица  контрагента
create table Counterparty (
	Counterparty_RK int not null,
	Counterparty_name text not null,
	Counterparty_type_cd int not null,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица связи контрагента со счетом 
create table Counterparty_x_Account (
	Counterparty_RK int not null,
	Account_num text not null,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица счета
create table Account (
	Account_num text not null,
	Account_type text not null,
	Account_open_date date not null,
	Account_close_date date,
	Currency_cd text not null,
	Second_order_account_num text not null,
	Status_cd int not null,
	Bank_cd text not null,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица справочник валют
create table Dict_currency (
	Currency_cd text not null,
	Currency_abc_cd text not null,
	Currency_name text not null,
	Currency_country text not null,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица справочник статусов
create table Dict_status (
	Status_cd int not null,
	Status_name text not null,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица справочник плана счетов
create table Dict_balance_account (
	First_order_account_num text not null,
	Second_order_account_num text not null,
	Balance_attribute text not null,
	Balance_name text not null,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица справочника отделений банка
create table Dict_bank_branch (
	Bank_cd text not null,
	Bank_name text not null,
	Bank_address text,
	Bank_phone text,
	Effective_from_date date not null,
	Effective_to_date date not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Таблица справочник типов контрагента
create table Dict_counterparty_type (
	Counterparty_type_cd int not null,
	Counterparty_type_name text not null,
	"User" text not null,
	Deleted_flg bool not null,
	processed_dttm timestamp not null
);

-- Инсерт в таблицу контрагент
insert into counterparty values
(1, 'Тимофеев Илья Леонидович',   1, '2010-04-12', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(2, 'Карпова Кира Эминовна',      1, '2007-10-01', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(3, 'ИП Лапин Кирилл Тимофеевич', 3, '2015-01-10', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(4, 'Крылов Денис Николаевич',    3, '2020-09-07', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(5, 'Андреева Вера Сергеевна',    1, '2016-03-30', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16');


-- Инсерт в таблицу счет
insert into account  values
('45507810000000000001', 'Кредитный',  '2010-04-12', '2999-01-01', '810', '45507', 1, '0101', '2010-04-12', '2019-07-23', 'Artemy Latyshev', false, '2023-04-16'),
('40819840000000000002', 'Текущий',    '2007-10-01', '2999-01-01', '840', '40819', 1, '0205', '2007-10-01', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
('42114810000000000003', 'Депозитный', '2015-01-10', '2999-01-01', '810', '42114', 1, '0310', '2015-01-10', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
('40802978000000000004', 'Расчетный',  '2020-09-07', '2999-01-01', '978', '40802', 1, '0442', '2020-09-07', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
('40813840000000000005', 'Валютный',   '2016-03-30', '2999-01-01', '840', '40813', 1, '0579', '2016-03-30', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
('45507810000000000001', 'Кредитный',  '2010-04-12', '2019-07-23', '810', '45507', 2, '0101', '2019-07-23', '2999-12-31', 'Artemy Latyshev', true,  '2023-04-16');


-- Инсерт в таблицу связи контрагента и счета
insert into counterparty_x_account values
(1, '45507810000000000001', '2010-04-12', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(2, '40819840000000000002', '2007-10-01', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(3, '42114810000000000003', '2015-01-10', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(4, '40802978000000000004', '2020-09-07', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16'),
(5, '40813840000000000005', '2016-03-30', '2999-12-31', 'Artemy Latyshev', false, '2023-04-16');




