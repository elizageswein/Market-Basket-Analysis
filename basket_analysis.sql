[8/30 3:28 PM] Eliza Geswein

-- Eliza Geswein

-- 08/2023

 

/*

performs market basket analysis on sample store Transaction database (src: https://www.kaggle.com/datasets/iamprateek/store-Transaction-data)

 

*/

 

-- import data in CSV file

drop table if exists tmp_eag.Transactions;

create table tmp_eag.Transactions(

    ﻿MONTH varchar(3) 

    ,DAY int(11)

    ,Transaction_ID varchar(4)

    ,Transaction_AMT decimal(6,2)

    ,QTY int(11)

    ,PRICE decimal(6,2)

    ,Item varchar(64)

    ,ITEM_ID int(11)

    ,Brand varchar(64)

    ,Brand_ID int(11)

    , primary key (Transaction_ID)

    , index idx_tib(Transaction_ID, ITEM_ID, Brand_ID)

    , index idx_i(ITEM_ID)

);

 

bulk insert into tmp_eag.Transactions

from 'Transactions_sample.csv'

with

(

    format='CSV'

    firstrow=2

)

go;

-- filter to only what we need
-- get column of integer part of TransactionID for indexing
drop table if exists tmp_eag.transactions_sample;
create table tmp_eag.transactions_sample
(
	TRANSACTION_ID varchar(4)
    , TRANSACTION_ID_NUM int(11)
    , ITEM varchar(64)
    , ITEM_ID int(11)
    , primary key (TRANSACTION_ID_NUM, ITEM_ID)
    , index idx_i (ITEM_ID)
    , index idx_t (TRANSACTION_ID_NUM)
);

insert into tmp_eag.transactions_sample(TRANSACTION_ID, TRANSACTION_ID_NUM, Item, ITEM_ID)
select TRANSACTION_ID, substring(TRANSACTION_ID, 2) as TRANSACTION_ID_NUM, ITEM, ITEM_ID
from tmp_eag.transactions
group by TRANSACTION_ID, ITEM_ID;

select TRANSACTION_ID, substring(TRANSACTION_ID, 2) as TRANSACTION_ID_NUM
from tmp_eag.transactions;

-- get unique items
 drop table if exists tmp_eag.items;
 create table tmp_eag.items
 (
	ITEM_ID int(11)
    , primary key (ITEM_ID)
);

insert into tmp_eag.items(ITEM_ID)
select distinct ITEM_ID
from tmp_eag.transactions_sample;

 
-- all possible Item, Brand combinations
drop table if exists tmp_eag.item_combinations;
create table tmp_eag.item_combinations
(
    ITEM_ID___1 int(11)
    , ITEM_ID___2 int(11)
    , primary key (ITEM_ID___1, ITEM_ID___2)
	, index idx_i1(ITEM_ID___1)
    , index idx_i2(ITEM_ID___2)
);

-- items table w index used to optimize cross join - takes .031 seconds vs. > 1 min from transactions_sample table
insert into tmp_eag.item_combinations(ITEM_ID___1, ITEM_ID___2)
select a.ITEM_ID as ITEM_ID___1, b.ITEM_ID as ITEM_ID___2
from
(
    select ITEM_ID
    from tmp_eag.items
) as a cross join
(
    select ITEM_ID
    from tmp_eag.items
) as b
group by a.ITEM_ID, b.ITEM_ID;

-- remove duplicate combinations
delete from tmp_eag.item_combinations
where ITEM_ID___1 >= ITEM_ID___2;

-- reset indexes
optimize local table tmp_eag.item_combinations;

 
-- count of Transactions that included Items 1 and 2
drop table if exists tmp_eag.item_combination_Transactions;
create table tmp_eag.item_combination_Transactions
(
    ITEM_ID___1 int(11)
    , ITEM_ID___2 int(11)
    , Transaction_count int(11)
    , primary key (ITEM_ID___1, ITEM_ID___2)
);

insert into tmp_eag.item_combination_Transactions(ITEM_ID___1, ITEM_ID___2, Transaction_count)
select a.ITEM_ID, b.ITEM_ID, count(a.TRANSACTION_ID_NUM)
from tmp_eag.transactions_sample as a, 
tmp_eag.transactions_sample as b, 
tmp_eag.item_combinations as c
where a.TRANSACTION_ID_NUM=b.TRANSACTION_ID_NUM
    and a.ITEM_ID=c.ITEM_ID___1
    and b.ITEM_ID=c.ITEM_ID___2
group by a.ITEM_ID, b.ITEM_ID;

drop table if exists tmp_eag.basket_analysis;
create table tmp_eag.basket_analysis
(
    ITEM_ID___1 int(11)
    , ITEM_ID___2 int(11)
    , Transaction_count_total int(11) default 0
    , Transaction_count___1 int(11) default 0
    , support___1 decimal(6,3) default 0
    , Transaction_count___2 int(11) default 0
    , support___2 decimal(6,3) default 0
    , Transaction_count___1_2 int(11) default 0
    , support___1_2 decimal(6,3) default 0
    , confidence___2_if_1 decimal(6,3) default 0
    , confidence___1_if_2 decimal(6,3) default 0
    , primary key (ITEM_ID___1, ITEM_ID___2)
    , index idx_i1(ITEM_ID___1)
    , index idx_i2 (ITEM_ID___2)
);

insert into tmp_eag.basket_analysis(ITEM_ID___1, ITEM_ID___2, Transaction_count___1_2)
select ITEM_ID___1, ITEM_ID___2, Transaction_count
from tmp_eag.item_combination_Transactions;

 

-- count of Transactions that included Item 1
update tmp_eag.basket_analysis as a,
(
    select ITEM_ID, count(TRANSACTION_ID_NUM) as Transaction_count___1
    from tmp_eag.transactions_sample
    group by ITEM_ID

) as b
set a.Transaction_count___1 = b.Transaction_count___1
where a.ITEM_ID___1 = b.ITEM_ID;

 
-- count of Transactions that included Item 2
update tmp_eag.basket_analysis as a,
(
    select ITEM_ID, count(TRANSACTION_ID_NUM) as Transaction_count___2
    from tmp_eag.transactions_sample
    group by ITEM_ID

) as b
set a.Transaction_count___2 = b.Transaction_count___2
where a.ITEM_ID___2 = b.ITEM_ID;


-- total Transaction count
update tmp_eag.basket_analysis as a,
(
    select count(distinct TRANSACTION_ID_NUM) as Transaction_count_total
    from tmp_eag.transactions_sample
) as b
set a.Transaction_count_total = b.Transaction_count_total;


-- support
-- pct of transactions that include Item 1
update tmp_eag.basket_analysis
set support___1 = Transaction_count___1/Transaction_count_total;

-- pct of transactions that include Item 2
update tmp_eag.basket_analysis
set support___2 = Transaction_count___2/Transaction_count_total;

-- pct of transactions that included Item 1 & Item 2
update tmp_eag.basket_analysis
set support___1_2 = Transaction_count___1_2/Transaction_count_total;

 
 -- confidence
-- P(ITEM_ID___2 | ITEM_ID___1)
-- probability of Item 2 being in a Transaction if Item 1 is in Transaction
update tmp_eag.basket_analysis
set confidence___2_if_1 = support___1_2 / support___1;

-- P(ITEM_ID___1 | ITEM_ID___2)
-- probability of Item 1 being in Transaction if Item 2 is in Transaction
update tmp_eag.basket_analysis
set confidence___1_if_2 = support___1_2 / support___2;

 
-- add Item labels
drop table if exists tmp_eag.basket_analysis_lbls;
create table tmp_eag.basket_analysis_lbls
(
    ITEM___1 varchar(64)
    , ITEM_ID___1 int(11)
    , ITEM___2 varchar(64)
    , ITEM_ID___2 int(11)
    , support___1 decimal(6,3) default 0
    , support___2 decimal(6,3) default 0
    , support___1_2 decimal(6,3) default 0
    , confidence___2_if_1 decimal(6,3) default 0
    , confidence___1_if_2 decimal(6,3) default 0
    , primary key (ITEM_ID___1, ITEM_ID___2)
);

insert into tmp_eag.basket_analysis_lbls(ITEM_ID___1, ITEM_ID___2, support___1, support___2, support___1_2, confidence___2_if_1, confidence___1_if_2)
select ITEM_ID___1, ITEM_ID___2, support___1, support___2, support___1_2, confidence___2_if_1, confidence___1_if_2
from tmp_eag.basket_analysis;

update tmp_eag.basket_analysis_lbls as a, tmp_eag.transactions_sample as b
set a.ITEM___1 = b.ITEM
where a.ITEM_ID___1 = b.ITEM_ID;

update tmp_eag.basket_analysis_lbls as a, tmp_eag.transactions_sample as b
set a.ITEM___2 = b.ITEM
where a.ITEM_ID___2 = b.ITEM_ID;

select * From tmp_eag.basket_analysis_lbls order by support___1_2 desc;

