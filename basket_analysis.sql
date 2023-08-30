-- Eliza Geswein
-- 08/2023

/*
performs market basket analysis on sample store transaction database (src: https://www.kaggle.com/datasets/iamprateek/store-transaction-data)

*/

drop table if exists tmp_eag.transactions;
create table tmp_eag.transactions(
	﻿MONTH varchar(3) 
	,DAY int(3)
	,TRANSACTION_ID varchar(4)
	,TRANSACTION_AMT decimal(6,2)
	,QTY int(3)
	,PRICE decimal(6,2)
	,ITEM varchar(64)
	,ITEM_ID int(3)
	,BRAND varchar(64)
	,BRAND_ID int(3)
	, primary key (TRANSACTION_ID)
	, index idx_tib(TRANSACTION_ID, ITEM_ID, BRAND_ID)
	, index idx_ib(ITEM_ID, BRAND_ID)
);

bulk insert into tmp_eag.transactions
from 'transactions_sample.csv'
with
(
	format='CSV'
	firstrow=2
)
go;



drop table if exists tmp_eag.products;
create table tmp_eag.products
(
	ITEM_ID int(3)
	, BRAND_ID int(3)
	, primary key (ITEM_ID, BRAND_ID)
	, index idx_ib(ITEM_ID, BRAND_ID)
);

insert into tmp_eag.products(ITEM_ID, BRAND_ID)
select ITEM_ID, BRAND_ID
from tmp_eag.transactions
group by ITEM_ID, BRAND_ID;

drop table if exists tmp_eag.product_combinations;
create table tmp_eag.product_combinations
(
    BrandID___1 int(3)
    , ItemID___1 int(3)
    , BrandID___2 int(3)
    , ItemID___2 int(3)
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_bi1bi2(BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_i1(ItemID___1)
    , index idx_i2(ItemID___2)
    , index idx_b1(BrandID___1)
    , index idx_b2(BrandID___2)
);

insert into tmp_eag.product_combinations(BrandID___1, ItemID___1, BrandID___2, ItemID___2)
select a.BrandID, a.ItemID as ItemID___1, b.BrandID, b.ItemID as ItemID___2
from
(
	select ItemID, BrandID
	from tmp_eag.products
) as a cross join
(
	select ItemID, BrandID
	from tmp_eag.products
) as b
group by a.ItemID, a.BrandID, b.ItemID, b.BrandID;

-- remove duplicate combinations
delete from tmp_eag.product_combinations
where BrandID___1=BrandID___2
	and ItemID___1=ItemID___2;

optimize local table tmp_eag.product_combinations;


drop table if exists tmp_eag.product_combinations_nodups;
create table tmp_eag.product_combinations_nodups
(
    BrandID___1 int(3)
    , ItemID___1 int(3)
    , BrandID___2 int(3)
    , ItemID___2 int(3)
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_oone (BrandID___1)
    , index idx_otwo (BrandID___2)
    , index idx_pone (ItemID___1)
    , index idx_ptwo (ItemID___2)
    , index idx_oopp (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
);

insert into tmp_eag.product_combinations_nodups(BrandID___1, ItemID___1, BrandID___2, ItemID___2)
select (case when BrandID___1 < BrandID___2 then BrandID___1 else BrandID___2 end) as BrandID_min,
	(case when ItemID___1 < ItemID___2 then ItemID___1 else ItemID___2 end) as ItemID_min,
    (case when BrandID___1 > BrandID___2 then BrandID___1 else BrandID___2 end) as BrandID_max,
    (case when ItemID___1 > ItemID___2 then ItemID___1 else ItemID___2 end) as ItemID_max
from tmp_eag.product_combinations
group by BrandID_min, ItemID_min, BrandID_max, ItemID_max;

optimize local table tmp_eag.product_combinations_nodups;


-- group by 1, 2, get count, divide by transaction total
drop table if exists tmp_eag.product_combination_transactions;
create table tmp_eag.product_combination_transactions
(
    BrandID___1 int(3)
    , ItemID___1 int(3)
    , BrandID___2 int(3)
    , ItemID___2 int(3)
    , transaction_count int(3)
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
);

insert into tmp_eag.product_combination_transactions(BrandID___1, ItemID___1, BrandID___2, ItemID___2, transaction_count)
select a.Brand_ID, a.Item_ID, b.Brand_ID, b.Item_ID, count(a.TransactionID)
from tmp_eag.transactions as a, 
tmp_eag.transactions as b, 
tmp_eag.product_combinations_nodups as c
where a.TransactionID=b.TransactionID
    and a.BrandID=c.BrandID___1
    and a.ItemID=c.ItemID___1
    and b.BrandID=c.BrandID___2
    and b.ItemID=c.ItemID___2
group by a.BrandID, a.ItemID, b.BrandID, b.ItemID;


drop table if exists tmp_eag.basket_analysis;
create table tmp_eag.basket_analysis
(
	Month int(3) default 202301
    , Outlet int(3) default 13
    , BrandID___1 int(3)
    , ItemID___1 int(3)
    , BrandID___2 int(3)
    , ItemID___2 int(3)
    , transaction_count_total int(3) default 0
    , transaction_count___1 int(3) default 0
    , support___1 decimal(19,6) default 0
    , transaction_count___2 int(3) default 0
    , support___2 decimal(19,6) default 0
    , transaction_count int(3) default 0
    , support___1_2 decimal(19,6) default 0
    , confidence___2_if_1 decimal(19,6) default 0
    , confidence___1_if_2 decimal(19,6) default 0
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_op1(BrandID___1, ItemID___1)
    , index idx_op2 (BrandID___2, ItemID___2)
    , index idx_p1(ItemID___1)
    , index idx_p2(ItemID___2)
    , index idx_o1(BrandID___1)
    , index idx_o2(BrandID___2)
);
    
insert into tmp_eag.basket_analysis(BrandID___1, ItemID___1, BrandID___2, ItemID___2, transaction_count)
select BrandID___1, ItemID___1, BrandID___2, ItemID___2, transaction_count
from tmp_eag.product_combination_transactions;

update tmp_eag.basket_analysis as a,
(
	select BrandID, ItemID, count(TransactionID) as transaction_count___1
	from tmp_eag.transactions
    group by BrandID, ItemID
) as b
set a.transaction_count___1 = b.transaction_count___1
where a.ItemID___1 = b.ItemID
	and a.BrandID___1 = b.BrandID;

update tmp_eag.basket_analysis as a,
(
	select BrandID, ItemID, count(TransactionID) as transaction_count___2
	from tmp_eag.transactions
    group by BrandID, ItemID
) as b
set a.transaction_count___2 = b.transaction_count___2
where a.ItemID___2 = b.ItemID
	and a.BrandID___2 = b.BrandID;

update tmp_eag.basket_analysis as a,
(
	select count(distinct TransactionID) as transaction_count_total
	from tmp_eag.transactions
) as b
set a.transaction_count_total = b.transaction_count_total;

update tmp_eag.basket_analysis
set support___1 = transaction_count___1/transaction_count_total * 100;

update tmp_eag.basket_analysis
set support___2 = transaction_count___2/transaction_count_total * 100;

update tmp_eag.basket_analysis
set support___1_2 = transaction_count/transaction_count_total * 100;

-- P(ItemID___2 | ItemID___1)
update tmp_eag.basket_analysis
set confidence___2_if_1 = support___1_2 / support___1;

-- P(ItemID___1 | ItemID___2)
update tmp_eag.basket_analysis
set confidence___1_if_2 = support___1_2 / support___2;


drop table if exists tmp_eag.basket_analysis_lbls;
create table tmp_eag.basket_analysis_lbls
(
	Month int(3) default 202301
    , Outlet int(3) default 13
    , BrandID___1 int(3)
    , Brand_lbl___1 varchar(64)
    , ItemID___1 int(3)
    , ItemID_lbl___1 varchar(64)
    , BrandID___2 int(3)
    , Brand_lbl___2 varchar(64)
    , ItemID___2 int(3)
    , ItemID_lbl___2 varchar(64)
    , support___1_2 decimal(6,3) default 0
    , confidence___2_if_1 decimal(6,3) default 0
    , confidence___1_if_2 decimal(6,3) default 0
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_op1(BrandID___1, ItemID___1)
    , index idx_op2 (BrandID___2, ItemID___2)
    , index idx_p1(ItemID___1)
    , index idx_p2(ItemID___2)
    , index idx_o1(BrandID___1)
    , index idx_o2(BrandID___2)
);

insert into tmp_eag.basket_analysis_lbls(BrandID___1, ItemID___1, BrandID___2, ItemID___2, support___1_2, confidence___2_if_1, confidence___1_if_2)
select BrandID___1, ItemID___1, BrandID___2, ItemID___2, support___1_2, confidence___2_if_1, confidence___1_if_2
from tmp_eag.basket_analysis;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as ItemID___1, FormatLabel as ItemID_lbl___1
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.ItemID_lbl___1 = b.ItemID_lbl___1
where a.ItemID___1=b.ItemID___1;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as ItemID___2, FormatLabel as ItemID_lbl___2
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.ItemID_lbl___2 = b.ItemID_lbl___2
where a.ItemID___2=b.ItemID___2;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as BrandID___1, FormatLabel as Brand_lbl___1
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.Brand_lbl___1 = b.Brand_lbl___1
where a.BrandID___1=b.BrandID___1;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as BrandID___2, FormatLabel as Brand_lbl___2
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.Brand_lbl___2 = b.Brand_lbl___2
where a.BrandID___2=b.BrandID___2;
