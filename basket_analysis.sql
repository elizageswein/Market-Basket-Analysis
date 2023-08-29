-- Eliza Geswein
-- 08/2023

/*
performs market basket analysis on sample store transaction database (src: https://www.kaggle.com/datasets/iamprateek/store-transaction-data)

*/

drop table if exists tmp_eag.transactions;
create table tmp_eag.transactions(
	Month int(11)
    , TransactionID bigint(20)
    , ItemID int(11)
    , BrandID int(11)
    , Qty int(11) default 0
    , primary key (Month, TransactionID, ItemID)
    , index idx_ibt(ItemID, BrandID, TransactionID)
    , index idx_bi(BrandID, ItemID)
    , index idx_b(BrandID)
    , index idx_t(TransactionID)
    , index idx_i(ItemID));

insert into tmp_eag.transactions(Month, TransactionID, Outlet, ItemID, BrandID, Qty)
;

optimize local table tmp_eag.transactions;

drop table if exists tmp_eag.product_combinations;
create table tmp_eag.product_combinations
(
Month int(11)
    , BrandID___1 int(11)
    , ItemID___1 int(11)
    , BrandID___2 int(11)
    , ItemID___2 int(11)
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_bi1bi2(BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_i1(ItemID___1)
    , index idx_i2(ItemID___2)
    , index idx_b1(BrandID___1)
    , index idx_b2(BrandID___2)
);

-- takes 160 seconds
insert into tmp_eag.product_combinations(BrandID___1, ItemID___1, BrandID___2, ItemID___2)
select a.BrandID, a.ItemID as ItemID___1, b.BrandID, b.ItemID as ItemID___2
from
(
	select ItemID, BrandID
	from tmp_eag.transactions
    	group by ItemID, BrandID
) as a cross join
(
	select ItemID, BrandID
	from tmp_eag.transactions
    	group by ItemID, BrandID
) as b
group by a.ItemID, a.BrandID, b.ItemID, b.BrandID;

-- remove duplicate combinations
delete from tmp_eag.product_combinations
where BrandID___1=BrandID___2 and ItemID___1=ItemID___2;

optimize local table tmp_eag.product_combinations;


drop table if exists tmp_eag.product_combinations_nodups;
create table tmp_eag.product_combinations_nodups
(
    BrandID___1 int(11)
    , ItemID___1 int(11)
    , BrandID___2 int(11)
    , ItemID___2 int(11)
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
    BrandID___1 int(11)
    , ItemID___1 int(11)
    , BrandID___2 int(11)
    , ItemID___2 int(11)
    , transaction_count int(11)
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
);

insert into tmp_eag.product_combination_transactions(BrandID___1, ItemID___1, BrandID___2, ItemID___2, transaction_count)
select a.BrandID, a.ItemID, b.BrandID, b.ItemID, count(a.TransactionID)
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
	Month int(11) default 202301
    , Outlet int(11) default 13
    , BrandID___1 int(11)
    , ItemID___1 int(11)
    , BrandID___2 int(11)
    , ItemID___2 int(11)
    , resp_count_total int(11) default 0
    , resp_count___1 int(11) default 0
    , resp_count_pct___1 decimal(19,6) default 0
    , resp_count___2 int(11) default 0
    , resp_count_pct___2 decimal(19,6) default 0
    , transaction_count int(11) default 0
    , resp_count_pct___1_2 decimal(19,6) default 0
    , confidence___2_1 decimal(19,6) default 0
    , confidence___1_2 decimal(19,6) default 0
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
	select BrandID, ItemID, count(TransactionID) as resp_count___1
	from tmp_eag.transactions
    group by BrandID, ItemID
) as b
set a.resp_count___1 = b.resp_count___1
where a.ItemID___1 = b.ItemID
	and a.BrandID___1 = b.BrandID;

update tmp_eag.basket_analysis as a,
(
	select BrandID, ItemID, count(TransactionID) as resp_count___2
	from tmp_eag.transactions
    group by BrandID, ItemID
) as b
set a.resp_count___2 = b.resp_count___2
where a.ItemID___2 = b.ItemID
	and a.BrandID___2 = b.BrandID;

update tmp_eag.basket_analysis as a,
(
	select count(distinct TransactionID) as resp_count_total
	from tmp_eag.transactions
) as b
set a.resp_count_total = b.resp_count_total;

update tmp_eag.basket_analysis
set resp_count_pct___1 = resp_count___1/resp_count_total * 100;

update tmp_eag.basket_analysis
set resp_count_pct___2 = resp_count___2/resp_count_total * 100;

update tmp_eag.basket_analysis
set resp_count_pct___1_2 = transaction_count/resp_count_total * 100;

-- P(ItemID___2 | ItemID___1)
update tmp_eag.basket_analysis
set confidence___2_1 = resp_count_pct___1_2 / resp_count_pct___1;

-- P(ItemID___1 | ItemID___2)
update tmp_eag.basket_analysis
set confidence___1_2 = resp_count_pct___1_2 / resp_count_pct___2;


drop table if exists tmp_eag.basket_analysis_lbls;
create table tmp_eag.basket_analysis_lbls
(
	Month int(11) default 202301
    , Outlet int(11) default 13
    , BrandID___1 int(11)
    , lbl_BrandID___1 varchar(64)
    , ItemID___1 int(11)
    , lbl_ItemID___1 varchar(64)
    , BrandID___2 int(11)
    , lbl_BrandID___2 varchar(64)
    , ItemID___2 int(11)
    , lbl_ItemID___2 varchar(64)
    , resp_count_pct___1_2 decimal(6,3) default 0
    , confidence___2_1 decimal(6,3) default 0
    , confidence___1_2 decimal(6,3) default 0
    , primary key (BrandID___1, ItemID___1, BrandID___2, ItemID___2)
    , index idx_op1(BrandID___1, ItemID___1)
    , index idx_op2 (BrandID___2, ItemID___2)
    , index idx_p1(ItemID___1)
    , index idx_p2(ItemID___2)
    , index idx_o1(BrandID___1)
    , index idx_o2(BrandID___2)
);

insert into tmp_eag.basket_analysis_lbls(BrandID___1, ItemID___1, BrandID___2, ItemID___2, resp_count_pct___1_2, confidence___2_1, confidence___1_2)
select BrandID___1, ItemID___1, BrandID___2, ItemID___2, resp_count_pct___1_2, confidence___2_1, confidence___1_2
from tmp_eag.basket_analysis;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as ItemID___1, FormatLabel as lbl_ItemID___1
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ItemID___1 = b.lbl_ItemID___1
where a.ItemID___1=b.ItemID___1;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as ItemID___2, FormatLabel as lbl_ItemID___2
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ItemID___2 = b.lbl_ItemID___2
where a.ItemID___2=b.ItemID___2;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as BrandID___1, FormatLabel as lbl_BrandID___1
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.lbl_BrandID___1 = b.lbl_BrandID___1
where a.BrandID___1=b.BrandID___1;

update tmp_eag.basket_analysis_lbls as a, (
	select FormatValue as BrandID___2, FormatLabel as lbl_BrandID___2
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.lbl_BrandID___2 = b.lbl_BrandID___2
where a.BrandID___2=b.BrandID___2;
