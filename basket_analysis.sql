-- basket analysis for home depot
drop table if exists tmp_eag.us_allpurch_13;
create table tmp_eag.us_allpurch_13(
	Period int(11)
    , RespondentID bigint(20)
    , Outlet int(11)
    , ProductID int(11)
    , Brand int(11)
    , quantity int(11) default 0
    , primary key (Period, RespondentID, ProductID)
    , index idx_pbr(ProductID, Brand, RespondentID)
    , index idx_pb(Brand, ProductID)
    , index idx_b(Brand)
    , index idx_r(RespondentID)
    , index idx_pr(ProductID));

insert into tmp_eag.us_allpurch_13(Period, RespondentID, Outlet, ProductID, Brand, quantity)
select Period, RespondentID, Outlet, ProductID, br, quantity
from us_allpurch.allpurch_202301
where Outlet=13
 and br is not null and br not in (998, 999);

select * from tmp_eag.us_allpurch_13
select ProductID, Brand from tmp_eag.us_allpurch_13 group by ProductID, Brand;
optimize local table tmp_eag.us_allpurch_13;

drop table if exists tmp_eag.allpurch_13_products_combined;
create table tmp_eag.allpurch_13_products_combined
(
	Period int(11) default 202301
	, Outlet int(11) default 13
    , Brand___1 int(11)
    , ProductID___1 int(11)
    , Brand___2 int(11)
    , ProductID___2 int(11)
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_bpbp(Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_b1(Brand___1)
    , index idx_b2(Brand___2)
);

-- takes 160 seconds
insert into tmp_eag.allpurch_13_products_combined(Brand___1, ProductID___1, Brand___2, ProductID___2)
select a.Brand, a.ProductID as ProductID___1, b.Brand, b.ProductID as ProductID___2
from
(
	select ProductID, Brand
	from tmp_eag.us_allpurch_13
    group by ProductID, Brand
) as a cross join
(
	select ProductID, Brand
	from tmp_eag.us_allpurch_13
    group by ProductID, Brand
) as b
group by a.ProductID, a.Brand, b.ProductID, b.Brand;

delete from tmp_eag.allpurch_13_products_combined
where Brand___1=Brand___2 and ProductID___1=ProductID___2;

optimize local table tmp_eag.allpurch_13_products_combined;

drop table if exists tmp_eag.bby_allpurch_202301_product_combinations_nodup;
create table tmp_eag.bby_allpurch_202301_product_combinations_nodup
(
    Outlet int(11) default 13
    , Brand___1 int(11)
    , ProductID___1 int(11)
    , Brand___2 int(11)
    , ProductID___2 int(11)
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_oone (Brand___1)
    , index idx_otwo (Brand___2)
    , index idx_pone (ProductID___1)
    , index idx_ptwo (ProductID___2)
    , index idx_oopp (Brand___1, ProductID___1, Brand___2, ProductID___2)
);

insert into tmp_eag.bby_allpurch_202301_product_combinations_nodup(Brand___1, ProductID___1, Brand___2, ProductID___2)
select (case when Brand___1 < Brand___2 then Brand___1 else Brand___2 end) as Brand_min,
	(case when ProductID___1 < ProductID___2 then ProductID___1 else ProductID___2 end) as ProductID_min,
    (case when Brand___1 > Brand___2 then Brand___1 else Brand___2 end) as Brand_max,
    (case when ProductID___1 > ProductID___2 then ProductID___1 else ProductID___2 end) as ProductID_max
from tmp_eag.allpurch_13_products_combined
group by Brand_min, ProductID_min, Brand_max, ProductID_max;

optimize local table tmp_eag.bby_allpurch_202301_product_combinations_nodup;


-- group by 1, 2, get count, divide by transaction total
drop table if exists tmp_eag.bby_resp_by_brand_product___1_2;
create table tmp_eag.bby_resp_by_brand_product___1_2
(
	Outlet int(11) default 13
    , Brand___1 int(11)
    , ProductID___1 int(11)
    , Brand___2 int(11)
    , ProductID___2 int(11)
    , resp_count___1_2 bigint(20)
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
);

insert into tmp_eag.bby_resp_by_brand_product___1_2(Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count___1_2)
select a.Brand, a.ProductID, b.Brand, b.ProductID, count(a.RespondentID)
from tmp_eag.us_allpurch_13 as a, 
tmp_eag.us_allpurch_13 as b, 
tmp_eag.bby_allpurch_202301_product_combinations_nodup as c
where a.RespondentID=b.RespondentID
	and a.Brand=c.Brand___1
    and a.ProductID=c.ProductID___1
    and b.Brand=c.Brand___2
    and b.ProductID=c.ProductID___2
group by a.Brand, a.ProductID, b.Brand, b.ProductID;



-- test for dups
-- no dups
select * from tmp_eag.bby_resp_by_brand_product___1_2
where (Brand___1=10 and ProductID___1=1) and (Brand___2=872 and ProductID___2=960)
or (Brand___2=10 and ProductID___2=1) and (Brand___1=872 and ProductID___1=960);



drop table if exists tmp_eag.bby_allpurch_202301_basket_analysis;
create table tmp_eag.bby_allpurch_202301_basket_analysis
(
	Period int(11) default 202301
    , Outlet int(11) default 13
    , Brand___1 int(11)
    , ProductID___1 int(11)
    , Brand___2 int(11)
    , ProductID___2 int(11)
    , resp_count_total int(11) default 0
    , resp_count___1 int(11) default 0
    , resp_count_pct___1 decimal(19,6) default 0
    , resp_count___2 int(11) default 0
    , resp_count_pct___2 decimal(19,6) default 0
    , resp_count___1_2 int(11) default 0
    , resp_count_pct___1_2 decimal(19,6) default 0
    , confidence___2_1 decimal(19,6) default 0
    , confidence___1_2 decimal(19,6) default 0
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_op1(Brand___1, ProductID___1)
    , index idx_op2 (Brand___2, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_o1(Brand___1)
    , index idx_o2(Brand___2)
);
    
insert into tmp_eag.bby_allpurch_202301_basket_analysis(Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count___1_2)
select Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count___1_2
from tmp_eag.bby_resp_by_brand_product___1_2;

update tmp_eag.bby_allpurch_202301_basket_analysis as a,
(
	select Brand, ProductID, count(RespondentID) as resp_count___1
	from tmp_eag.us_allpurch_13
    group by Brand, ProductID
) as b
set a.resp_count___1 = b.resp_count___1
where a.ProductID___1 = b.ProductID
	and a.Brand___1 = b.Brand;

update tmp_eag.bby_allpurch_202301_basket_analysis as a,
(
	select Brand, ProductID, count(RespondentID) as resp_count___2
	from tmp_eag.us_allpurch_13
    group by Brand, ProductID
) as b
set a.resp_count___2 = b.resp_count___2
where a.ProductID___2 = b.ProductID
	and a.Brand___2 = b.Brand;

update tmp_eag.bby_allpurch_202301_basket_analysis as a,
(
	select count(distinct RespondentID) as resp_count_total
	from tmp_eag.us_allpurch_13
) as b
set a.resp_count_total = b.resp_count_total;

update tmp_eag.bby_allpurch_202301_basket_analysis
set resp_count_pct___1 = resp_count___1/resp_count_total * 100;

update tmp_eag.bby_allpurch_202301_basket_analysis
set resp_count_pct___2 = resp_count___2/resp_count_total * 100;

update tmp_eag.bby_allpurch_202301_basket_analysis
set resp_count_pct___1_2 = resp_count___1_2/resp_count_total * 100;

-- P(ProductID___2 | ProductID___1)
update tmp_eag.bby_allpurch_202301_basket_analysis
set confidence___2_1 = resp_count_pct___1_2 / resp_count_pct___1;

-- P(ProductID___1 | ProductID___2)
update tmp_eag.bby_allpurch_202301_basket_analysis
set confidence___1_2 = resp_count_pct___1_2 / resp_count_pct___2;


drop table if exists tmp_eag.bby_allpurch_202301_basket_analysis_lbl;
create table tmp_eag.bby_allpurch_202301_basket_analysis_lbl
(
	Period int(11) default 202301
    , Outlet int(11) default 13
    , Brand___1 int(11)
    , lbl_Brand___1 varchar(64)
    , ProductID___1 int(11)
    , lbl_ProductID___1 varchar(64)
    , Brand___2 int(11)
    , lbl_Brand___2 varchar(64)
    , ProductID___2 int(11)
    , lbl_ProductID___2 varchar(64)
    , resp_count_pct___1_2 decimal(6,3) default 0
    , confidence___2_1 decimal(6,3) default 0
    , confidence___1_2 decimal(6,3) default 0
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_op1(Brand___1, ProductID___1)
    , index idx_op2 (Brand___2, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_o1(Brand___1)
    , index idx_o2(Brand___2)
);

insert into tmp_eag.bby_allpurch_202301_basket_analysis_lbl(Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count_pct___1_2, confidence___2_1, confidence___1_2)
select Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count_pct___1_2, confidence___2_1, confidence___1_2
from tmp_eag.bby_allpurch_202301_basket_analysis;

update tmp_eag.bby_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as ProductID___1, FormatLabel as lbl_ProductID___1
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ProductID___1 = b.lbl_ProductID___1
where a.ProductID___1=b.ProductID___1;

update tmp_eag.bby_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as ProductID___2, FormatLabel as lbl_ProductID___2
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ProductID___2 = b.lbl_ProductID___2
where a.ProductID___2=b.ProductID___2;

update tmp_eag.bby_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as Brand___1, FormatLabel as lbl_Brand___1
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.lbl_Brand___1 = b.lbl_Brand___1
where a.Brand___1=b.Brand___1;

update tmp_eag.bby_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as Brand___2, FormatLabel as lbl_Brand___2
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.lbl_Brand___2 = b.lbl_Brand___2
where a.Brand___2=b.Brand___2;
