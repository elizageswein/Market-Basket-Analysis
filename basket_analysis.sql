drop table if exists tmp_eag.us_allpurch___allpurch_202301;
create table tmp_eag.us_allpurch___allpurch_202301
(
	Period int(11)
    , RespondentID bigint(20)
    , ProductID int(11)
    , Outlet int(11)
    , quantity int(11) default 0
    , primary key (ProductID, Outlet, RespondentID)
    , index idx_o(Outlet)
    , index idx_p(ProductID)
    , index idx_op(Outlet, ProductID)
);
    
insert into tmp_eag.us_allpurch___allpurch_202301(Period, RespondentID, ProductID, Outlet, quantity)
select Period, RespondentID, ProductID, Outlet, quantity
from us_allpurch.allpurch_202301
where q_Production = 1;

drop table if exists tmp_eag.us_allpurch_202301_products;
create table tmp_eag.us_allpurch_202301_products
(
    Outlet int(11)
    , ProductID int(11)
    , primary key (Outlet, ProductID)
    , index idx_op (Outlet, ProductID)
    , index idx_o (Outlet)
    , index idx_p (ProductID)
);

insert into tmp_eag.us_allpurch_202301_products(Outlet, ProductID)
select Outlet, ProductID
from tmp_eag.us_allpurch___allpurch_202301
group by Outlet, ProductID;

optimize local table tmp_eag.us_allpurch_202301_products;

drop table if exists tmp_eag.us_allpurch_202301_product_combinations;
create table tmp_eag.us_allpurch_202301_product_combinations
(
    Outlet___1 int(11)
    , ProductID___1 int(11)
    , Outlet___2 int(11)
    , ProductID___2 int(11)
    , primary key (Outlet___1, ProductID___1, Outlet___2, ProductID___2)
    , index idx_onetwo (Outlet___1, ProductID___1, Outlet___2, ProductID___2)
    , index idx_one (Outlet___1, ProductID___1)
    , index idx_two (Outlet___2, ProductID___2)
);

insert into tmp_eag.us_allpurch_202301_product_combinations(Outlet___1, ProductID___1, Outlet___2, ProductID___2)
select a.Outlet, a.ProductID, b.Outlet, b.ProductID
from
(
	select Outlet, ProductID
    from tmp_eag.us_allpurch___allpurch_202301
    group by Outlet, ProductID
) as a
cross join
(
	select Outlet, ProductID
    from tmp_eag.us_allpurch___allpurch_202301
    group by Outlet, ProductID
) as b
group by a.Outlet, a.ProductID, b.Outlet, b.ProductID;

-- remove duplicate combinations
delete from tmp_eag.us_allpurch_202301_product_combinations
where Outlet___1 = Outlet___2 and ProductID___1=ProductID___2;

optimize local table tmp_eag.us_allpurch_202301_product_combinations;

drop table if exists tmp_eag.us_allpurch_202301_product_combinations_nodup;
create table tmp_eag.us_allpurch_202301_product_combinations_nodup
(
    Outlet___1 int(11)
    , ProductID___1 int(11)
    , Outlet___2 int(11)
    , ProductID___2 int(11)
    , primary key (Outlet___1, ProductID___1, Outlet___2, ProductID___2)
    , index idx_oone (Outlet___1)
    , index idx_otwo (Outlet___2)
    , index idx_pone (ProductID___1)
    , index idx_ptwo (ProductID___2)
    , index idx_oopp (Outlet___1, ProductID___1, Outlet___2, ProductID___2)
);

insert into tmp_eag.us_allpurch_202301_product_combinations_nodup(Outlet___1, ProductID___1, Outlet___2, ProductID___2)
select (case when Outlet___1 < Outlet___2 then Outlet___1 else Outlet___2 end) as Outlet_min,
	(case when ProductID___1 < ProductID___2 then ProductID___1 else ProductID___2 end) as ProductID_min,
    (case when Outlet___1 > Outlet___2 then Outlet___1 else Outlet___2 end) as Outlet_max,
    (case when ProductID___1 > ProductID___2 then ProductID___1 else ProductID___2 end) as ProductID_max
from tmp_eag.us_allpurch_202301_product_combinations
group by Outlet_min, ProductID_min, Outlet_max, ProductID_max;



#tmp_eag.resp_Outlet_ProductID___1 X us_allpurch_202301_product_combinations_nodup BY Outlet_ProductID___1
#tmp_eag.resp_Outlet_ProductID___2 X us_allpurch_202301_product_combinations_nodup BY Outlet_ProductID___2
#where tmp_eag.resp_Outlet_ProductID___1.RespondentID=tmp_eag.resp_Outlet_ProductID___2.RespondentID

-- group by 1, 2, get count, divide by transaction total
drop table if exists tmp_eag.resp_by_Outlet_ProductID___1_2;
create table tmp_eag.resp_by_Outlet_ProductID___1_2
(
	Outlet___1 int(11)
    , ProductID___1 int(11)
    , Outlet___2 int(11)
    , ProductID___2 int(11)
    , resp_count___1_2 bigint(20)
    , primary key (Outlet___1, ProductID___1, Outlet___2, ProductID___2)
);

insert into tmp_eag.resp_by_Outlet_ProductID___1_2(Outlet___1, ProductID___1, Outlet___2, ProductID___2, resp_count___1_2)
select a.Outlet, a.ProductID, b.Outlet, b.ProductID, count(a.RespondentID)
from tmp_eag.us_allpurch___allpurch_202301 as a, 
tmp_eag.us_allpurch___allpurch_202301 as b, 
tmp_eag.us_allpurch_202301_product_combinations_nodup as c
where a.RespondentID=b.RespondentID
	and a.Outlet=c.Outlet___1
    and a.ProductID=c.ProductID___1
    and b.Outlet=c.Outlet___2
    and b.ProductID=c.ProductID___2
group by a.Outlet, a.ProductID, b.Outlet, b.ProductID;


-- test for dups
-- no dups
select * from tmp_eag.resp_by_Outlet_ProductID___1_2
where (Outlet___1=326 and ProductID___1=401) and (Outlet___2=6 and ProductID___2=520)
or (Outlet___2=326 and ProductID___2=401) and (Outlet___1=6 and ProductID___1=520);


drop table if exists tmp_eag.allpurch_202301_basket_analysis;
create table tmp_eag.allpurch_202301_basket_analysis
(
	Period int(11) default 202301
    , Outlet___1 int(11)
    , Outlet___2 int(11)
    , ProductID___1 int(11)
    , ProductID___2 int(11)
    , resp_count_total int(11) default 0
    , resp_count___1 int(11) default 0
    , resp_count_pct___1 decimal(19,6) default 0
    , resp_count___2 int(11) default 0
    , resp_count_pct___2 decimal(19,6) default 0
    , resp_count___1_2 int(11) default 0
    , resp_count_pct___1_2 decimal(19,6) default 0
    , probability___2_1 decimal(19,6) default 0
    , probability___1_2 decimal(19,6) default 0
    , primary key (period, Outlet___1, ProductID___1, Outlet___2, ProductID___2)
    , index idx_op1(Outlet___1, ProductID___1)
    , index idx_op2 (Outlet___1, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_o1(Outlet___1)
    , index idx_o2(Outlet___2)
);
    
insert into tmp_eag.allpurch_202301_basket_analysis(Outlet___1, ProductID___1, Outlet___2, ProductID___2, resp_count___1_2)
select Outlet___1, ProductID___1, Outlet___2, ProductID___2, resp_count___1_2
from tmp_eag.resp_by_Outlet_ProductID___1_2;

-- count of respondents who bought product 1 at outlet 1
update tmp_eag.allpurch_202301_basket_analysis as a,
(
	select Outlet, ProductID, count(RespondentID) as resp_count___1
	from tmp_eag.us_allpurch___allpurch_202301
    group by Outlet, ProductID
) as b
set a.resp_count___1 = b.resp_count___1
where a.ProductID___1 = b.ProductID
	and a.Outlet___1 = b.Outlet;

-- count of respondents who bought product 2 at outlet 2
update tmp_eag.allpurch_202301_basket_analysis as a,
(
	select Outlet, ProductID, count(RespondentID) as resp_count___2
	from tmp_eag.us_allpurch___allpurch_202301
    group by Outlet, ProductID
) as b
set a.resp_count___2 = b.resp_count___2
where a.ProductID___2 = b.ProductID
	and a.Outlet___2 = b.Outlet;

-- total respondent count
update tmp_eag.allpurch_202301_basket_analysis as a,
(
	select count(distinct RespondentID) as resp_count_total
	from tmp_eag.us_allpurch___allpurch_202301
) as b
set a.resp_count_total = b.resp_count_total;

-- % of respondents who bought product 1 at outlet 1
update tmp_eag.allpurch_202301_basket_analysis
set resp_count_pct___1 = resp_count___1/resp_count_total * 100;

-- % of respondents who bought product 2 at outlet 2
update tmp_eag.allpurch_202301_basket_analysis
set resp_count_pct___2 = resp_count___2/resp_count_total * 100;

-- % of respondents who bought product 1 at outlet 1 AND product 2 at outlet 2
update tmp_eag.allpurch_202301_basket_analysis
set resp_count_pct___1_2 = resp_count___1_2/resp_count_total * 100;

-- P(Outlet_ProductID___2 | Outlet_ProductID___1)
-- probability of product 2 being bought at outlet 2 given that product 1 at outlet 1 is bought
update tmp_eag.allpurch_202301_basket_analysis
set probability___2_1 = resp_count_pct___1_2 / resp_count_pct___1;

-- P(Outlet_ProductID___1 | Outlet_ProductID___2)
-- probability of product 1 being bought at outlet 1 given that product 2 at outlet 2 is bought
update tmp_eag.allpurch_202301_basket_analysis
set probability___1_2 = resp_count_pct___1_2 / resp_count_pct___2;

-- probability vs. expected probability
-- how much better/worse calculated probability is at predicting this association vs. just random choice
-- if >1, antecedent increases likelihood of consequent being bought by same respondent
-- if <1, antecedent decreases likelihood of consequent being bought by same respondent
update tmp_eag.allpurch_202301_basket_analysis
set probability_support___2_1 = resp_count_pct___1_2 / (resp_count_pct___2 * resp_count_pct___1)

drop table if exists tmp_eag.allpurch_202301_basket_analysis_lbl;
create table tmp_eag.allpurch_202301_basket_analysis_lbl
(
	Period int(11) default 202301
    , Outlet___1 int(11)
    , lbl_Outlet___1 varchar(64)
    , ProductID___1 int(11)
    , lbl_ProductID___1 varchar(64)
    , Outlet___2 int(11)
    , lbl_Outlet___2 varchar(64)
    , ProductID___2 int(11)
    , lbl_ProductID___2 varchar(64)
    , resp_count_pct___1_2 decimal(6,3) default 0
    , probability___2_1 decimal(6,3) default 0
    , probability___1_2 decimal(6,3) default 0
    , primary key (period, Outlet___1, ProductID___1, Outlet___2, ProductID___2)
    , index idx_op1(Outlet___1, ProductID___1)
    , index idx_op2 (Outlet___1, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_o1(Outlet___1)
    , index idx_o2(Outlet___2)
);

insert into tmp_eag.allpurch_202301_basket_analysis_lbl(Period, Outlet___1, ProductID___1, Outlet___2, ProductID___2, resp_count_pct___1_2, probability___2_1, probability___1_2)
select Period, Outlet___1, ProductID___1, Outlet___2, ProductID___2, resp_count_pct___1_2, probability___2_1, probability___1_2
from tmp_eag.allpurch_202301_basket_analysis;

update tmp_eag.allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as ProductID___1, FormatLabel as lbl_ProductID___1
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ProductID___1 = b.lbl_ProductID___1
where a.ProductID___1=b.ProductID___1;

update tmp_eag.allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as ProductID___2, FormatLabel as lbl_ProductID___2
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ProductID___2 = b.lbl_ProductID___2
where a.ProductID___2=b.ProductID___2;

update tmp_eag.allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as Outlet___1, FormatLabel as lbl_Outlet___1
	from tq_admin.vw_formats where FormatID = 1299
) as b
set a.lbl_Outlet___1 = b.lbl_Outlet___1
where a.Outlet___1=b.Outlet___1;

update tmp_eag.allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as Outlet___2, FormatLabel as lbl_Outlet___2
	from tq_admin.vw_formats where FormatID = 1299
) as b
set a.lbl_Outlet___2 = b.lbl_Outlet___2
where a.Outlet___2=b.Outlet___2;


select * from tmp_eag.allpurch_202301_basket_analysis_lbl
order by resp_count_pct___1_2 desc;

select * from tmp_eag.allpurch_202301_basket_analysis_lbl
where Outlet___1 != Outlet___2
order by resp_count_pct___1_2 desc;



-- basket analysis for home depot
drop table if exists tmp_eag.us_allpurch_326;
create table tmp_eag.us_allpurch_326(
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

insert into tmp_eag.us_allpurch_326(Period, RespondentID, Outlet, ProductID, Brand, quantity)
select Period, RespondentID, Outlet, ProductID, br, quantity
from us_allpurch.allpurch_202301
where Outlet=326
 and br is not null and br != 999;

select ProductID, Brand from tmp_eag.us_allpurch_326 group by ProductID, Brand;
optimize local table tmp_eag.us_allpurch_326;

drop table if exists tmp_eag.allpurch_326_products_combined;
create table tmp_eag.allpurch_326_products_combined
(
	Period int(11) default 202301
	, Outlet int(11) default 326
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
insert into tmp_eag.allpurch_326_products_combined(Brand___1, ProductID___1, Brand___2, ProductID___2)
select a.Brand, a.ProductID as ProductID___1, b.Brand, b.ProductID as ProductID___2
from
(
	select ProductID, Brand
	from tmp_eag.us_allpurch_326
    group by ProductID, Brand
) as a cross join
(
	select ProductID, Brand
	from tmp_eag.us_allpurch_326
    group by ProductID, Brand
) as b
group by a.ProductID, a.Brand, b.ProductID, b.Brand;

delete from tmp_eag.allpurch_326_products_combined
where Brand___1=Brand___2 and ProductID___1=ProductID___2;

optimize local table tmp_eag.allpurch_326_products_combined;

drop table if exists tmp_eag.thd_allpurch_202301_product_combinations_nodup;
create table tmp_eag.thd_allpurch_202301_product_combinations_nodup
(
    Outlet int(11) default 326
    , Brand___1 int(11)
    , ProductID___1 int(11)
    , Brand___2 int(11)
    , ProductID___2 int(11)
    , Brand_min int(11)
    , ProductID_min int(11)
    , Brand_max int(11)
    , ProductID_max int(11)
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_oone (Brand___1)
    , index idx_otwo (Brand___2)
    , index idx_pone (ProductID___1)
    , index idx_ptwo (ProductID___2)
    , index idx_oopp (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_bmi(Brand_min)
    , index idx_bma(Brand_max)
    , index idx_pmi(ProductID_min)
    , index idx_pma(ProductID_max)
);

-- takes about 2-3 mins
insert into tmp_eag.thd_allpurch_202301_product_combinations_nodup(Brand___1, ProductID___1, Brand___2, ProductID___2)
select (case when Brand___1 < Brand___2 then Brand___1 else Brand___2 end) as Brand_min,
	(case when ProductID___1 < ProductID___2 then ProductID___1 else ProductID___2 end) as ProductID_min,
    (case when Brand___1 > Brand___2 then Brand___1 else Brand___2 end) as Brand_max,
    (case when ProductID___1 > ProductID___2 then ProductID___1 else ProductID___2 end) as ProductID_max
from tmp_eag.allpurch_326_products_combined
group by Brand_min, ProductID_min, Brand_max, ProductID_max;

/*
-- alternative method - takes about 6 mins
-- takes about 54 seconds for each update
update tmp_eag.thd_allpurch_202301_product_combinations_nodup
set Brand_min = (case when Brand___1 < Brand___2 then Brand___1 else Brand___2 end);

update tmp_eag.thd_allpurch_202301_product_combinations_nodup
set ProductID_min = (case when ProductID___1 < ProductID___2 then ProductID___1 else ProductID___2 end);

update tmp_eag.thd_allpurch_202301_product_combinations_nodup
set Brand_max = (case when Brand___1 > Brand___2 then Brand___1 else Brand___2 end);

update tmp_eag.thd_allpurch_202301_product_combinations_nodup
set ProductID_max = (case when ProductID___1 > ProductID___2 then ProductID___1 else ProductID___2 end);
*/

optimize local table tmp_eag.thd_allpurch_202301_product_combinations_nodup;

drop table if exists tmp_eag.thd_resp_Brand_ProductID;
create table tmp_eag.thd_resp_Brand_ProductID
(
	Outlet int(11) default 326
    , Brand int(11)
    , ProductID int(11)
    , RespondentID bigint(20)
    , primary key (Brand, ProductID, RespondentID)
    , index idx_b (Brand)
    , index idx_p (ProductID)
    , index idx_r (RespondentID)
);

insert into tmp_eag.thd_resp_Brand_ProductID(Brand, ProductID, RespondentID)
select Brand, ProductID, RespondentID
from tmp_eag.us_allpurch_326;

optimize local table tmp_eag.thd_resp_Brand_ProductID;

#tmp_eag.resp_Outlet_ProductID___1 X us_allpurch_202301_product_combinations_nodup BY Outlet_ProductID___1
#tmp_eag.resp_Outlet_ProductID___2 X us_allpurch_202301_product_combinations_nodup BY Outlet_ProductID___2
#where tmp_eag.resp_Outlet_ProductID___1.RespondentID=tmp_eag.resp_Outlet_ProductID___2.RespondentID

-- group by 1, 2, get count, divide by transaction total
drop table if exists tmp_eag.thd_resp_by_brand_product___1_2;
create table tmp_eag.thd_resp_by_brand_product___1_2
(
	Outlet int(11) default 326
    , Brand___1 int(11)
    , ProductID___1 int(11)
    , Brand___2 int(11)
    , ProductID___2 int(11)
    , resp_count___1_2 bigint(20)
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
);

insert into tmp_eag.thd_resp_by_brand_product___1_2(Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count___1_2)
select a.Brand, a.ProductID, b.Brand, b.ProductID, count(a.RespondentID)
from tmp_eag.us_allpurch_326 as a, 
tmp_eag.us_allpurch_326 as b, 
tmp_eag.thd_allpurch_202301_product_combinations_nodup as c
where a.RespondentID=b.RespondentID
	and a.Brand=c.Brand___1
    and a.ProductID=c.ProductID___1
    and b.Brand=c.Brand___2
    and b.ProductID=c.ProductID___2
group by a.Brand, a.ProductID, b.Brand, b.ProductID;



-- test for dups
-- no dups
select * from tmp_eag.thd_resp_by_brand_product___1_2
where (Brand___1=10 and ProductID___1=1) and (Brand___2=872 and ProductID___2=960)
or (Brand___2=10 and ProductID___2=1) and (Brand___1=872 and ProductID___1=960);



drop table if exists tmp_eag.thd_allpurch_202301_basket_analysis;
create table tmp_eag.thd_allpurch_202301_basket_analysis
(
	Period int(11) default 202301
    , Outlet int(11) default 326
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
    , probability___2_1 decimal(19,6) default 0
    , probability___1_2 decimal(19,6) default 0
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_op1(Brand___1, ProductID___1)
    , index idx_op2 (Brand___2, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_o1(Brand___1)
    , index idx_o2(Brand___2)
);
    
insert into tmp_eag.thd_allpurch_202301_basket_analysis(Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count___1_2)
select Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count___1_2
from tmp_eag.thd_resp_by_brand_product___1_2;

update tmp_eag.thd_allpurch_202301_basket_analysis as a,
(
	select Brand, ProductID, count(RespondentID) as resp_count___1
	from tmp_eag.us_allpurch_326
    group by Brand, ProductID
) as b
set a.resp_count___1 = b.resp_count___1
where a.ProductID___1 = b.ProductID
	and a.Brand___1 = b.Brand;

update tmp_eag.thd_allpurch_202301_basket_analysis as a,
(
	select Brand, ProductID, count(RespondentID) as resp_count___2
	from tmp_eag.us_allpurch_326
    group by Brand, ProductID
) as b
set a.resp_count___2 = b.resp_count___2
where a.ProductID___2 = b.ProductID
	and a.Brand___2 = b.Brand;

update tmp_eag.thd_allpurch_202301_basket_analysis as a,
(
	select count(distinct RespondentID) as resp_count_total
	from tmp_eag.us_allpurch_326
) as b
set a.resp_count_total = b.resp_count_total;

update tmp_eag.thd_allpurch_202301_basket_analysis
set resp_count_pct___1 = resp_count___1/resp_count_total * 100;

update tmp_eag.thd_allpurch_202301_basket_analysis
set resp_count_pct___2 = resp_count___2/resp_count_total * 100;

update tmp_eag.thd_allpurch_202301_basket_analysis
set resp_count_pct___1_2 = resp_count___1_2/resp_count_total * 100;

-- P(ProductID___2 | ProductID___1)
update tmp_eag.thd_allpurch_202301_basket_analysis
set probability___2_1 = resp_count_pct___1_2 / resp_count_pct___1;

-- P(ProductID___1 | ProductID___2)
update tmp_eag.thd_allpurch_202301_basket_analysis
set probability___1_2 = resp_count_pct___1_2 / resp_count_pct___2;


drop table if exists tmp_eag.thd_allpurch_202301_basket_analysis_lbl;
create table tmp_eag.thd_allpurch_202301_basket_analysis_lbl
(
	Period int(11) default 202301
    , Outlet int(11) default 326
    , Brand___1 int(11)
    , lbl_Brand___1 varchar(64)
    , ProductID___1 int(11)
    , lbl_ProductID___1 varchar(64)
    , Brand___2 int(11)
    , lbl_Brand___2 varchar(64)
    , ProductID___2 int(11)
    , lbl_ProductID___2 varchar(64)
    , resp_count_pct___1_2 decimal(6,3) default 0
    , probability___2_1 decimal(6,3) default 0
    , probability___1_2 decimal(6,3) default 0
    , primary key (Brand___1, ProductID___1, Brand___2, ProductID___2)
    , index idx_op1(Brand___1, ProductID___1)
    , index idx_op2 (Brand___2, ProductID___2)
    , index idx_p1(ProductID___1)
    , index idx_p2(ProductID___2)
    , index idx_o1(Brand___1)
    , index idx_o2(Brand___2)
);

insert into tmp_eag.thd_allpurch_202301_basket_analysis_lbl(Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count_pct___1_2, probability___2_1, probability___1_2)
select Brand___1, ProductID___1, Brand___2, ProductID___2, resp_count_pct___1_2, probability___2_1, probability___1_2
from tmp_eag.thd_allpurch_202301_basket_analysis;

update tmp_eag.thd_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as ProductID___1, FormatLabel as lbl_ProductID___1
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ProductID___1 = b.lbl_ProductID___1
where a.ProductID___1=b.ProductID___1;

update tmp_eag.thd_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as ProductID___2, FormatLabel as lbl_ProductID___2
	from tq_admin.vw_formats where FormatID = 109
) as b
set a.lbl_ProductID___2 = b.lbl_ProductID___2
where a.ProductID___2=b.ProductID___2;

update tmp_eag.thd_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as Brand___1, FormatLabel as lbl_Brand___1
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.lbl_Brand___1 = b.lbl_Brand___1
where a.Brand___1=b.Brand___1;

update tmp_eag.thd_allpurch_202301_basket_analysis_lbl as a, (
	select FormatValue as Brand___2, FormatLabel as lbl_Brand___2
	from tq_admin.vw_formats where FormatID = 1298
) as b
set a.lbl_Brand___2 = b.lbl_Brand___2
where a.Brand___2=b.Brand___2;


select * from tmp_eag.thd_allpurch_202301_basket_analysis_lbl
order by resp_count_pct___1_2 desc;

select * from tmp_eag.thd_allpurch_202301_basket_analysis_lbl
where Brand___1 != Brand___2
order by resp_count_pct___1_2 desc;
