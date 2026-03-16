--Your Tasks

--1. First download data from here
-- I download all of them


--database yaratish

create database BikeStore
go

use BikeStore
go

--2. Ingest & Transform
--Create a clean schema with proper keys, types, and constraints
--clean schema yaratish -> agar hamma schema dbo ichida bolsa,tartibszlik,securityni boshqarish qiyin va katta loyihalarda chalkashlik boladi

create schema stg;              --staging(raw csv data)
go

create schema core;             --clean normalized table
go

create schema rpt;              --reporting layer
go



--CSV -> stg (constraints yo‘q) -> data validation -> core tables (constraints bilan)
--Use BULK INSERT or OPENROWSET to load all .csv files into staging tables

drop table if exists stg.products 

create table stg.products 
      (product_id INT,
	  product_name VARCHAR (255),
	  brand_id INT,
	  category_id INT,
	  model_year INT,
	  list_price DECIMAL (10,2));


BULK INSERT stg.products
from 'c:\temp\products.csv'
with (firstrow=2,                 --csv fayllarning birnchi qatori header bogani uchun 2 dan boshlanadi
     fieldterminator=',',
	 rowterminator='\n');


select*from stg.products


Drop table if exists stg.orders

create table stg.orders
    (order_id VARCHAR(50),
    customer_id VARCHAR(50),
	order_status VARCHAR(50),
	order_date VARCHAR(50),
	required_date VARCHAR(50),
	shipped_date VARCHAR(50),
	store_id VARCHAR(50),
    staff_id VARCHAR(50));


BULK INSERT stg.orders
from 'c:\temp\orders.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');


select*from stg.orders





create table stg.order_items
    (order_id INT,
	item_id INT,
	product_id INT,
	quantity INT,
	list_price DECIMAL(10,2),
	discount DECIMAL (5,2));



BULK INSERT stg.order_items
from 'c:\temp\order_items.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');

select*from stg.order_items



create table stg.customers
     (customer_id INT,
	 first_name VARCHAR (50),
	 last_name VARCHAR (50),
	 phone VARCHAR (20),
	 email VARCHAR (100),
	 street VARCHAR (100),
	 city VARCHAR (50),
	 state VARCHAR (50),
	 zip_code VARCHAR (20));


BULK INSERT stg.customers
from 'c:\temp\customers.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');


select*from stg.customers


create table stg.stores
     (store_id INT,
	 store_name VARCHAR (50),
	 phone VARCHAR (20),
	 email VARCHAR (100),
	 street VARCHAR (100),
	 city VARCHAR (50),
	 state VARCHAR (50),
	 zip_code VARCHAR (20));


BULK INSERT stg.stores
from 'c:\temp\stores.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');


select*from stg.stores

drop table if exists stg.staffs

create table stg.staffs
    (staff_id INT,
	first_name VARCHAR (50),
	last_name VARCHAR (50),
	email VARCHAR (100),
	phone VARCHAR (20),
	active BIT,                                  --BIT- data type bolib 0 yoki 1 qiymat saqlaydi
	store_id INT,
	manager_id VARCHAR(50));


BULK INSERT stg.staffs
from 'c:\temp\staffs.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');


select*from stg.staffs


create table stg.stocks
     (store_id INT,
	 product_id INT,
	 quantity INT);


BULK INSERT stg.stocks
from 'c:\temp\stocks.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');

select*from stg.stocks


create table stg.brands
     (brand_id INT,
	 brand_name VARCHAR (50));


BULK INSERT stg.brands
from 'c:\temp\brands.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');

select*from stg.brands


create table stg.categories
     (category_id INT,
	 category_name VARCHAR(50));



BULK INSERT stg.categories
from 'c:\temp\categories.csv'
with (firstrow=2,
     fieldterminator=',',
	 rowterminator='\n');


select * from stg.categories


--Normalize and transfer data into final tables using INSERT INTO ... SELECT logic…

--PRIMARY KEY – unique ID boladi
--NOT NULL – bosh bolishi mumkin emas
--UNIQUE – takrorlanmaydi
--FOREIGN KEY – boshqa jadvalga bog‘lanadi



drop table if exists products
create table products 
      (product_id INT PRIMARY KEY,
	  product_name VARCHAR (255) NOT NULL,
	  brand_id INT NOT NULL,
	  category_id INT NOT NULL,
	  model_year INT,
	  list_price DECIMAL (10,2));

select*from products 



drop table if exists orders

create table orders
    (order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
	order_status VARCHAR(50),
	order_date DATE NOT NULL,
	required_date DATE,
	shipped_date DATE ,
	store_id VARCHAR(50),
    staff_id VARCHAR(50));



drop table if exists order_items

create table order_items
    (order_id INT NOT NULL,
	item_id INT NOT NULL,
	product_id INT NOT NULL,
	quantity INT,
	list_price DECIMAL(10,2),
	discount DECIMAL (5,2));



drop table if exists customers

create table customers
     (customer_id INT PRIMARY KEY,
	 first_name VARCHAR (50) NOT NULL,
	 last_name VARCHAR (50) NOT NULL,
	 phone VARCHAR (20) UNIQUE,
	 email VARCHAR (100) UNIQUE,
	 street VARCHAR (100),
	 city VARCHAR (50) NOT NULL,
	 state VARCHAR (50),
	 zip_code VARCHAR (20));



drop table if exists stores

create table stores
     (store_id INT PRIMARY KEY,
	 store_name VARCHAR (50) NOT NULL,
	 phone VARCHAR (20),
	 email VARCHAR (100) UNIQUE,
	 street VARCHAR (100),
	 city VARCHAR (50),
	 state VARCHAR (50),
	 zip_code VARCHAR (20));



drop table if exists staffs

create table staffs
    (staff_id INT PRIMARY KEY,
	first_name VARCHAR (50) NOT NULL,
	last_name VARCHAR (50) NOT NULL,
	email VARCHAR (100) UNIQUE,
	phone VARCHAR (20),
	active BIT,                               
	store_id INT,
	manager_id VARCHAR(50));




drop table if exists stocks

create table stocks
     (store_id INT,
	 product_id INT,
	 quantity INT NOT NULL
	 PRIMARY KEY (store_id, product_id));                  --composite primary key , store_id + product_id birga unique bo‘ladi




drop table if exists brands

create table brands
     (brand_id INT PRIMARY KEY,
	 brand_name VARCHAR (50) NOT NULL UNIQUE);



drop table if exists categories

create table categories
     (category_id INT PRIMARY KEY,
	 category_name VARCHAR(50) NOT NULL UNIQUE );
go



--3. Views (Minimum 6)
--Create views that serve as automated reports for business users:
--Each view should be reusable, clean, and production-grade


--current database ichida view yaratiladi yani hozr qaysi database ni ishlatyotgan bosam
--view statementda batch ichida yagona bolishi kk shunchun go bn toxtatamz go=batch delimiter



--•	vw_StoreSalesSummary: Revenue, #Orders, AOV per store
--Dokon boyicha jami daromad, buyurtmalar soni va ortacha buyurtma qiymatini (AOV) hisoblash.
drop view if exists vw_StoreSalesSummary
go

create view vw_StoreSalesSummary as
select 
      s.store_id,
	  count(DISTINCT o.order_id) as total_orders,              --uplicate hsoblanb ketmaslgi uchun
	  sum (oi.quantity*oi.list_price) as total_revenue,        --har bir product narxi bn sonini kopaytrb revenue hsobladk
	  avg (oi.quantity*oi.list_price) as aov
	  from orders o
	  join order_items oi on o.order_id=oi.order_id           --orderdagi productlan olish uchun
	  join stores s on o.store_id=s.store_id                  --qaysi store sotganini biish uchun
	  group by s.store_id;
	  go

select*from vw_StoreSalesSummary
go



--•	vw_TopSellingProducts: Rank products by total sales
--eng kop sotilgan productlani korstadi

drop view if exists vw_TopSellingProducts
go

create view vw_TopSellingProducts as
select
p.product_name,
sum(oi.quantity) as total_sold                --product nechta sotilganini 
from order_items oi
join products p on oi.product_id=p.product_id    --product nomini olish uchun
group by p.product_name;
go

--view ichida order by ishlatish mumknmas

select*from vw_TopSellingProducts
order by total_sold DESC;
go


--•	vw_InventoryStatus: Items running low on stock
--stock da kam qolgan productlani korstadi

create view vw_InventoryStatus as
select
p.product_name,
s.store_id,
st.quantity
from stocks st
join products p on st.product_id=p.product_id
join stores s on st.store_id=s.store_id                   --qaysi store da ekanini korsh uchun
where st.quantity<10;                                     --standart example 10 dan kam bolsa 
go

select*from stg.stocks
go


--•	vw_StaffPerformance: Orders and revenue handled per staff
--har bir staff qancha savdo qilganini korstadi

create view vw_StaffPerformance as
select 
s.staff_id,
count (o.order_id) as total_orders,                   --staff nechta order qlganini hsoblash uchun
sum (oi.quantity*oi.list_price) as total_revenue
from staffs s
join orders o on s.staff_id=o.staff_id                 --staff qaysi orderlani bajarganini olish uchun
join order_items oi on o.order_id=oi.order_id          --revenue hsoblash uchun
group by s.staff_id;
go


--•	vw_RegionalTrends: Revenue by city or region
--qaysi shahar koproq savdo qilganini topsh

create view vw_RegionalTrends as
select
st.city,
sum (oi.quantity*oi.list_price) as total_revenue   --shahrdagi jami revenue
from orders o 
join order_items oi on o.order_id=oi.order_id
join stores st on o.store_id=st.store_id               --1-store qaysi shaharda ekanini topdm
group by st.city;
go


--•	vw_SalesByCategory: Sales volume and margin by product category
--category boyicha volume bn margin sotuvini korstsh kk
--margin=revenue-discount             (quantity*price)-(quantity*price*discount)
create view vw_SalesByCategory as
select 
c.category_name,
sum(oi.quantity) as sales_volume,
sum ((oi.quantity* oi.list_price) - (oi.quantity* oi.list_price*oi.discount)) as total_margin
from order_items oi 
join products p on oi.product_id=p.product_id
join categories c on p.category_id=c.category_id
group by c.category_name;
go



----4.Stored Procedures (Minimum 4)
--Build procedures that automate insights or workflows, such as:


--•	sp_CalculateStoreKPI: Input store ID, return full KPI breakdown
--store uchun kpi hsoblash kk
drop procedure if exists sp_CalculateStoreKPI
go

select*from order_items
select *from orders
go



create procedure sp_CalculateStoreKPI
@StoreID INT                                     --parameter yani qaysi storeni tekshrmoqchi bosam execda osha id ni beraman
AS 
BEGIN

select 
count (DISTINCT o.order_id) as total_orders,                                    --duplicat product qmaslik uchun distinct, nechta buyurtma bolganini hsoblidi
sum (oi.quantity* oi.list_price * (1-oi.discount)) as total_revenue,            --revenue=quantity*price*(1-discount), sotuvdan kelgan money
avg(oi.quantity* oi.list_price * (1-oi.discount)) as avg_order_value,            --ortacha bitta order puli qancha boganligi, customer spending analizi uchun
sum (oi.quantity) as total_sold_items
from orders o
join order_items oi on o.order_id=oi.order_id
where o.store_id= @StoreID                                                      --only tanlab bergan store uchun 
end;


exec sp_CalculateStoreKPI @StoreID=1;




--•	sp_GenerateRestockList: Output low-stock items per store
--store da kam qogan mahsulotlani topamz

select*from stocks
go


create procedure sp_GenerateRestockList
as
begin

select * from stocks
where quantity <10
end;

exec sp_GenerateRestockList;
go


--•	sp_CompareSalesYearOverYear: Compare sales between two years
--2 ta yil savdosini taqqosla

select*from orders
select*from order_items
go


create procedure sp_CompareSalesYearOverYear
@year1 INT,
@year2 INT
as
begin

select year (o.order_date) as sales_year,                     --order qilgan yilimni ajratdm
sum (oi.quantity*oi.list_price* (1- oi.discount)) as total_revenue
from orders o
join order_items oi on o.order_id=oi.order_id 
where year (o.order_date) in (@year1,@year2)              
group by year (o.order_date)
end;


exec sp_CompareSalesYearOverYear 2017,2018;
go


--•	sp_GetCustomerProfile: Returns total spend, orders, and most bought items
--umumi spend, orders va eng kop bought qlngan itemslani topamz


select*from order_items
select*from orders
select*from products
go


create procedure sp_GetCustomerProfile
@customer_id INT 
as
begin

select o.customer_id,
count(DISTINCT o.order_id) as total_orders,
sum (oi.quantity*oi.list_price* (1- oi.discount)) as total_spend,
max (p.product_name) as most_bought_product
from orders o
join order_items oi on oi.order_id=o.order_id
join products p on oi.product_id=p.product_id
where o.customer_id=@customer_id
group by o.customer_id
end;


exec sp_GetCustomerProfile 2;
go





--5. Business KPIs

--You must define and calculate the following KPIs using your Views and SPs:
--Explain each KPIs importance from a business perspective and how it is implemented (via view, SP, or both).



--KPI	Business Insight
--kpi-key performance indicators -kompaniya rahbariga savdo samaradorligi, boshqaruvni tushuntrshga yordam beradi




--Total Revenue	Company-wide performance
--kompaniya umumiy daromadini hsobladk-bu orqali komp samaradorligi va biznes osishini baholashmz mumkn


create view vw_totalrevenue as
select 
sum (quantity*list_price*(1-discount)) as total_revenue
from order_items;
go

create procedure sp_totalrevenue 
as
begin

select 
sum(quantity * list_price *(1-discount)) as total_revenue
from order_items
end;
go

exec sp_totalrevenue 
go


--Average Order Value (AOV)	Customer spending behavior
--mijozlar bir buyurtmada ortacha qancha pul sarflashini hsoblaymz-marketingni improve qlsh va sales performanse ni oshrsh uchun
--aov=total revenue/number of orders


create view vw_averageordervalue as
select
sum(oi.quantity*oi.list_price)/count(distinct oi.order_id) as oav
from orders o
join order_items oi
on o.order_id=oi.order_id;
go


create procedure sp_averageordervalue
as
begin

select
sum(oi.quantity*oi.list_price)/count(distinct oi.order_id) as oav
from orders o
join order_items oi
on o.order_id=oi.order_id
end;

exec sp_averageordervalue
go



--Inventory Turnover	Efficiency of stock flow
--ombordagi mahsulotlar- qanchalik tez sotilayotganini korstadi
--inventory turnover=unit sold/average inventory


create view vw_inventoryturnover as
select
sum (oi.quantity)/avg (st.quantity) as inventory_turnover
from order_items oi
join stocks st on oi.product_id=st.product_id;
go




create procedure sp_inventoryturnover 
as
begin
select
sum (oi.quantity)/avg (st.quantity) as inventory_turnover
from order_items oi
join stocks st on oi.product_id=st.product_id
end

exec sp_inventoryturnover 
go


--Product Return Rate	(if applicable) – Quality issues?
--datasetda return status yoki return table yoq shunchun hsoblab bolmaydi
--return rate=returned orders/total orders*100




--Revenue by Store	Identifies top/weak branches
--har bir filial qancha daromad keltryotganini hsoblaymz

create view vw_revenuebystore as
select
sum(oi.quantity*oi.list_price) as revenue
from orders o
join order_items oi on o.order_id=oi.order_id
join stores s on o.store_id=s.store_id;
go



create procedure sp_revenuebystore
as
begin

select
s.store_name,
sum(oi.quantity*oi.list_price) as revenue
from orders o
join order_items oi on o.order_id=oi.order_id
join stores s on o.store_id=s.store_id
group by s.store_name
end;


exec sp_revenuebystore
go


--Gross Profit by Category	High/low margin areas
--har bir categoriya boyicha formulani hsoblaymz qaysi category kop foyda keltryotganini korstsh uchun
--gross profit=total revenue-cost  cost=70%
--real cost data was not available, that is why i used estimated cost based on industry standard bi assumptions



create view vw_grossprofitbycategory as
select c.category_name,
sum(oi.quantity*oi.list_price) as total_revenue,
sum(oi.quantity*oi.list_price*0.7) as estimated_cost,
sum(oi.quantity*oi.list_price) -sum(oi.quantity*oi.list_price*0.7) as gross_profit
from order_items oi
join products p on oi.product_id = p.product_id
join categories c on p.category_id = c.category_id
group by c.category_name;
go

create procedure sp_grossprofitbycategory 
as
begin

select*from vw_grossprofitbycategory
end;

exec sp_grossprofitbycategory
go




--Sales by Brand	Vendor effectiveness
--qaysi brend eng kop sotlyotganini aniqlaymz-mahshur brend ->market supporting, kam sotilgan brand->review qlsh kk

create view vw_salesbybrand as
select b.brand_name,
sum(oi.quantity) as total_sales
from order_items oi
join products p on oi.product_id=p.product_id
join brands b on p.brand_id=b.brand_id
group by b.brand_name;

go


create procedure sp_salesbybrand 
as
begin

select b.brand_name,
sum(oi.quantity) as total_sales
from order_items oi
join products p on oi.product_id=p.product_id
join brands b on p.brand_id=b.brand_id
group by b.brand_name
end;

exec sp_salesbybrand
go


--staff revenue contribution productivity tracking
--har bir xodim qancha daromad keltrganini korstamz, staff performance va employee productivityni baholaymz
--yuqori revenue keltrgan staff->bonus yoki promotion(discount,1+1,  e.g) beramz


create view vw_staffrevenuecontribution as
select 
sum(oi.quantity*oi.list_price) as revenue_total
from staffs st
join orders o on st.staff_id=o.staff_id
join order_items oi on o.order_id=oi.order_id;
go


create procedure sp_staffrevenuecontribution 
as
begin

select st.first_name,
sum(oi.quantity*oi.list_price) as revenue_total
from staffs st
join orders o on st.staff_id=o.staff_id
join order_items oi on o.order_id=oi.order_id
group by st.first_name
end;
go

exec sp_staffrevenuecontribution 
go



--6. Automation
--•	Create a SQL Agent Job that:
--o	Loads .csv files from a folder daily/weekly
--o	Runs at least 2 stored procedures
--o	Saves results in audit or reporting tables
--•	Include a .txt of your SQL Agent job configuration steps or screenshots
--agent job- sql serverda avtomatik ishga tushadgan vazifa-csv fayl import qlishi, sp ishga tushirishi.. mumkn

--table job natijalarini save
create table audit_job_log
(log_id INT IDENTITY (1,1) PRIMARY KEY,
job_name VARCHAR(100),
run_date DATETIME default GETDATE(),
status VARCHAR (50),
rows_affected INT);
go





create procedure sp_loadcsvdata
as
begin

bulk insert staging.products_raw
from 'c:\temp\products.csv'
with
    (firstrow=2,
	fieldterminator=',',
	rowterminator='\n');


insert into audit_job_log(job_name,status,rows_affected)
values ('csv load job','success', @@rowcount)
end;
go


drop procedure if exists sp_staffrevenuecontribution
go

create procedure sp_staffrevenuecontribution 
as
begin

select st.first_name,
sum(oi.quantity*oi.list_price) as revenue_total
from staffs st
join orders o on st.staff_id=o.staff_id
join order_items oi on o.order_id=oi.order_id
group by st.first_name
end;
go

exec sp_staffrevenuecontribution 
go



drop procedure if exists sp_salesbybrand 
go


create procedure sp_salesbybrand 
as
begin

select b.brand_name,
sum(oi.quantity) as total_sales
from order_items oi
join products p on oi.product_id=p.product_id
join brands b on p.brand_id=b.brand_id
group by b.brand_name
end;

exec sp_salesbybrand
go



