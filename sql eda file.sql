select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or Blank values 
-- 4. Remove any column that is not needed

create table layoff_staging
like layoffs;

select *
from layoffs;

insert layoff_staging
select *
from layoffs;

select *
from layoff_staging;

select *, row_number() over(partition by company, industry, total_laid_off, percentage_laid_off,`date`) as row_num
from layoff_staging;

with duplicate_entry as (
select *, 
row_number() over(partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,stage, country,funds_raised_millions) as row_num
from layoff_staging
)
select *
from duplicate_entry
where row_num>1;

select *
from layoff_staging
where company='Casper';

create table layoff_staging2
like layoffs;

alter table layoff_staging2
add column row_num int;

insert layoff_staging2
select *, 
row_number() over(partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,stage, country,funds_raised_millions) as row_num
from layoff_staging;

SET SQL_SAFE_UPDATES = 0;

delete
from layoff_staging2
where row_num >1;

select *
from layoff_staging2;

-- standardizing data 

select company, trim(company) as comp
from layoff_staging2;

update layoff_staging2
set company=trim(company);

select *
from layoff_staging2
where industry like 'Crypto%';

update layoff_staging2
set industry='crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoff_staging2
order by 1;

update layoff_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';

desc layoff_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set `date`= str_to_date(`date`, '%m/%d/%Y');

alter table layoff_staging2
modify column `date` date;

 select *
 from layoff_staging2
 where industry is null 
 or industry='';
 
-- populate those fields
select *
from layoff_staging2
where company='Airbnb';

select l1.company,l2.company, l1.industry, l2.industry
from layoff_staging2 as l1
join layoff_staging2 as l2 on l1.company=l2.company and l1.location=l2.location 
where (l1.industry is null or l1.industry='')
and (l2.industry is not null  and l2.industry!='');

update layoff_staging2 as l1
join layoff_staging2 as l2 on l1.company=l2.company and l1.location=l2.location 
set l1.industry=l2.industry
where (l1.industry is null or l1.industry='')
and (l2.industry is not null  and l2.industry!='');

select *
from layoff_staging2
where total_laid_off is null 
and
percentage_laid_off is null;

delete
from layoff_staging2
where total_laid_off is null 
and
percentage_laid_off is null;

select *
from layoff_staging2;

alter table layoff_staging2
drop column row_num;

-- Exploratory data analysis

select max(percentage_laid_off)
from layoff_staging2;

select percentage_laid_off, percentage_laid_off*100
from layoff_staging2;

update layoff_staging2
set percentage_laid_off=percentage_laid_off*100;

desc layoff_staging2;

update layoff_staging2
set percentage_laid_off=trim(percentage_laid_off);

alter table layoff_staging2
modify column percentage_laid_off int;

select company,total_laid_off, percentage_laid_off
from layoff_staging2
where percentage_laid_off=100
order by 2 desc;

select company,sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

select industry,sum(total_laid_off)
from layoff_staging2
group by industry
order by 2 desc;

select year(`date`) as yr, sum(total_laid_off)
from layoff_staging2
group by yr
order by 2 desc;

select date_format(`date`,'%Y-%m' ) as yr_month, sum(total_laid_off)
from layoff_staging2
where date_format(`date`,'%Y-%m' ) is not null
group by yr_month
order by 2 desc;

with rolling as (
select date_format(`date`,'%Y-%m' ) as yr_month, sum(total_laid_off) as laid_off
from layoff_staging2
where date_format(`date`,'%Y-%m' ) is not null
group by yr_month
order by 1
)
select yr_month,laid_off, sum(laid_off) over(order by yr_month) as rolling_sum
from rolling;

with company_year as (
select company,year(`date`) as layoff_year, sum(total_laid_off) as total_lay
from layoff_staging2
where year(`date`) is not null
group by company,year(`date`)
), company_rank as (
select *,dense_rank() over(partition by layoff_year order by total_lay desc ) as layoff_rank
from company_year
)
select *
from company_rank
where layoff_rank<=5;

-- data file is in excel
























