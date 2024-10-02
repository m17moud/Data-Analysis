select *
from layoffs ;

create table layoffs_staging 
like layoffs;

select *
from layoffs_staging ;

insert layoffs_staging 
select * 
from layoffs ;

select * ,
ROW_NUMBER () OVER(partition by company , location , industry , total_laid_off, percentage_laid_off , `date`, stage, country, funds_raised_millions ) as row_num 
from layoffs_staging ;

with cte as
(
select * ,
ROW_NUMBER () OVER(partition by company , location , industry , total_laid_off, percentage_laid_off , `date`, stage, country, funds_raised_millions ) as row_num 
from layoffs_staging 
)
select * 
from cte 
where row_num > 1 ;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
  ,`row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2 ;

insert into  layoffs_staging2 
select * ,
ROW_NUMBER () OVER(partition by company , location , industry , total_laid_off, percentage_laid_off , `date`, stage, country, funds_raised_millions ) as row_num 
from layoffs_staging ;

SET SQL_SAFE_UPDATES = 0; 

delete
from layoffs_staging2 
where row_num > 1 ;



select *
from layoffs_staging2 
;


select distinct company , trim(company) 
from layoffs_staging2 ;

update layoffs_staging2
set company = trim(company);

select distinct industry 
from layoffs_staging2
order by 1;
 
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'
;

select * 
from  layoffs_staging2 ;

select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(country);

update layoffs_staging2 
set country = 'United States'
where country like 'United States%';

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select *
from layoffs_staging2 
;
alter table layoffs_staging2
modify column `date` DATE ;


select company , industry
from layoffs_staging2 
where industry = null ; 

update layoffs_staging2 
set industry = null 
where  industry = ''; 

select   t1.industry , t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
      on t1.company = t2.company
      and t1.location = t2.location 
where t2.industry is null and t1.industry is not null ;

update layoffs_staging2 t1
join layoffs_staging2 t2
      on t1.company = t2.company
      and t1.location = t2.location 
set t2.industry = t1.industry 
where t2.industry is null and t1.industry is not null ;


delete
from layoffs_staging2 
where industry is null ;

select *
from layoffs_staging2 
where industry is null ;

delete
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

alter table layoffs_staging2 
drop column row_num ;

select company , sum(total_laid_off)
from layoffs_staging2 
group by company 
order by 2 desc;

select industry , sum(total_laid_off)
from layoffs_staging2 
group by industry 
order by 2 desc;

select country , sum(total_laid_off)
from layoffs_staging2 
group by country 
order by 2 desc;

select `date` , sum(total_laid_off)
from layoffs_staging2 
group by `date` 
order by 2 desc;



select company , avg(percentage_laid_off)
from layoffs_staging2 
group by company
order by 2 desc;

select company , sum(funds_raised_millions)
from layoffs_staging2 
group by company 
order by 2 desc;

select company , year(`date`) ,sum(total_laid_off)
from layoffs_staging2 
group by company , year(`date`) 
order by 3 desc;

select company , month(`date`) ,sum(total_laid_off)
from layoffs_staging2 
group by company , month(`date`) 
order by 3 desc;

select company , substring(`date`,1,7) as `date`,sum(total_laid_off)
from layoffs_staging2 
group by company , substring(`date`,1,7) 
order by 3 desc;

with company_year (company , years , total_laid_off ) as 
(
select company , year(`date`) ,sum(total_laid_off)
from layoffs_staging2 
group by company , year(`date`) 
order by 3 desc
)
select * , dense_rank() over(partition by years  order by total_laid_off desc) as ranking
from company_year 
where years is not null 
order by ranking asc ;

