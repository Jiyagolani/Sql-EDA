SELECT * FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;
SELECT * FROM layoffs_staging;

INSERT layoffs_staging SELECT * FROM layoffs;

SELECT * , ROW_NUMBER() OVER
(PARTITION BY company , industry ,location , country, 
funds_raised_millions, total_laid_off , 
percentage_laid_off,'date') AS row_num FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT * , ROW_NUMBER() OVER
(PARTITION BY company , location , country, 
funds_raised_millions, industry , total_laid_off , 
percentage_laid_off,'date') AS row_num FROM layoffs_staging
)
SELECT  * FROM duplicate_cte WHERE row_num>1;


SELECT * FROM layoffs_staging WHERE company='Abra';








CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SELECT *
FROM layoffs_staging2 WHERE row_num>1;

INSERT INTO layoffs_staging2
SELECT * , ROW_NUMBER() OVER
(PARTITION BY company , industry ,location , country, 
funds_raised_millions, total_laid_off , 
percentage_laid_off,'date') AS row_num FROM layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1 LIMIT 500000;
SELECT *
FROM layoffs_staging2 ;




SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry='Crpyto' WHERE industry LIKE 'Crypto%' ;

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT country , TRIM(trailing'.' from country)
FROM layoffs_staging2;
UPDATE layoffs_staging2
SET country=TRIM(trailing'.' from country)
WHERE country LIKE 'United States%' ;

select `date`,
str_to_date(`date`,' %m /%d/ %Y')
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date`= str_to_date(`date`,' %m /%d/ %Y');


ALTER TABLE layoffs_staging2
modify column `date` DATE;


SELECT *
FROM layoffs_staging2 WHERE  total_laid_off IS NULL
AND percentage_laid_off IS NULL;
update layoffs_staging2
 set industry=null where industry='';

SELECT *
FROM layoffs_staging2 WHERE industry IS NULL
OR industry= '';

SELECT *
FROM layoffs_staging2 WHERE company like 'Bally%';

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
join layoffs_staging2 t2
    ON t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;



UPDATE layoffs_staging2 t1
join layoffs_staging2 t2
    ON t1.company=t2.company
SET t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

SELECT *
FROM layoffs_staging2 WHERE  total_laid_off IS NULL
AND percentage_laid_off IS NULL;

delete FROM layoffs_staging2 WHERE  total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
drop column row_num;

SELECT *
FROM layoffs_staging2;






















