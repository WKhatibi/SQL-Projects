
##Exploratory Data Analysis
SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT company, SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
GROUP BY company
ORDER BY sum_total DESC;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'OLX%';

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT country, SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
GROUP BY country
ORDER BY sum_total DESC;

SELECT YEAR(`date`), SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY sum_total DESC;

SELECT stage, SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
GROUP BY stage
ORDER BY sum_total DESC;

#rolling sum of layoffs
SELECT substr(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
WHERE substr(`date`, 1, 7) IS NOT NULL
GROUP BY substr(`date`, 1, 7)
ORDER BY 1 ASC;

WITH Rolling_total AS
(
SELECT substr(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substr(`date`, 1, 7) IS NOT NULL
GROUP BY substr(`date`, 1, 7)
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, 
SUM(total_off) OVER(order by  `MONTH`) AS rolling_total
FROM Rolling_total;


SELECT company, YEAR(`date`), SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
order by 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) AS sum_total
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, 
dense_rank() OVER(partition by years order by total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking<= 5;



