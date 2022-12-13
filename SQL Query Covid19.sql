DROP TABLE IF EXISTS covid_death;
CREATE TABLE covid_death (
	iso_code text,
    continent text, 
    location text,
    date date,
    population bigint,
    total_cases bigint,
    new_cases bigint,
    new_cases_smoothed decimal(8,2),
    total_deaths bigint,
    new_deaths int,
    new_deaths_smoothed decimal(8,2),
    total_cases_per_million decimal(8,2),
    new_cases_per_million decimal(8,2),
    new_cases_smoothed_per_million decimal(8,2),
    total_deaths_per_million decimal(8,2),
    new_deaths_per_million decimal(8,2),
    new_deaths_smoothed_per_million decimal(8,2),
    reproduction_rate decimal(3,2),
    icu_patients int,
    icu_patients_per_million decimal(8,2),
    hosp_patients int,
    hosp_patients_per_million decimal(8,2),
    weekly_icu_admissions decimal(8,2),
    weekly_icu_admissions_per_million decimal(8,2),
    weekly_hosp_admissions decimal(8,2),
    weekly_hosp_admissions_per_million decimal(8,2)
    );
    
DROP TABLE IF EXISTS covid_vaccinations;    
CREATE TABLE covid_vaccinations (
	iso_code text,	
	continent text,
	location text,
	date date,
	total_tests bigint,
	new_tests bigint,
	total_tests_per_thousand decimal(8,2),
	new_tests_per_thousand decimal(8,2),
	new_tests_smoothed int,
	new_tests_smoothed_per_thousand decimal(8,2),
	positive_rate decimal(3,2),
	tests_per_case decimal(5,1),
	tests_units text,
	total_vaccinations bigint,
	people_vaccinated bigint,
	people_fully_vaccinated int,
	total_boosters int,
	new_vaccinations int,
	new_vaccinations_smoothed int,
	total_vaccinations_per_hundred decimal(5,2),
	people_vaccinated_per_hundred decimal(5,2),
	people_fully_vaccinated_per_hundred decimal(5,2),
	total_boosters_per_hundred decimal(5,2),
	new_vaccinations_smoothed_per_million int,
	new_people_vaccinated_smoothed int,
	new_people_vaccinated_smoothed_per_hundred decimal(4,2),
	stringency_index decimal(4,2),
	population_density decimal(8,2),
	median_age int,
	aged_65_older decimal(4,2),
	aged_70_older decimal(4,2),
	gdp_per_capita decimal(8,2),
	extreme_poverty decimal(3,1),
	cardiovasc_death_rate decimal(5,2),
	diabetes_prevalence decimal(4,2),
	female_smokers decimal(3,1),
	male_smokers decimal(3,1),
	handwashing_facilities decimal(4,2),
	hospital_beds_per_thousand decimal(4,2),
	life_expectancy decimal(4,2),
	human_development_index decimal(3,2)
    );

load data local infile 'D:\\Portofolio Project\\Covid19 Project\\CovidDeaths.csv'  
into table covid_death
fields terminated by ','
ignore 1 rows;

load data local infile 'D:\\Portofolio Project\\Covid19 Project\\CovidVaccinations.csv'
into table covid_vaccinations
fields terminated by ','
ignore 1 rows;

-----------------------------------------------------------------------------------------------------------------------
-- view data that will be used
SELECT * FROM covid_vaccinations;

SELECT * FROM covid_death;

-----------------------------------------------------------------------------------------------------------------------

-- Select Data that we are going to be starting with
SELECT 
	location, 
    date, 
    total_cases, 
    new_cases, 
    total_deaths, 
    population
FROM covid_death
WHERE continent is not null
ORDER BY 1,2;

-- Total cases vs population
-- Show what percentage population infected with covid
SELECT 
	location,
    date,
    population,
    total_cases,
    (total_cases/population)*100 AS percent_population_infected
FROM covid_death
WHERE location LIKE 'Indonesia' 
AND continent is not null
ORDER BY 1, 2;

-- Countries with Highest Infection Rate compared to Population
SELECT
	location,
    population,
    MAX(total_cases) AS highest_infection_count,
    MAX((total_cases/population))*100 AS percent_population_infected
FROM covid_death
GROUP BY 1, 2
ORDER BY 4 DESC;

-- Death percentage Worldwide
SELECT
	location,
    date,
    total_cases,
    total_deaths,
    (total_deaths/total_cases)*100 as death_percentage
FROM covid_death
WHERE continent is not null
ORDER BY 1, 2;

-- Death percentage in Indonesia
SELECT
	location,
    date,
    total_cases,
    total_deaths,
    (total_deaths/total_cases)*100 as death_percentage
FROM covid_death
WHERE location = 'indonesia'
AND continent is not null
ORDER BY 1, 2;

-- Countries with highest death count
SELECT
	location,
    MAX(total_deaths) AS total_death
FROM covid_death
WHERE continent not like ""
GROUP BY location
ORDER BY total_death DESC;

-- contintents with the highest death count per population
SELECT
	continent,
    MAX(total_deaths) AS total_death
FROM covid_death
WHERE continent not like ""
GROUP BY continent
ORDER BY total_death DESC;

-- Global Number
SELECT
	MAX(total_cases) AS total_cases,
	MAX(total_deaths) AS total_death,
	MAX(total_deaths)/MAX(total_cases)*100 AS death_percentage
FROM covid_death
WHERE location = 'World'
ORDER BY 1, 2;

-- Indonesia Number
SELECT
	MAX(total_cases) AS total_cases,
	MAX(total_deaths) AS total_death,
	MAX(total_deaths)/MAX(total_cases)*100 AS death_percentage
FROM covid_death
WHERE location = 'Indonesia'
ORDER BY 1, 2;

-- Percentage of Population that has recieved at least one Covid Vaccine
SELECT
	cde.continent,
	cde.location,
    cde.date,
    cde.population,
    cva.new_vaccinations,
    SUM(new_vaccinations) OVER (PARTITION BY cde.location ORDER BY cde.location, cde.date) AS total_vaccinations_growth
FROM covid_death AS cde
JOIN covid_vaccinations AS cva
	ON cde.location = cva.location
    AND cde.date = cva.date
WHERE cde.continent IS NOT NULL
ORDER BY 2, 3;

-- Using CTE to perform Calculation on Partition By in previous query
WITH PopvsVac (Continent, Location, Date, Population, New_Vaccinations, total_vaccinations_growth)
AS (
SELECT
	cde.continent,
	cde.location,
    cde.date,
    cde.population,
    cva.new_vaccinations,
    SUM(new_vaccinations) OVER (PARTITION BY cde.location ORDER BY cde.location, cde.date) AS total_vaccinations_growth
FROM covid_death AS cde
JOIN covid_vaccinations AS cva
	ON cde.location = cva.location
    AND cde.date = cva.date
WHERE cde.continent IS NOT NULL
ORDER BY 2, 3 )
SELECT 
	*, 
	(total_vaccinations_growth/Population)*100 AS vaccination_growth_percentage
FROM PopvsVac;

-- Total of Vaccatinations
SELECT
	cde.location,
    cde.population,
    MAX(cva.total_vaccinations) AS total_vaccinations,
    MAX(cva.people_fully_vaccinated) AS people_fully_vaccinated,
    MAX(cva.total_vaccinations/population)*100 AS percentage_people_vaccination
FROM covid_death AS cde
JOIN covid_vaccinations AS cva
	ON cde.location = cva.location
    AND cde.date = cva.date
WHERE cde.continent not like ""
GROUP BY 1,2
ORDER BY 1,2;