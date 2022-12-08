SELECT*FROM Covid_database.covid_deaths_data
ORDER BY 3,4;

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM Covid_database.covid_deaths_data
ORDER BY 1,2;

-- Looking total cases vs total deaths 
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM Covid_database.covid_deaths_data
ORDER BY 1,2;

-- Looking tat percetage of death in the united states 
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_percentage
FROM Covid_database.covid_deaths_data
WHERE location LIKE "%states%"
ORDER BY 1,2;

-- what percentage of the population got covid
SELECT location, date, population, total_cases,ROUND(((total_cases/population) *100), 6) AS percentage_of_poputation_infected
FROM Covid_database.covid_deaths_data
ORDER BY 1,2;

-- looking at top 10 countries with the highest infection rate relative to population
SELECT location, population, MAX(total_cases)AS Highest_infection_count, MAX((total_cases/population))*100 AS percentage_of_poputation_infected
FROM Covid_database.covid_deaths_data
GROUP BY location, population
ORDER BY percentage_of_poputation_infected DESC
LIMIT 10
;

-- showing countries with the highest death count 
SELECT location,MAX(cast(total_deaths as float)) AS Total_death_count
FROM Covid_database.covid_deaths_data
WHERE continent is not null
GROUP BY location
ORDER BY Total_death_count DESC
;

-- highest death count by continent
SELECT continent, MAX(cast(total_deaths as float)) AS Total_death_count
FROM Covid_database.covid_deaths_data
WHERE continent is not null
GROUP BY continent
ORDER BY Total_death_count DESC
;

-- Global outcomes 

-- global death percentage by date
USE Covid_database;
SELECT date, SUM(new_cases) as total_case,
SUM(cast(new_deaths as float)) as total_deaths, 
SUM(cast(new_deaths as float))/SUM(new_cases) *100 AS Death_percentage
FROM Covid_database.covid_deaths_data
WHERE continent is not null
GROUP BY date
ORDER BY 1,2;

-- overall death percentage 
SELECT SUM(new_cases) as total_case,
SUM(cast(new_deaths as float)) as total_deaths, 
SUM(cast(new_deaths as float))/SUM(new_cases) *100 AS Death_percentage
FROM Covid_database.covid_deaths_data
WHERE continent is not null
ORDER BY 1,2;


-- Total population vs vaccinations 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
FROM Covid_database.covid_deaths_data dea
JOIN Covid_database.CovidVaccination_data vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2,3
;

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as FLOAT)) OVER (partition by dea.location order by dea.location, dea.date)
AS RollingPeopleVaccinated
FROM Covid_database.covid_deaths_data dea
JOIN Covid_database.CovidVaccination_data vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2,3
;

-- USE CTE 
WITH PopulationVsVaccination(Continent,Location, Date, Population, New_vaccinations, RollingPeopleVaccinated)
AS
(SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(cast(vac.new_vaccinations as FLOAT)) OVER (partition by dea.location order by dea.location, dea.date)
AS RollingPeopleVaccinated
FROM Covid_database.covid_deaths_data dea
JOIN Covid_database.CovidVaccination_data vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
)
SELECT*, (RollingPeopleVaccinated/population)*100
FROM PopulationVsVaccination;



