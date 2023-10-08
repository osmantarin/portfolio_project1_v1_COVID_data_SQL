SELECT *
FROM PortfolioProject..CovidDeaths
order by 3,4

--SELECT *
--FROM PortfolioProject..CovidVaccinations
--order by 3,4

-- Select data that I will be using

SELECT Location, date, total_cases, new_cases, total_deaths, population 
From PortfolioProject..CovidDeaths
order by 1,2




-- Looking at Total Cases vs Total Deaths 
	--This will examine the percent of indvidiual deaths compared to those that are infected

	--The CAST function was necessary to convert total_deaths and total_cases as decimal values before performing any arithmetic 

	--The new column is labeled "death_rate"

	--The new column shows the likelihood of dying if you contract COVID in your country 

SELECT Location, date, total_cases, total_deaths, 
       CAST(total_deaths AS decimal) / CAST(total_cases AS decimal)*100 AS death_rate
FROM PortfolioProject..CovidDeaths
ORDER BY 1, 2;




--Looking at Total Cases vs Total Deaths
	--We can further examine the death rate by a specific country such as the United States
	
	--The "like" clause was used after the WHERE function to approximate the name search as I was not entirely sure how the United States was named in the data set 


SELECT Location, date, total_cases, total_deaths, 
       (CAST(total_deaths AS decimal) / CAST(total_cases AS decimal))*100 AS death_rate
FROM PortfolioProject..CovidDeaths
WHERE location like '%states%'
ORDER BY 1, 2;




-- Looking at Total Cases vs Population (USA)
	--We can further examine what percentage of the population of the United States has been infected with COVID 

	--The new column is labeled "infection_rate"

SELECT Location, date, Population, total_cases,
       (CAST(total_cases AS decimal) / CAST(Population AS decimal))*100 AS infection_rate
FROM PortfolioProject..CovidDeaths
WHERE location like '%states%'
ORDER BY 1, 2;




-- Looking at Total Cases vs Population (Global)
	--We can also examine global infection rates by removing the "WHERE" clause

SELECT Location, date, Population, total_cases,
       (CAST(total_cases AS decimal) / CAST(Population AS decimal))*100 AS infection_rate
FROM PortfolioProject..CovidDeaths
--WHERE location like '%states%'
ORDER BY 1, 2;



-- Ranking countries by infection rate 
	--We can further examine countries with the highest infection rate compared to the global population 

	--The new column "highest_infection_count" calculates the maximum value of the "total_cases" column within each group
	  ----This corresponds to the highest number of COVID-19 cases recorded for a specific country.

	--We also have another new column labeled "percentage_population_infected"

	--Note we have included a "Group by" clause in this query since we have added the aggregate function "MAX(total_cases)." 
	  ----The database needs instructions on how to group the data before applying the aggregate function

	--We order the database by the percentage of the population that is infected in descending order

SELECT Location, Population, MAX(total_cases) as highest_infection_count,
       (CAST(MAX(total_cases) AS decimal) / CAST(Population AS decimal))*100 AS percentage_population_infected
FROM PortfolioProject..CovidDeaths
--WHERE location like '%states%'
GROUP BY Location, Population 
ORDER BY percentage_population_infected DESC



-- Showing Countries with Highest Death Count per Population 
	--We introduce the "total_death_count" column in order to sort the total death count by location 

	--Note we use the "cast" function to convert total_deaths column to an integer format due to the original data type (nvarchar255)
		--This is due to how the data type is ready when we use the aggregate function
		--We CAST the data so it is read by the aggregate MAX function as numeric, in this case as big integers due to the nature of the numbers 

	--We have also included a revised "WHERE" clause that removes locations and categories that are not countries, such as continents and land masses



SELECT Location, MAX(CAST(total_deaths AS BIGINT)) AS total_death_count
FROM PortfolioProject..CovidDeaths
-- WHERE location like '%states%'
WHERE continent is not null
GROUP BY Location 
ORDER BY total_death_count DESC;



-- Examining the data by continent 
	-- We examine continents with the highest death count per population

	--During my brief initial examination of the dataset, I noticed the continent column features a "NULL" entry when referring to just continents and not countries
		--As a result I included a "WHERE" clause that specifies entries where the continent column is "NULL" in order to only reference continents 

	--Note the "AND location NOT LIKE" addition to the WHERE clause that removes entries related to upper and middle class incomes in the location column
		--The "NOT LIKE" addition to the WHERE clause is also instructed to remove the entry for the global death count 


SELECT location, MAX(CAST(total_deaths AS BIGINT)) AS total_death_count
FROM PortfolioProject..CovidDeaths
-- WHERE location like '%states%'
WHERE continent IS NULL
    AND location NOT LIKE '%income%' 
	AND location NOT LIKE '%world%'
GROUP BY location  
ORDER BY total_death_count DESC;


-- Global Analysis of COVID Data
	--In this query we add the total number of cases and total number of deaths per day
	
	--I converted the "new_deaths" column to an integer format using the "CAST" function as noted previously

	--The "NULLIF" function was included due to days when the total number of cases were zero 
		--Instead of performing division by zero, the function inputs "NULL" into the column 

	--The "WHERE" clause was utilized to exclude COVID cases by continent since they are accounted for by country 


SELECT date, SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) AS total_deaths,
       (SUM(CAST(new_deaths AS INT)) * 100.0 / NULLIF(SUM(new_cases), 0)) AS death_rate
FROM PortfolioProject..CovidDeaths
--WHERE location like '%states%'
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2;



-- Total Deaths to date
	--Removing the "date" column from the previous code will provide the total number of cases globally to date as well as the total number of deaths and the death rate

SELECT SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) AS total_deaths,
       (SUM(CAST(new_deaths AS INT)) * 100.0 / NULLIF(SUM(new_cases), 0)) AS death_rate
FROM PortfolioProject..CovidDeaths
--WHERE location like '%states%'
WHERE continent IS NOT NULL
ORDER BY 1, 2;


-- Looking at Total Population vs Vaccinations
	--I combine two datasets that we examine in order to answer the question:
		--What is the total number of people in the world that are vaccinated? 

	--Note we have assigned an alias to each of the CovidDeaths and CovidVaccinations datasets for this query

	--Since date is included in both tables, we must specify which dataset we want to pull dates from

	--I instructed the "SUM" function to provide a rolling count of new vaccinations 
		--The "OVER (Partition by)" groups the data by location and calculates the sum within each group
		--The "ORDER BY" clause within specifies the order in which the data within each partition will be processed


SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rolling_individuals_vaccinated
	
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location 
	and dea.date = vac.date

where dea.continent is not null
order by 2,3


-- USE CTE
	-- I created a temporary table to combine the data about COVID deaths and vaccinations 
	-- The table calculates the cumulative number of individuals vaccinated over time for each location 
	-- The table also determines the vaccination rate as a percentage of the population 

With PopvsVac (Continent, Location, Date, Population, new_vaccinations, rolling_individuals_vaccinated)

as

(SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rolling_individuals_vaccinated
	
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location 
	and dea.date = vac.date

where dea.continent is not null)
--order by 2,3

SELECT *, (rolling_individuals_vaccinated / Population)*100
FROM PopvsVac




-- TEMP TABLE
Create Table #PercentPopulationVaccinated 
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime, 
Population numeric,
New_vaccinations numeric, 
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rolling_individuals_vaccinated
	--, (RollingPeopleVaccinated/population)*100
	
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location 
	and dea.date = vac.date

where dea.continent is not null
--order by 2,3

SELECT *, (RollingPeopleVaccinated/Population)*100
FROM #PercentPopulationVaccinated 




--Creating View to store data for later visualizations 

Create View PercentPopulationVaccinated as

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
	SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (Partition by dea.Location ORDER BY dea.location, dea.date) AS rolling_individuals_vaccinated
	--, (RollingPeopleVaccinated/population)*100
	
FROM PortfolioProject..CovidDeaths dea
Join PortfolioProject..CovidVaccinations vac
	On dea.location = vac.location 
	and dea.date = vac.date

where dea.continent is not null
--order by 2,3



SELECT *
FROM PercentPopulationVaccinated









