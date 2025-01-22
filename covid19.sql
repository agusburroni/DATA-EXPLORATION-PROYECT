Datasets: https://ourworldindata.org/covid-deaths

-- Covid19 Data Exploration
-- Comenzamos estandarizando algunos tipos de datos, y cambiando blancos por nulos.

UPDATE coviddeaths 
SET 
    total_deaths = NULL
WHERE
    total_deaths = '';

UPDATE coviddeaths 
SET 
    continent = NULL
WHERE
    continent = '';

UPDATE coviddeaths 
SET 
    `date` = STR_TO_DATE(`date`, '%d/%m/%Y');

ALTER TABLE coviddeaths
MODIFY COLUMN `date` DATE;

UPDATE covidvaccinations 
SET 
    `date` = STR_TO_DATE(`date`, '%d/%m/%Y');

UPDATE covidvaccinations
SET new_vaccinations = NULL
where new_vaccinations = '';

ALTER TABLE covidvaccinations
MODIFY COLUMN `date` DATE;

-- Muestra el porcentaje de probabilidad de morir en cada país, ordenado por el porcentaje de muerte.

SELECT
Location,
max(total_cases), 
max(total_deaths),
round((max(total_deaths) / max(total_cases) * 100),2) as percentage_of_death
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY percentage_of_death DESC;

-- Por pura curiosidad busque como fue variando a lo largo del tiempo este mismo porcentaje pero solo en Argentina (Podemos usar esta misma query cambiando unicamente el nombre del pais)

SELECT 
    Location,
    date,
    total_deaths,
    total_cases,
    ROUND((total_deaths / total_cases * 100), 2) AS percentage_of_death
FROM
    coviddeaths
WHERE
    Location = 'Argentina'
ORDER BY date;

-- Muestra el porcentaje de poblacion infectada por el virus en cada país, ordenado por el porcentaje mas alto.

SELECT
    Location,
    population,
    MAX(total_cases) as total_cases,
	ROUND((MAX(total_cases) / population * 100), 2) AS percentage_of_infected
FROM coviddeaths
GROUP BY 1,2
ORDER BY percentage_of_infected desc;

-- El mismo metodo(porcentaje de la poblacion infectada), pero separado por continente para una mejor visualizacion. En este caso vemos Europa.

SELECT 
    Location,
    population,
    MAX(total_cases) AS total_cases,
    ROUND(MAX((total_cases) / population * 100), 2) AS percentage_of_infected
FROM
    coviddeaths
WHERE
    continent = 'Europe'
GROUP BY 1 , 2
ORDER BY percentage_of_infected DESC;

-- Paises con el mayor recuento de muertes

SELECT 
    Location, MAX(CAST(total_deaths AS SIGNED)) AS total_deaths
FROM
    coviddeaths
WHERE
    continent IS NOT NULL
GROUP BY location
ORDER BY total_deaths DESC;

-- Continentes con mas muertes por Covid19 y su porcentaje de muertes segun su poblacion.

SELECT 
    location, population, MAX(CAST(total_deaths AS SIGNED)) AS total_deaths,
    round(max(((total_deaths/population)*100)),2) as percentage_of_death
FROM
    coviddeaths
WHERE
    continent IS NULL AND location != 'European Union'
GROUP BY 1,2
ORDER BY percentage_of_death DESC;

-- Numero total de casos y muertes a nivel mundial.

SELECT 
    SUM(NEW_CASES) AS total_cases,
    SUM(CAST(new_deaths AS SIGNED)) AS total_deaths,
    SUM(CAST(new_deaths AS SIGNED)) / SUM(NEW_CASES) * 100 AS death_percentage
FROM
    coviddeaths
WHERE
    continent IS NOT NULL
ORDER BY 1 , 2;

-- Cantidad total de personas vacunadas en el mundo por pais a lo largo del tiempo + total de vacunaciones hasta el momento

SELECT 
    cd.continent, cd.location, cd.date, population, cv.new_vaccinations, sum(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) as total_location_vaccinations_until_date
    FROM
    coviddeaths cd
        JOIN
    covidvaccinations cv ON cd.location = cv.location
        AND cd.date = cv.date
WHERE
    cd.continent IS NOT NULL and cv.new_vaccinations IS NOT NULL
    ORDER BY cd.location;

    
-- Tabla temporal para visualizar la vacunacion en Sudamerica a lo largo del tiempo + porcentaje de poblacion vacunada. Esto nos servira para tener los datos mas a mano para posteriores consultas.

DROP TABLE IF EXISTS south_america_vaccinations;

CREATE TABLE south_america_vaccinations (
continent VARCHAR(255),
location VARCHAR(255),
date DATETIME,
population NUMERIC,
new_vaccinations NUMERIC,
total_location_vaccinations_until_date NUMERIC
);

INSERT INTO south_america_vaccinations
SELECT
cd.continent,
cd.location,
cd.date,
cd.population,
cv.new_vaccinations,
sum(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) as total_location_vaccinations_until_date
FROM coviddeaths cd
JOIN covidvaccinations cv ON cd.location = cv.location AND cd.date = cv.date
WHERE cd.continent LIKE 'South%' and new_vaccinations IS NOT NULL
ORDER BY cd.location;

SELECT *, round((total_location_vaccinations_until_date / population) * 100,2) as percentage_population_vaccinations FROM south_america_vaccinations;

-- Mismo resultado que la query anterior pero con una CTE

With south_american_vaccinations (continent, location, date, population, new_vaccinations, total_location_vaccinations_until_date)
as (
 SELECT 
     cd.continent, cd.location, cd.date, population, cv.new_vaccinations, sum(cv.new_vaccinations) OVER (PARTITION BY cd.location ORDER BY cd.location, cd.date) as total_location_vaccinations_until_date
    FROM
    coviddeaths cd
        JOIN
    covidvaccinations cv ON cd.location = cv.location
        AND cd.date = cv.date
WHERE cd.continent LIKE 'South%' and
 cv.new_vaccinations IS NOT NULL
ORDER BY cd.location )
  SELECT 
    *, round((total_location_vaccinations_until_date / population) * 100,2) as percentage_population_vaccinations
FROM
    south_american_vaccinations;

-- Creacion de una Vista para su posterior visualizacion

CREATE VIEW percentage_population_death_continent AS
    SELECT 
        location,
        population,
        MAX(CAST(total_deaths AS SIGNED)) AS total_deaths,
        ROUND(MAX(((total_deaths / population) * 100)),
                2) AS percentage_of_death
    FROM
        coviddeaths
    WHERE
        continent IS NULL
            AND location != 'European Union'
    GROUP BY 1 , 2
    ORDER BY percentage_of_death DESC;

SELECT 
    *
FROM
    percentage_population_death_continent;
    
-- Poblacion total y vacunados totales creando CTE

WITH population_vaccinated AS (
SELECT cd.location, population, vaccinations, continent
FROM (
SELECT location, max(population) as population, continent
FROM coviddeaths cd
WHERE continent IS NOT NULL
GROUP BY cd.location, cd.continent
) AS cd
JOIN (
SELECT location, max(total_vaccinations) as vaccinations
FROM covidvaccinations cv
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY location
) AS cv
ON cd.location = cv.location
)
SELECT
sum(population_vaccinated.population) as total_world_population,
sum(population_vaccinated.vaccinations) as total_world_vaccinations,
round((sum(population_vaccinated.vaccinations)/ sum(population_vaccinated.population))*100,2) AS percentage_vaccinated
FROM population_vaccinated;
