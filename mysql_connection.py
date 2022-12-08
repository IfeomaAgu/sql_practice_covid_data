from pickle import TRUE
import pymysql
import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

engine = sqlalchemy.create_engine("mysql+pymysql://root:password@localhost:3306/Covid_database")


# load environment variables from .env file
load_dotenv()
host = os.environ.get("mysql_host")
user = os.environ.get("mysql_user")
password = os.environ.get("mysql_pass")
database = os.environ.get("mysql_db")


# establish a database connection
def connect_to_database(): #call it whe you need it
    return pymysql.connect(
    host = host,
    user = user,
    password = password,
    database = database
)


def add_to_db(command):
    """ gets stuff from a db """
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(f"{command}") 
    connection.commit()
    cursor.close()
    connection.close()


def get_from_db(command):
    """ gets stuff from a db, returns the result """
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(f"{command}") 
    myresult = cursor.fetchall()
    connection.commit()
    return(myresult)

def create_covid_death_table():
    create_table_query = "CREATE TABLE covid_deaths_data\
    (\
    iso_code VARCHAR(10) NOT NULL PRIMARY KEY,\
    continent VARCHAR(50) NOT NULL,\
    location VARCHAR(50) NOT NULL,\
    date DATE NOT NULL, population FLOAT NOT NULL,\
    total_cases FLOAT NOT NULL, \
    new_cases FLOAT NOT NULL,\
    new_cases_smoothed FLOAT,\
    total_deaths FLOAT,\
    new_deaths_smoothed FLOAT,\
    new_cases_per_million FLOAT,\
    new_cases_smoothed_per_million FLOAT,\
    total_deaths_per_million FLOAT,\
    new_deaths_per_million FLOAT,\
    new_deaths_smoothed_per_million FLOAT,\
    reproduction_rate FLOAT,icu_patients FLOAT,\
    icu_patients_per_million FLOAT,\
    hosp_patients FLOAT,\
    hosp_patients_per_million FLOAT,\
    weekly_icu_admissions FLOAT,\
    weekly_icu_admissions_per_million FLOAT,\
    weekly_hosp_admissions FLOAT,\
    weekly_hosp_admissions_per_million FLOAT\
    );"         
    add_to_db(create_table_query)

# create_covid_death_table()


def create_covid_data_table():
    create_table_query = "CREATE TABLE covid_data\
    (\
    iso_code VARCHAR(10) NOT NULL PRIMARY KEY,\
    continent VARCHAR(50) NOT NULL,\
    location VARCHAR(50) NOT NULL,\
    date DATE NOT NULL,\
    total_cases FLOAT NOT NULL,\
    new_cases FLOAT NOT NULL,\
    new_cases_smoothed VARCHAR(255),\
    total_deaths FLOAT, new_deaths FLOAT,\
    new_deaths_smoothed FLOAT,\
    total_cases_per_million FLOAT,\
    new_cases_per_million FLOAT,\
    new_cases_smoothed_per_million FLOAT,\
    total_deaths_per_million FLOAT,\
    new_deaths_per_million FLOAT,\
    new_deaths_smoothed_per_million FLOAT,\
    reproduction_rate FLOAT,\
    icu_patients FLOAT,\
    icu_patients_per_million FLOAT,\
    hosp_patients FLOAT,\
    hosp_patients_per_million FLOAT,\
    weekly_icu_admissions FLOAT,\
    weekly_icu_admissions_per_million FLOAT,\
    weekly_hosp_admissions FLOAT,\
    weekly_hosp_admissions_per_million FLOAT,\
    total_tests FLOAT,new_tests FLOAT,\
    total_tests_per_thousand FLOAT,\
    new_tests_per_thousand FLOAT,\
    new_tests_smoothed FLOAT,\
    new_tests_smoothed_per_thousand FLOAT,\
    positive_rate FLOAT,\
    tests_per_case FLOAT,\
    tests_units FLOAT,\
    total_vaccinations FLOAT,\
    people_vaccinated FLOAT,\
    people_fully_vaccinated FLOAT,\
    total_boosters FLOAT,\
    new_vaccinations FLOAT,\
    new_vaccinations_smoothed FLOAT,\
    total_vaccinations_per_hundred FLOAT,\
    people_vaccinated_per_hundred FLOAT,\
    people_fully_vaccinated_per_hundred FLOAT,\
    total_boosters_per_hundred FLOAT,\
    new_vaccinations_smoothed_per_million FLOAT,\
    new_people_vaccinated_smoothed FLOAT,\
    new_people_vaccinated_smoothed_per_hundred FLOAT,\
    stringency_index FLOAT,population_density FLOAT,\
    median_age FLOAT,aged_65_older FLOAT,\
    aged_70_older FLOAT,\
    gdp_per_capita FLOAT,\
    extreme_poverty FLOAT,\
    cardiovasc_death_rate FLOAT,\
    diabetes_prevalence FLOAT,\
    female_smokers FLOAT,\
    male_smokers FLOAT,\
    handwashing_facilities FLOAT,\
    hospital_beds_per_thousand FLOAT,\
    life_expectancy FLOAT,\
    human_development_index FLOAT,\
    population FLOAT NOT NULL,\
    excess_mortality_cumulative_absolute FLOAT,\
    excess_mortality_cumulative FLOAT,\
    excess_mortality FLOAT,\
    excess_mortality_cumulative_per_million FLOAT\
    );"         
    add_to_db(create_table_query)

# create_covid_data_table()

def create_coviddata_table():
    create_table_query = "CREATE TABLE CovidData\
    (\
    iso_code VARCHAR(10) NOT NULL,\
    continent VARCHAR(50) NOT NULL,\
    location VARCHAR(50) NOT NULL,\
    date DATE NOT NULL,\
    total_cases VARCHAR(255) NOT NULL,\
    new_cases VARCHAR(255) NOT NULL,\
    new_cases_smoothed VARCHAR(255),\
    total_deaths VARCHAR(255),\
    new_deaths VARCHAR(255),\
    new_deaths_smoothed VARCHAR(255),\
    total_cases_per_million VARCHAR(255),\
    new_cases_per_million VARCHAR(255),\
    new_cases_smoothed_per_million VARCHAR(255),\
    total_deaths_per_million VARCHAR(255),\
    new_deaths_per_million VARCHAR(255),\
    new_deaths_smoothed_per_million VARCHAR(255),\
    reproduction_rate VARCHAR(255),\
    icu_patients VARCHAR(255),\
    icu_patients_per_million VARCHAR(255),\
    hosp_patients VARCHAR(255),\
    hosp_patients_per_million VARCHAR(255),\
    weekly_icu_admissions VARCHAR(255),\
    weekly_icu_admissions_per_million VARCHAR(255),\
    weekly_hosp_admissions VARCHAR(255),\
    weekly_hosp_admissions_per_million VARCHAR(255),\
    total_tests VARCHAR(255),new_tests VARCHAR(255),\
    total_tests_per_thousand VARCHAR(255),\
    new_tests_per_thousand VARCHAR(255),\
    new_tests_smoothed VARCHAR(255),\
    new_tests_smoothed_per_thousand VARCHAR(255),\
    positive_rate VARCHAR(255),tests_per_case VARCHAR(255),\
    tests_units VARCHAR(255),\
    total_vaccinations VARCHAR(255),\
    people_vaccinated VARCHAR(255),\
    people_fully_vaccinated VARCHAR(255),\
    total_boosters VARCHAR(255),\
    new_vaccinations VARCHAR(255),\
    new_vaccinations_smoothed VARCHAR(255),\
    total_vaccinations_per_hundred VARCHAR(255),\
    people_vaccinated_per_hundred VARCHAR(255),\
    people_fully_vaccinated_per_hundred VARCHAR(255),\
    total_boosters_per_hundred VARCHAR(255),\
    new_vaccinations_smoothed_per_million VARCHAR(255),\
    new_people_vaccinated_smoothed VARCHAR(255),\
    new_people_vaccinated_smoothed_per_hundred VARCHAR(255),\
    stringency_index VARCHAR(255),\
    population_density VARCHAR(255),\
    median_age VARCHAR(255),\
    aged_65_older VARCHAR(255),\
    aged_70_older VARCHAR(255),\
    gdp_per_capita VARCHAR(255),\
    extreme_poverty VARCHAR(255),\
    cardiovasc_death_rate VARCHAR(255),\
    diabetes_prevalence VARCHAR(255),\
    female_smokers VARCHAR(255),\
    male_smokers VARCHAR(255),\
    handwashing_facilities VARCHAR(255),\
    hospital_beds_per_thousand VARCHAR(255),\
    life_expectancy VARCHAR(255),\
    human_development_index VARCHAR(255),\
    population VARCHAR(255) NOT NULL,\
    excess_mortality_cumulative_absolute VARCHAR(255),\
    excess_mortality_cumulative VARCHAR(255),\
    excess_mortality VARCHAR(255),\
    excess_mortality_cumulative_per_million VARCHAR(255)\
    );"         
    add_to_db(create_table_query)
# create_coviddata_table()

def create_covid_deaths_data_table():
    create_table_query = "CREATE TABLE covid_deaths_data\
    (iso_code VARCHAR(10) NOT NULL,\
    continent VARCHAR(50) NOT NULL,\
    location VARCHAR(50) NOT NULL,\
    date DATE NOT NULL,\
    population VARCHAR(255) NOT NULL,\
    total_cases VARCHAR(255) NOT NULL,\
    new_cases VARCHAR(255),\
    new_cases_smoothed VARCHAR(255),\
    total_deaths VARCHAR(255), \
    ew_deaths VARCHAR(255),\
    new_deaths_smoothed VARCHAR(255),\
    total_cases_per_million VARCHAR(255),\
    new_cases_per_million VARCHAR(255),\
    new_cases_smoothed_per_million VARCHAR(255),\
    total_deaths_per_million VARCHAR(255),\
    new_deaths_per_million VARCHAR(255),\
    new_deaths_smoothed_per_million VARCHAR(255),\
    reproduction_rate VARCHAR(255),\
    icu_patients VARCHAR(255),\
    icu_patients_per_million VARCHAR(255),hosp_patients VARCHAR(255),\
    hosp_patients_per_million VARCHAR(255),\
    weekly_icu_admissions VARCHAR(255),\
    weekly_icu_admissions_per_million VARCHAR(255),\
    weekly_hosp_admissions VARCHAR(255),\
    weekly_hosp_admissions_per_million VARCHAR(255));"         
    add_to_db(create_table_query)
#create_covid_deaths_data_table()

def create_CovidVaccination_data_table():
    create_table_query= "CREATE TABLE CovidVaccination_data\
    (\
    iso_code VARCHAR(10) NOT NULL,\
    continent VARCHAR(50) NOT NULL,\
    location VARCHAR(50) NOT NULL,\
    date DATE NOT NULL,\
    total_tests VARCHAR(255),\
    new_tests VARCHAR(255),\
    total_tests_per_thousand VARCHAR(255),\
    new_tests_per_thousand VARCHAR(255),\
    new_tests_smoothed VARCHAR(255),\
    new_tests_smoothed_per_thousand VARCHAR(255),\
    positive_rate VARCHAR(255),\
    tests_per_case VARCHAR(255),\
    tests_units VARCHAR(255),\
    total_vaccinations VARCHAR(255),\
    people_vaccinated VARCHAR(255),\
    people_fully_vaccinated VARCHAR(255),\
    total_boosters VARCHAR(255),\
    new_vaccinations VARCHAR(255),\
    new_vaccinations_smoothed VARCHAR(255),\
    total_vaccinations_per_hundred VARCHAR(255),\
    people_vaccinated_per_hundred VARCHAR(255),\
    people_fully_vaccinated_per_hundred VARCHAR(255),\
    total_boosters_per_hundred VARCHAR(255),\
    new_vaccinations_smoothed_per_million VARCHAR(255),\
    new_people_vaccinated_smoothed VARCHAR(255),\
    new_people_vaccinated_smoothed_per_hundred VARCHAR(255),\
    stringency_index VARCHAR(255),\
    population_density VARCHAR(255),\
    median_age VARCHAR(255),\
    aged_65_older VARCHAR(255),\
    aged_70_older VARCHAR(255),\
    gdp_per_capita VARCHAR(255),\
    extreme_poverty VARCHAR(255),\
    cardiovasc_death_rate VARCHAR(255),\
    diabetes_prevalence VARCHAR(255),\
    female_smokers VARCHAR(255),\
    male_smokers VARCHAR(255),\
    handwashing_facilities VARCHAR(255),\
    hospital_beds_per_thousand VARCHAR(255),\
    life_expectancy VARCHAR(255),\
    human_development_index VARCHAR(255),\
    excess_mortality_cumulative_absolute VARCHAR(255),\
    excess_mortality_cumulative VARCHAR(255),\
    excess_mortality VARCHAR(255),\
    excess_mortality_cumulative_per_million VARCHAR(255));"
    add_to_db(create_table_query)
# create_CovidVaccination_data_table()
