# Project 1 - Stock Master : Know stocks in your local currency    

## Project Context and Goals

## Project Goal

The aim of this project was to establish a comprehensive pipeline for extracting data from a live dataset that is constantly updating, and then loading it into a relational database to simulate a real-life data warehouse. The pipeline would encompass data transformations before loading (ETL) and after loading (ELT) to replicate potential data manipulation requirements for real-time applications.

The data must be formatted in a user-friendly manner to facilitate prompt engagement by data analysts (DA) and data scientists (DS) without necessitating substantial time allocation to data manipulations. This entails loading the data into distinct tables that can be seamlessly merged, featuring clear and intuitive column names and appropriate data types for each individual record.


## Business Objective

The primary objective of our project is to curate analytical datasets by leveraging the Market Stack API and Fixer API. These datasets will encompass a comprehensive range of stock data, including stock opening, high, low, closing, and volume metrics. Furthermore, we intend to amalgamate exchange data from the API to furnish investors with stock prices denominated in their respective local currencies.


## Consumers

Our data caters to the needs of local small investors and analysts from Australia, China, and India, enabling them to easily access stock prices in their local currency. It offers real-time information on stocks across global stock exchanges. Users can seamlessly retrieve data through SQL queries and custom SQL views from the database, and leverage it to create visually engaging dashboards featuring maps and key performance metrics.

## Questions

What questions are you trying to answer with your data? How will data support our local small traders?

Example:

> - What is the price of a stock today or historically?
> - What is the price of stock in Austrialian dollar, Chinese Yuan or Indian rupees?
> - What is the average closing price of the stock in USD, Euro, Yuan, austrailain dollar or indian rupees?


## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?

- **Market Stack API**: MarketStack is a powerful API that provides real-time, intraday, and historical market data for global stock markets. It's widely used for accessing up-to-date stock price information in various formats.

- **Fixer API**: Fixer API is a popular service that provides real-time and historical exchange rate data for a wide range of currencies. It’s widely used for currency conversion applications.

- **Postgress**: Progress is used for storing store data from API, transformation of data and loading the results for traders to acess using SQL. Analyst can use the SQL to build a report/dashboard for analysis. 

**Table of contents for source data**

| Source name | Source type | Refresh Cadence | Link |
| - | - | - |- |
| Trader database | PostgreSQL database | Daily | - |
| Market Stack  | REST API | Daily | https://marketstack.com/ |
| Fixer API  | REST API | Hourly | https://fixer.io/ |

## Solution architecture

Here is a dummy  solution architecture diagram (Ari will push the final arhcitecture)

![images/system_design_dummy](images/system_design_dummy.png)

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram.
