# Financial Market ETL Pipeline

A professional end-to-end ETL pipeline for extracting, transforming, and loading historical stock market data using Python.

This project is designed as a portfolio-ready data engineering and financial analytics project, with a modular architecture, configurable parameters, logging, and quantitative indicators commonly used in market analysis.

---

## Project Overview

The goal of this project is to build a structured ETL pipeline that:

- Extracts historical stock market data from Yahoo Finance
- Saves raw market data snapshots
- Cleans and transforms the data
- Computes key quantitative financial indicators
- Saves processed data for downstream analysis, dashboards, or machine learning models

This project demonstrates good software engineering and data engineering practices for financial data workflows.

---

## Features

- Modular ETL architecture (`extract`, `transform`, `load`)
- Centralized configuration via `config.py`
- Logging to both console and file
- Raw and processed data separation
- Quantitative financial indicators:
  - Daily return
  - Log return
  - Simple Moving Averages (SMA 20, 50, 200)
  - Rolling volatility
  - Annualized volatility
  - Cumulative return
  - Drawdown
  - Maximum drawdown
- GitHub-ready project structure

---

## Project Structure

```bash
financial-market-etl/
│
├── main.py
├── requirements.txt
├── README.md
├── .gitignore
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── logger.py
│
├── data/
│   ├── raw/
│   └── processed/
│
└── logs/