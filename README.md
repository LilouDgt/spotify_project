# 🎵 Bad Bunny Spotify Analytics

[![Python](https://img.shields.io/badge/python-3.9-blue?logo=python)](https://www.python.org/) 
[![BigQuery](https://img.shields.io/badge/BigQuery-Project-red?logo=googlecloud)](https://cloud.google.com/bigquery) 
[![dbt](https://img.shields.io/badge/dbt-Models-orange?logo=dbt)](https://www.getdbt.com/)

A project showcasing **data extraction, transformation, and modeling** of Spotify data for the artist **Bad Bunny** using Python, BigQuery, and dbt.  

---

## 🚀 Project Overview

- Extract track, album, and popularity data from **Spotify API**.  
- Load cleaned data into **Google BigQuery**.  
- Build dbt models: **sources → staging → dimension tables** for analytics.  
- Demonstrates end-to-end **data engineering workflow** from API to analytics-ready tables.

---

## 📂 Project Structure

```
bad-bunny-spotify-project/
├── data/ # Optional local datasets
├── dbt/
│ ├── models/
│ │ ├── staging/ # Staging models
│ │ ├── marts/
│ │ │ └── dim/ # Dimension tables
│ │ └── sources.yml # Source definitions
│ └── dbt_project.yml # DBT project config
├── notebooks/ # Exploration notebooks
├── scripts/ # Python API extraction & BigQuery load
├── .gitignore
└── README.md
```


---

## 🛠 Technologies Used

- **Python** – API requests & data cleaning  
- **Spotify API** – Music data extraction  
- **Google BigQuery** – Data storage & querying  
- **dbt (Data Build Tool)** – Data modeling with SQL & YAML  
- **Git/GitHub** – Version control  

---


