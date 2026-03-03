# EIA Data Tool

A small Flask app for exploring U.S. state-level electricity generation and trade data sourced 
from the [EIA Open Data API](https://www.eia.gov/opendata/).

This project is independent and not affiliated with the EIA.

## Features

- Annual electricity data for all 50 states + DC from 1990–2024
- Tracks net generation, interstate trade, and international imports/exports
- Charts for generation and trade trends over time
- Data cached locally in SQLite — only re-fetches from the API every 30 days

## Requirements

- Python 3.11+
- An EIA API key (free, see below)

## Getting an API key

Register at [https://www.eia.gov/opendata/register.php](https://www.eia.gov/opendata/register.php). 
Your key will be emailed to you.

## Setup

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

Create a .env file at the project root and add your key:
```
EIA_API_KEY="your_key_here"
```

Fetch data and populate the local database:

```bash
python -m utils.fetch_yearly_source_disposition_data
````

Start the app:
```bash
python app.py
```
Then open http://127.0.0.1:5000 in your browser.

## Project Structure
```
EIA-Data-Tool/
├── .env
├── requirements.txt
├── app.py
├── utils/
│   ├── __init__.py
│   ├── chart_data_formatters.py
│   └── fetch_yearly_source_disposition_data.py
├── db/
│   ├── __init__.py
│   ├── eia.db
│   └── db.py
├── static/
│   ├── css/
│   └── js/
├── templates/
│   ├── base.html
│   └── index.html
└── data/
```