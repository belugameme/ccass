# hkex ccass scraper

## Overview

This project has 2 parts. 

### Back end

The data scrapping pipeline uses Kedro framework, export the data tables to hosted DB via ODBC / Spark. For Kedro, refer to the [Kedro documentation](https://kedro.readthedocs.io).

Below datasets / tables are produced:
* stock_list: list of stock_code with stock_name
* stock_participants: history of participants share and share% per business_date per stock_code
* stock_top_10_participants: precalculated top 10 participants per asofdate per stock_code
* stock_participants_diff: precalculated participants' share changes per asofdate per 

The init / incremental load params can be configured at: hkex-ccass-scraper/conf/base/parameters.yml

The node logics and pipeline instances at: hkex-ccass-scraper/src/hkex_ccass_scraper/pipelines/data_engineering/

### Front end

The visualization uses plotly, hosted at: https://ccass-plotter.herokuapp.com 

The visualization code at app.py