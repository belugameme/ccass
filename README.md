# hkex ccass scraper

## Overview

This project has 2 parts. 

The data scrapping pipeline uses Kedro framework, export the data tables to hosted DB via ODBC / Spark. For Kedro, refer to the [Kedro documentation](https://kedro.readthedocs.io).

Below datasets / tables are produced:
* stock_list: list of stock_code with stock_name
* stock_participants: history of participants share and share% per business_date per stock_code
* stock_top_10_participants: precalculated top 10 participants per asofdate per stock_code
* stock_participants_diff: precalculated participants' share changes per asofdate per stock_code

The visualization uses plotly, hosted at: https://ccass-plotter.herokuapp.com 