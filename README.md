# hkex ccass scraper

## Overview

This project has 2 parts. 

The data scrapping pipeline uses Kedro framework, export the data tables to hosted DB via ODBC / Spark. For Kedro, refer to the [Kedro documentation](https://kedro.readthedocs.io).

The visualization uses plotly, hosted at: https://ccass-plotter.herokuapp.com 