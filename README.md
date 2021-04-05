# fantasy_premier_league

Repository for Fantasy Premier League data ETL code. Very much a work in progress I infrequently come back to, so a lot of the coding standards are well below what I would aim for professionally! ETL is/was in the process of being rewritten (hence duplicate code currently) in order to clean it up. Therefore ETL folder (and code) is very messy.

End goals:
* ETL of data available from official FPL API (endpoints beginning with: https://fantasy.premierleague.com/api/). Includes loading from API, transforms of data, and loading to postgresql database.
* Predicting players likely to score highly based on historical data using simple machine learning methods
* Creation of a dashboard/web interface to show player data including results of ML models - **future**

Currently developing everything locally but looking to move to (cheap) cloud-based infrastructure to host database and dashboard/web-app.

This is a personal project so I don't claim everything to be correct or fully robust, and any progress depends on whether I fancy doing more coding in my spare time!

[![Build Status](https://travis-ci.org/Hazzais/fantasy_premier_league.svg?branch=develop)](https://travis-ci.org/Hazzais/fantasy_premier_league)
