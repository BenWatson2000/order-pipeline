# Claude prompts

A record of the prompts I gave Claude (via Claude Code) while building this,
kept alongside the code so it's clear where AI was used. The README's
"Use of AI" section summarises the same thing.

---

## Commits 1–2: Understanding the requirements

> Read the case study brief and the raw order event files. Summarise what's
> being asked, profile the data and list the data quality issues you find,
> then suggest a simple stack, project layout and build order for a small,
> reproducible ETL pipeline.

The plan that comes out of this is to build bottom-up: start with the
transform layer, where the cleaning and modelling rules live and can be unit
tested on their own, then add the pipeline, CLI and queries on top. Setting
up the project and adding the raw data are routine and don't need their own
prompts.

## Commit 3: Cleaning rules and order model

> Write the transform layer as pure functions: clean the raw events
> (reject only rows that can't be used, and fix and flag the rest), build
> one row per order line item, and build one row per order with its
> lifecycle stages, current status and whether it's active. Add unit tests
> for each rule.

## Commit 4: Extract and load

> Write the pipeline that reads the raw files, runs the transforms, and
> writes the tables to Parquet and a DuckDB database, plus a data quality
> report with counts for each cleaning rule.

## Commit 5: CLI

> Add a command-line interface with a command to run the pipeline and one
> to run SQL against the output database.

## Commit 6: Analyst queries

> Write SQL to answer the analyst questions in the brief: customers with an
> active order, and orders shipped in 2024 by product category.

