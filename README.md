🚲 Citi-Vault

Modern Data Stack project using CitiBike data, GBFS feeds, Data Vault 2.0 and dbt

Citi-Vault is a full end-to-end data engineering project designed to explore a modern analytics stack including:
	•	Data ingestion (Citibike CSVs, GBFS API snapshots, Weather API)
	•	Orchestration with Apache Airflow
	•	Storage in PostgreSQL (Dockerized)
	•	Data modeling using Data Vault 2.0 (Hubs, Links, Satellites)
	•	Analytics layer using dbt (Staging, Marts, Facts)
	•	Self-service ready with clean dimensions & fact tables

The goal is to reproduce the architecture & best practices of a real analytics team, while experimenting with a scalable modeling framework (Data Vault).


🚀 Features

✔ 1. Automated ingestion
	•	Citibike historical trips from CSV
	•	GBFS live snapshots (station status, station information, regions)
	•	Weather API (Meteostat)

✔ 2. PostgreSQL warehouse (Docker)
	•	Dedicated raw, staging, vault, marts schemas
	•	Safe & idempotent loaders (upsert for trips)

✔ 3. Data Vault 2.0 modeling
	•	Hubs
	•	hub_station
	•	hub_region
	•	hub_trip
	•	Links
	•	link_trip_start_station
	•	link_trip_end_station
	•	link_station_region
	•	Satellites
	•	sat_station_information
	•	sat_station_status
	•	sat_trip_informations
	•	sat_region_information

✔ 4. Analytics Layer
	•	Dimensional models:
	•	dim_station
	•	dim_region
	•	Fact table:
	•	fct_trips (with station joins, durations, member/casual flags…)

✔ 5. Testing & Data Quality (dbt tests)
	•	unique, not null, relationships, freshness checks…
