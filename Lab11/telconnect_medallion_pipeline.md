# Lab: TelConnect Medallion Architecture & Lakeflow Job Pipeline

## Exercise 1: Set Up the TelConnect Unity Catalog Structure

In this exercise, you prepare the Unity Catalog foundation that the Lakeflow Job tasks will read from and write to. You create a catalog with three schemas following a medallion architecture pattern:

* `bronze` — raw CDR records as ingested
* `silver` — cleaned and validated CDR data
* `gold` — aggregated network usage metrics for reporting

### ✅ Provided: Accept the job parameter for the processing date

When this notebook runs as a Lakeflow Job task, the job passes the `processing_date` parameter automatically. The `dbutils.widgets` API makes that value available inside the notebook.

Run the cell below to register the widget. When running manually, it defaults to today's date.

![Register Processing Date Widget](1_8.png)

### ✅ Provided: Create the Unity Catalog structure

Run the cell below to create the `telconnect_lab` catalog and the three medallion schemas. You will use these throughout the lab.

![Create Unity Catalog Structure](2_9.png)

---

## Exercise 2: Ingest Raw CDR Data (Bronze Layer)

The Bronze layer stores raw Call Detail Records exactly as received. A sample CSV representing network events for a single processing day is provided. You load it into the workspace, register it as a Bronze Delta table, and verify the record count.

### ✅ Provided: Create the raw_uploads volume and download sample data

Run the cell below to create the Unity Catalog volume and automatically download `telecom_cdrs.csv` from GitHub into the volume.

![Create Volume and Download Sample Data](3_9.png)

### ✅ Provided: Load the CDR CSV into a DataFrame

Run the cell below to read the uploaded CSV into a Spark DataFrame. Inspect the output — note which records contain quality issues that you will clean in Exercise 3.

![Load CDR CSV into DataFrame](4_8.png)

### ✅ Provided: Write raw CDR data to a Bronze Delta table

Run the cell below to persist `raw_df` as a managed Delta table in Unity Catalog. This is the starting point for all downstream cleaning and aggregation.

![Write Raw CDR Data to Bronze Table](5_8.png)

---

## Exercise 3: Clean and Validate CDR Data (Silver Layer)

The Silver layer contains CDRs that have passed validation. In a production pipeline, only clean records flow downstream to aggregations and billing systems.

Review the raw data carefully. You will find the following quality issues that must be corrected:

| Issue | Example | Handling rule |
| :--- | :--- | :--- |
| **Negative duration** | `CDR010` | Drop records where `duration_seconds < 0` |
| **Missing callee for VOICE/DATA calls** | `CDR007` | Drop `VOICE` and `DATA` records where `callee_msisdn` is null |
| **Zero-duration completed calls** | Several | Drop `VOICE` records with `call_status = COMPLETED` and `duration_seconds = 0` |
| **Invalid call_type value** | Check data | Keep only: `VOICE`, `DATA`, `SMS` |

### ✅ Provided: Apply cleaning rules and write to Silver

Run the cell below. It applies the four data quality rules to `bronze_df` and writes the validated records to the Silver table. Review the printout to see how many records were dropped at each step.

![Apply Cleaning Rules and Write to Silver](6_8.png)

---

## Exercise 4: Aggregate Network Metrics (Gold Layer)

The Gold layer contains aggregated metrics consumed by dashboards and operations teams. TelConnect's network operations centre needs a daily summary per region and network type showing:

* Total calls handled
* Total successful calls (`COMPLETED` status)
* Call drop rate (percentage of calls that were `DROPPED`)
* Average call duration (for `VOICE` calls only)
* Total data transferred in MB (for `DATA` calls only)

![Read from Silver Table](7_8.png)

### ✅ Provided: Build the regional network summary

Run the cell below. It aggregates the Silver CDR data by region and network type, computing the five operational metrics, and writes the result to the Gold table.

![Build Regional Network Summary](8_7.png)

---

## Exercise 5: Make the Notebook Job-Ready

When this notebook runs as a Lakeflow Job task, it should communicate outcomes back to the job. Databricks provides two mechanisms:

* `dbutils.notebook.exit()` — exits the notebook and passes a string value to any task that calls this notebook via the *Run Notebook* task type.
* `dbutils.jobs.taskValues.set()` — stores a named value that downstream tasks in the same job run can retrieve.

You use both here to make the notebook observable and chainable.

### ✅ Provided: Count Gold records and share the result with the job

![Count Gold Records and Share Task Values](9_5.png)