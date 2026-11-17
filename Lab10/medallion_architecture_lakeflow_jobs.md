# Lab: Medallion Architecture & Lakeflow Job Patterns (GlobStay)

## Exercise 1: Ingest Raw Booking Data (Bronze Layer)

The Bronze layer stores raw, unmodified data exactly as it arrives from source systems. In a production pipeline, data would arrive from a Property Management System (PMS). For this lab, a sample dataset representing bookings across five GlobStay properties is provided for you.

Data issues are intentional — they reflect real-world conditions you will address in the Silver layer.

### ✅ Provided: Create the Unity Catalog structure and sample data

Run the two cells below to:

* Create a catalog `hospitality_lab` with `bronze`, `silver`, and `gold` schemas.
* Create a sample raw bookings DataFrame (`raw_df`) containing deliberate quality issues.

Inspect the output carefully — you will need to understand the data issues before tackling Exercise 2.

### 🎯 Challenge: Write raw data to a Bronze table

Write `raw_df` to a Delta table named `bookings_bronze` in the `hospitality_lab.bronze` schema.

**Requirements:**

* Use `overwrite` mode so the cell is re-runnable.
* After writing, verify the record count matches the raw DataFrame.

> 🤖 **Genie Code tip:** Ask: *"How do I write a PySpark DataFrame to a Unity Catalog table using saveAsTable with overwrite mode?"*

*(Nota: Si se ejecuta la celda sin haber corrido previamente las celdas de configuración que crean `raw_df`, se producirá un error `NameError: name 'raw_df' is not defined`).*

![Write raw data to a Bronze table](3_8.png)

---

## Exercise 2: Clean and Validate Booking Data (Silver Layer)

The Silver layer contains cleaned, validated data that downstream processes can rely on. Each quality issue present in the bronze layer must be addressed:

| Issue | Found in | Rule |
| :--- | :--- | :--- |
| **Duplicate bookings** | `B001` appears twice | Keep only the first occurrence per `booking_id` |
| **Null room_type** | `B005` | Exclude records with no room type |
| **Invalid date order** | `B006` (`check_out` before `check_in`) | Exclude records where `check_out_date <= check_in_date` |
| **Non-positive rate** | `B007` (`-50`), `B012` (`0`) | Exclude records where `rate_per_night <= 0` |

> **Note:** Null `guest_email` values (`B004`, `B015`) are acceptable — not all bookings require an email address.

### 🎯 Challenge: Create the Silver table

Read from `hospitality_lab.bronze.bookings_bronze`, apply all four cleaning rules, and write the result to `hospitality_lab.silver.bookings_silver` (`overwrite` mode).

**Hints:**

* Use `.dropDuplicates(["booking_id"])` to deduplicate.
* Use `.filter(col("room_type").isNotNull())` to remove nulls.
* Import `to_date` and `datediff` from `pyspark.sql.functions` to compare dates.
* Chain `.filter()` calls for each rule.

After writing, print both the Bronze and Silver record counts to confirm records were removed.

> 🤖 **Genie Code tip:** Ask: *"How do I filter PySpark rows where check_out_date is not after check_in_date using date comparison?"*

![Create the Silver table](4_7.png)

---

## Exercise 3: Build Analytics-Ready Data (Gold Layer)

The Gold layer contains highly curated, business-facing datasets. GlobStay leadership wants two analytics tables:

1. `property_performance` — total confirmed bookings, nights sold, and revenue per hotel property.
2. `channel_performance` — booking volume and revenue broken down by booking channel (direct, OTA, corporate).

### 🎯 Challenge 3a: Property performance table

Create a Gold table `hospitality_lab.gold.property_performance` using Spark SQL.

**Requirements:**

* **Source:** `hospitality_lab.silver.bookings_silver`
* Filter to `booking_status = 'confirmed'` only.
* **Columns:** `property_id`, `property_name`, `confirmed_bookings`, `total_nights`, `total_revenue`
* Calculate `total_nights` using `DATEDIFF(check_out_date, check_in_date)`.
* Calculate `total_revenue` as `total_nights * rate_per_night`.
* Order by `total_revenue DESC`.

> 🤖 **Genie Code tip:** Ask: *"Write a Spark SQL CREATE OR REPLACE TABLE statement that aggregates booking revenue per property using DATEDIFF and SUM."*

### 🎯 Challenge 3b: Booking channel performance table

Create a Gold table `hospitality_lab.gold.channel_performance` showing how each booking channel contributes to GlobStay revenue.

**Requirements:**

* **Source:** `hospitality_lab.silver.bookings_silver`
* Filter to `booking_status = 'confirmed'` only.
* **Columns:** `channel`, `booking_count`, `total_revenue`, `avg_revenue_per_booking` (rounded to 2 decimal places).
* Order by `total_revenue DESC`.

> 🤖 **Genie Code tip:** Ask: *"How do I calculate average revenue per booking in Spark SQL using ROUND and AVG?"*

![Booking channel performance table](5_7.png)

---

## Exercise 4: Error Handling in Pipeline Notebooks

When a notebook runs as a Lakeflow Job task, it must handle errors gracefully and communicate its outcome to downstream tasks. Two key patterns are:

1. `try/except` with `PySparkException` — catch specific Spark errors and handle them without crashing the notebook.
2. `dbutils.notebook.exit()` — signal success or failure with a structured message that the job can inspect.

### 🎯 Challenge 4a: Implement safe table reading with try/except

Write a function `safe_read_table(table_name)` that:

1. Tries to read the table using `spark.table(table_name)`.
2. Catches `PySparkException` from `pyspark.errors`.
3. Prints a descriptive message with the error class if the table is not found.
4. Returns `None` on error, or the DataFrame on success.

Test it with a valid table and a non-existent table to confirm both paths work.

**Hints:**

* Import: `from pyspark.errors import PySparkException`
* Use `e.getErrorClass()` to get a structured error code.
* Re-raise unexpected errors that you haven't handled.

> 🤖 **Genie Code tip:** Ask: *"Show me how to catch a PySparkException in Python and print its error class."*

![Implement safe table reading with try/except](6_7.png)

### 🎯 Challenge 4b: Signal task outcome with dbutils.notebook.exit()

When a notebook runs as a Lakeflow Job task, calling `dbutils.notebook.exit()` lets downstream tasks read the outcome using `{{tasks.<task_name>.values.<key>}}`.

Write a function `report_pipeline_outcome(table_name)` that:

1. Reads the specified table and counts its records.
2. On success, calls `dbutils.notebook.exit()` with the message `"SUCCESS: Processed <n> records from <table_name>"`.
3. On any exception, calls `dbutils.notebook.exit()` with the message `"FAILED: <error message>"`.

> ⚠️ **Warning:** `dbutils.notebook.exit()` halts notebook execution immediately. Define the function but do not call it — instead, leave the call commented out. In the job, calling it is the correct behaviour.

> 🤖 **Genie Code tip:** Ask: *"How does dbutils.notebook.exit work in Azure Databricks when a notebook runs as a job task?"*

![Signal task outcome with dbutils.notebook.exit()](7_7.png)

---

## Exercise 5: Parameterized Notebook Tasks

Reusable pipeline notebooks accept parameters at runtime — making the same notebook work for different scenarios (e.g. different dates, environments, or filter values) without code changes.

Two mechanisms are used in Lakeflow Jobs:

* `dbutils.widgets` — pass parameters into a notebook from a job task configuration.
* `dbutils.jobs.taskValues` — pass values between notebook tasks in the same job run.

### 🎯 Challenge 5a: Add a runtime widget parameter

Add a text widget named `booking_status` with default value `confirmed` and label `Booking Status Filter`.

Then retrieve the widget value and use it to filter `hospitality_lab.silver.bookings_silver`. Display the filtered result and print the record count.

**Hints:**

* `dbutils.widgets.text(name, default, label)`
* `dbutils.widgets.get(name)` returns the current value.
* In a Lakeflow Job, the task Parameters configuration overrides the default value at runtime.

> 🤖 **Genie Code tip:** Ask: *"How do I create a text widget in Databricks and retrieve its value to filter a DataFrame?"*

![Add a runtime widget parameter](8_6.png)

### 🎯 Challenge 5b: Pass values to downstream tasks

After processing, a notebook task can share computed values with downstream tasks using `dbutils.jobs.taskValues.set()`.

Set two task values from the Silver table:

1. Key `silver_record_count` — the number of records in `bookings_silver`.
2. Key `properties_processed` — a Python list of distinct `property_id` values.

Then print what was set so you can verify it.

> **Note:** `dbutils.jobs.taskValues` is only available inside a Lakeflow Job run. In an interactive notebook session it prints a warning and is a no-op — this is expected.

> 🤖 **Genie Code tip:** Ask: *"How do I use dbutils.jobs.taskValues.set to pass a list value from one Databricks notebook task to another?"*

![Pass values to downstream tasks](9_4.png)