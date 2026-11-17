# Lab 09: Implement and Manage Data Quality Constraints in Unity Catalog

## Introduction

You are a data engineer at **ClearCover Insurance**, a fictional insurance provider. Each day, raw claims data arrives from regional offices and partner brokers. Unfortunately, the data is inconsistent: some records are missing required identifiers, claim amounts are formatted as strings or carry negative values, dates are occasionally malformed, and the source schema may silently gain new columns over time.

Your mission is to build a **Lakeflow Spark Declarative Pipeline** that enforces data quality constraints at every layer of the pipeline — catching bad records before they reach actuarial models and reporting dashboards.

You work through the following exercises:

| Exercise | Topic |
| :--- | :--- |
| **Exercise 1** | Set up the ClearCover Insurance Data Platform (notebook) |
| **Exercise 2** | Explore data quality issues in Catalog Explorer |
| **Exercise 3** | Implement nullability and status validation |
| **Exercise 4** | Add data type checks using `col().cast()` |
| **Exercise 5** | Handle schema drift with rescued data |
| **Exercise 6** | Run and monitor the pipeline |

> 🤖 **Genie Code — Use it always**
> Throughout every exercise in this lab, you are expected and encouraged to use Genie Code. Every exercise includes a suggested prompt to get you started. Genie Code is your pair programmer — use it to generate code, understand errors, and explore alternatives.
> To open Genie Code, select the Genie Code icon on the right side of any notebook cell, or use the keyboard shortcut.

## Prerequisites

* An Azure Databricks Premium workspace provisioned using *Lab 00: Set up your Azure Databricks environment*.
* You are familiar with basic Python and PySpark concepts.
* You have completed the earlier labs in this learning path (or are comfortable with Unity Catalog basics).

## Importing the Setup Notebook

1. In the Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store the lab.
3. Click the **⋮ (kebab)** menu or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/09-implement-manage-data-quality-constraints-unity-catalog.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless compute**.

---

## Exercise 1: Set up the ClearCover Insurance Data Platform

Run all cells in the setup notebook `09-implement-manage-data-quality-constraints-unity-catalog` from top to bottom.

The notebook creates the following objects:

| Object | Description |
| :--- | :--- |
| `insurance_lab` catalog | Top-level namespace for the ClearCover Insurance platform |
| `insurance_lab.bronze` schema | Raw, unprocessed claims data as received from source systems |
| `insurance_lab.silver` schema | Validated and type-safe records |
| `insurance_lab.gold` schema | Aggregated reporting data |
| `insurance_lab.bronze.raw_files` volume | Landing zone for raw CSV claim files |
| `insurance_lab.bronze.claims_raw` table | Delta table with 20 raw claims records |

After the notebook completes, verify the objects in **Catalog Explorer** before continuing.

---

## Exercise 2: Explore Data Quality Issues

Before writing any pipeline code, explore the raw data to understand the quality problems you need to fix.

### Task 2.1: Query the raw claims table

Open a new SQL query editor (or a notebook cell) and run:

```sql
SELECT *
FROM insurance_lab.bronze.claims_raw
ORDER BY claim_id NULLS LAST;
```

![Query Raw Claims Table](1_7.png)

*(Nota: En la captura se observa un error `[INSUFFICIENT_PERMISSIONS]` indicando que el usuario no tiene privilegios `USE CATALOG` sobre `insurance_lab` en ese entorno).*

Review the results and find at least one row for each of the following issues:

| Issue | Column(s) to check |
| :--- | :--- |
| **Missing primary identifier** | `claim_id` or `customer_id` is `NULL` |
| **Unparseable date** | `claim_date` contains a non-date string |
| **Unparseable amount** | `claim_amount` contains `N/A` or is empty |
| **Negative amount** | `claim_amount` is a negative number |
| **Invalid status** | `status` is not `OPEN`, `PENDING`, or `CLOSED` |

### Task 2.2: Inspect the schema

Run the following to confirm that `claim_date` and `claim_amount` are stored as `STRING`:

```sql
DESCRIBE TABLE insurance_lab.bronze.claims_raw;
```

![Inspect Schema](2_8.png)

*(Nota: Al igual que en la consulta anterior, la ejecución muestra `[UNAUTHORIZED_ACCESS] / PERMISSION_DENIED` por falta de permisos `USE CATALOG` sobre `insurance_lab`).*

These columns are intentionally strings in the bronze layer. The pipeline exercises will enforce the correct types during ingestion into silver.

---

## Exercise 3: Nullability and Status Validation

### Task 3.0: Create the ETL pipeline and import the pipeline file

Before writing any pipeline code, import the starter pipeline file and create the Lakeflow Spark Declarative Pipeline in Databricks.

**Import the pipeline file:**

1. In the Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to the folder where you stored the lab notebook.
3. Click the **⋮ (kebab)** menu or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/09-implement-manage-data-quality-constraints.py`
5. The file appears in your workspace as a Python source file — note its path for the next step.

**Create the pipeline:**

1. In the Databricks workspace left sidebar, click **Jobs & Pipelines**.
2. Click **Create ETL pipeline (Python)**.
3. Click the gear (**⚙️**) icon to open the pipeline settings dialog, then configure the pipeline with the following settings:

| Setting | Value |
| :--- | :--- |
| **Pipeline name** | ClearCover Claims Quality Pipeline |
| **Pipeline mode** | Triggered |
| **Target** | catalog `insurance_lab`, schema `silver` |
| **Compute** | Serverless |

4. In the pipeline editor, locate the left pane (asset browser). Open its menu and select **Import**, then browse to your imported `09-implement-manage-data-quality-constraints.py` file to add it as the pipeline source code.

Open the imported pipeline file and keep it open throughout exercises 3–5. You will now edit it to add data quality constraints.