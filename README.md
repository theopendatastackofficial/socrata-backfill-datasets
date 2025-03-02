# What is this Repo?

This repository serves as a local data platform for ingesting very large datasets from the socrata API:

# Project Setup Guide

This project assumes you are using a code IDE, either locally such as with [VSCode](https://code.visualstudio.com/docs/setup/setup-overview) or with [Github Codespaces](
https://docs.github.com/en/codespaces/getting-started/quickstart). Codespaces can be run by first making a free Github account, clicking the green **Code** button at the top of this repo, and then selecting **Codespaces**.

## 1. Install `uv`

Before proceeding, you will need to install `uv`. You can install it via pip:

```bash
pip install uv
```

Alternatively, follow the instructions here: [Install UV](https://docs.astral.sh/uv/getting-started/installation/#installation-methods).

Once `uv` is installed, proceed to clone the repository.

## 2. Clone the Repository

To clone the repository, run the following command:

```bash
git clone https://github.com/ChristianCasazza/mtadata 
```

You can also make the repo have a custom name by adding it at the end:

```bash
git clone https://github.com/ChristianCasazza/mtadata custom_name
```

Then, navigate into the repository directory:

```bash
cd custom_name
```

## 3. Setup the Project

This repository includes two setup scripts:
- **`setup.sh`**: For Linux/macOS
- **`setup.bat`**: For Windows

These scripts automate the following tasks:
1. Create and activate a virtual environment using `uv`.
2. Install project dependencies.
3. Ask for your Socrata App Token (`SOCRATA_API_TOKEN`). If no key is provided, the script will use the community key: `uHoP8dT0q1BTcacXLCcxrDp8z`.
   - **Important**: The community key is shared and rate-limited. Please use your own key if possible. You can obtain one in two minutes by signing up [here](https://evergreen.data.socrata.com/signup) and following [these instructions](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys).
4. Copy `.env.example` to `.env` and append `SOCRATA_API_TOKEN` to the file.
5. Dynamically generate the `LAKE_PATH` variable for your system and append it to `.env`.
6. Start the Dagster development server.

### Run the Setup Script

#### On Linux/macOS:
```bash
./setup.sh
```

If you encounter a `Permission denied` error, ensure the script is executable by running:
```bash
chmod +x setup.sh
```

#### On Windows:
```cmd
setup.bat
```

If PowerShell does not recognize the script, ensure you're in the correct directory and use `.\` before the script name:
```powershell
.\setup.bat
```

The script will guide you through the setup interactively. Once complete, your `.env` file will be configured, and the Dagster server will be running.

## 4. Access Dagster

After the setup script finishes, you can access the Dagster web UI. The script will display a URL in the terminal. Click on the URL or paste it into your browser to access the Dagster interface.

## 5. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.


This will begin downloading the datasets in 500k batches, usually taking a few hours.

# Querying the data for Ad-hoc analysis

## Working in a notebook

### Overview


The `DuckDBWrapper` class provides a simple interface to interact with DuckDB, allowing you to register data files (Parquet, CSV, JSON), execute queries, and export results in multiple formats.

---

### Installation and Initialization

In the top right corner of your notebook, select your .venv in python enviornments. If using VScode, it may suggest to install Jupyter and python extensions.

Then, in the notebook, you just need to run the first two cells. The first cell will load the DuckDBWrapper Class. Then, you can initialize a `DuckDBWrapper` instance in the second cell with:

#### Initialize an in-memory DuckDB instance

```bash
con = DuckDBWrapper()
```
#### Initialize a persistent DuckDB database

```

```bash
con = DuckDBWrapper("my_database.duckdb")
```

You can run the rest of the cells to learn how to utilize the class.
