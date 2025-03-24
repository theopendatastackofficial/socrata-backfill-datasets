# What is this Repo?

This repository serves as a local data platform for ingesting very large datasets from the Socrata API:

# Project Setup Guide

This project assumes you are using a code IDE, either locally such as with [VSCode](https://code.visualstudio.com/docs/setup/setup-overview) or with [Github Codespaces](
https://docs.github.com/en/codespaces/getting-started/quickstart). Codespaces can be run by first making a free Github account, clicking the green **Code** button at the top of this repo, and then selecting **Codespaces**.

## 1. Install `uv`

Before proceeding, you will need to install `uv`. You can install it using one of the following methods:

### **Installation Methods**

Install `uv` with our standalone installers or your package manager of choice.

#### **Standalone Installer**

`uv` provides a standalone installer to download and install `uv`:

##### **macOS and Linux**
```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```
If your system doesn't have `curl`, you can use `wget`:
```sh
wget -qO- https://astral.sh/uv/install.sh | sh
```
Request a specific version by including it in the URL:
```sh
curl -LsSf https://astral.sh/uv/0.6.3/install.sh | sh
```

##### **Tip**

The installation script may be inspected before use:

```sh
curl -LsSf https://astral.sh/uv/install.sh | less
```

Alternatively, the installer or binaries can be downloaded directly from GitHub.

See the documentation on [installer configuration](https://docs.astral.sh/uv/getting-started/installation/) for details on customizing your `uv` installation.

### **What This Command Does**
- `curl -LsSf`: Downloads the install script quietly and safely.
- `| sh`: Runs the script in your shell, installing `uv` automatically.

### **If You Encounter Issues**
- If you run the command and get an error, copy and paste your terminal output into ChatGPT for troubleshooting.

---

## 2. Test Your `uv` Installation

### **Open the Terminal in VSCode**
- In VSCode, look at the top menu and select **Terminal > New Terminal**, or press:
  - **Windows/Linux:** `Ctrl` + `` ` `` (Control + backtick)
  - **macOS:** `Command` + `` ` ``

### **Run the command:**
```sh
uv
```

#### **Expected output:**
If `uv` was installed correctly, you should see a help message that starts with:

```sh
An extremely fast Python package manager.

Usage: uv [OPTIONS] <COMMAND>

Commands:
  run      Run a command or script
  init     Create a new project
  ...
  help     Display documentation for a command
```

#### **If you get an error:**
- Copy and paste the exact error message into ChatGPT and explain you’re having problems using `uv`.
- ChatGPT can help troubleshoot your specific error message.

If `uv` displays its usage information without an error, congratulations! You’re all set to work with Python in your local environment.

---

## 3. Clone the Repository

To clone the repository, run the following command:

```bash
git clone https://github.com/theopendatastackofficial/socrata-backfill-datasets
```

You can also make the repo have a custom name by adding it at the end:

Then, navigate into the repository directory. It is best to do this by using the VSCode explorer on the left side to open the file system. 

```bash
cd socrata-backfill-assets
```

Then, navigate into the repository directory:


## 4. Setup the Project

This repository includes two setup scripts:
- **`setup.sh`**: For Linux/macOS
- **`setup.bat`**: For Windows

These scripts automate the following tasks:
1. Create and activate a virtual environment using `uv`.
2. Install project dependencies.
3. Ask for your Socrata App Token (`SOCRATA_API_TOKEN`). If no key is provided, the script will use the community key. 
   - **Important**: The community key is shared and rate-limited. Please use your own key if possible. You can obtain one in two minutes by signing up [here](https://evergreen.data.socrata.com/signup) and following [these instructions](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys).
4. Copy `.env.example` to `.env` and append `SOCRATA_API_TOKEN` to the file.
5. Dynamically generate the `WAREHOUSE_PATH` variable for your system and append it to `.env`.
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

## 5. Access Dagster

After the setup script finishes, you can access the Dagster web UI. The script will display a URL in the terminal. Click on the URL or paste it into your browser to access the Dagster interface.

## 6. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.

This will begin downloading the datasets in 500k batches, usually taking a few hours.

# Querying the data for Ad-hoc analysis

## Working in a notebook

### Overview

The `DuckDBWrapper` class provides a simple interface to interact with DuckDB, allowing you to register data files (Parquet, CSV, JSON), execute queries, and export results in multiple formats. It has a specific function meant for creating views when datastes are saved with the format year=/month=/*.parquet.

---

### Installation and Initialization

In the top right corner of your notebook, select your .venv in Python environments. If using VSCode, it may suggest installing Jupyter and Python extensions.

Then, in the notebook, you just need to run the first two cells. The first cell will load the `DuckDBWrapper` class. Then, you can initialize a `DuckDBWrapper` instance in the second cell with:

#### Initialize an in-memory DuckDB instance

```python
con = DuckDBWrapper()
```

#### Initialize a persistent DuckDB database

```python
con = DuckDBWrapper("data.duckdb")
```

You can run the rest of the cells to learn how to utilize the class.
