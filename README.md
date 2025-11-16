# NoSQL Database Size and Sharding Calculator

## Description

This Python package provides a `NoSQLDatabaseCalculator` class to automate database size and sharding strategy calculations. It is designed to solve the problems outlined in the "Big Data Structure" homework (TD 2.7).

The calculator takes a set of statistics (e.g., number of clients, products) and a collection's JSON schema as input. It then generates a calculator object that can be used to analyze and compare different database denormalization strategies.

## 🚀 Features

* **Document Size Calculation:** Computes the size of a single document in bytes, accounting for:
    * Scalar types (Integer, String, Date, LongString)
    * Nested objects and arrays
    * Average array lengths (from statistics)
    * Key/value pair overhead
* **Collection & Database Size:** Aggregates document sizes to calculate the total size of a collection and a full database (e.g., DB1-DB5) in Gigabytes.
* **Sharding Strategy Analysis:** Computes distribution statistics for different sharding keys, showing:
    * Average documents per server
    * Average distinct key values per server
* **Automatic Type Detection:** Guesses the collection type (e.g., Product, Client, OrderLine) based on the fields present in its JSON schema.


## Directory layout

```
BigDataStructure-TD1/
├─ nosqlcalc/                      # Installable package
│  ├─ __init__.py                  # Exports NoSQLDatabaseCalculator
│  └─ Database_calculator.py       # Compatibility shim for legacy imports (optional)
│
│
├─ tests.py                        # Lightweight sanity tests (optional; run with pytest)
│
├─ notebooks/                      # Jupyter notebooks (just we started here, a quick tests)
│  └─ (your .ipynb files)
│
├─ README.md                       # Project overview, usage, and sizing rules
├─ .gitignore                      # Python/git ignores
```

## 🏃 How to Run

This project is run using standard Python.

### 1. Run the Full Test Suite

The `tests.py` file is the main entry point for running the complete analysis for the homework. It will:
1.  Test all database configurations (DB1-DB5).
2.  Print a summary comparison of their total sizes.
3.  Print the sharding strategy analysis.

To run it, execute the following command from the `BigDataStructure-TD1/` directory:

```bash
python run_tests.py
