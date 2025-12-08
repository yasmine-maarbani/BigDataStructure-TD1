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
│  └─ Database_calculator.py       # Main calculator
│
│
├─ test_HW1.py                        # Test HomeWork 1 from data of the TD1
├─ test_HW2.py                        # Test HomeWork 2 from data of the TD2
│
├─ README.md                       # Project overview, usage, and sizing rules
├─ .gitignore                      # Python/git ignores
```

## 🏃 How to Run

This project is run using standard Python.

### 1. Run the Full Test Suite (HW1)

The `test_HW1.py` file is the main entry point for running the complete analysis for the homework 1. It will:
1.  Test all database configurations (DB1-DB5).
2.  Print a summary comparison of their total sizes.
3.  Print the sharding strategy analysis.

Execute this file to lauch the test.

### 2. Run the Full Test Suite (HW2)

The `test_HW2.py` file is the main entry point for running the complete analysis for the homework 2. It will:

- Il initialise plusieurs configurations de base de données (schémas) :
    - `DB1` : modèle normalisé (collections séparées : `Prod`, `St`, `OL`, `Cl`, `Wa`).
    - `DB2` : dénormalisation — `Stock` intégré dans `Product` (stock embarqué dans `Prod`).
    - `DB3` : dénormalisation inverse — `Product` intégré dans `Stock`.
- Il définit un ensemble de requêtes types (Q1..Q5) et plusieurs stratégies de sharding (R1.x..R5.x).
- Pour chaque combinaison pertinente, il simule le coût de la requête en octets (Vt) :
    - Cas "Filter" (pas de JOIN) : calcule le coût sur une collection filtrée (C1).
    - Cas "Join" : estime les coûts combinés (C1 + boucles sur C2) selon la stratégie de sharding.
- Il affiche pour chaque simulation :
    - la requête analysée, la stratégie de sharding utilisée,
    - le résumé des coûts (`C1_sharding_strategy`, `C2_sharding_strategy` si pertinent),
    - `Vt TOTAL` en octets et en MB, et le nombre d'itérations/loops si applicable.

- It initializes several database configurations (schemas):
    - `DB1`: normalized model (separate collections: `Prod`, `St`, `OL`, `Cl`, `Wa`).
    - `DB2`: denormalization — `Stock` integrated into `Product` (stock embedded in `Prod`).
    - `DB3`: reverse denormalization — `Product` integrated into `Stock`.
- It defines a set of typical queries (Q1..Q5) and several sharding strategies (R1.x..R5.x).
- For each relevant combination, it simulates the cost of the query in bytes (Vt):
    - "Filter" case (no JOIN): calculates the cost on a filtered collection (C1).
    - "Join" case: estimates the combined costs (C1 + loops on C2) according to the sharding strategy.
- For each simulation, it displays:
    - the query analyzed, the sharding strategy used,
    - the summary of costs (`C1_sharding_strategy`, `C2_sharding_strategy` if relevant),
    - `Vt TOTAL` in bytes and MB, and the number of iterations/loops if applicable.

Script organization (`test_HW2.py`):

- PART 1: Tests on `DB1` (filter and JOINs for Q1, Q2, Q4, Q5)
- PART 2: Impact of denormalization on query Q4 — comparison between `DB1`, `DB2`, `DB3`.
- PART 3: Summary table comparing models and expected cost.
- OPTIONAL: Tests on Q3 (OrderLine filtered by date)

Execute this file `test_HW1` to launch the test.
