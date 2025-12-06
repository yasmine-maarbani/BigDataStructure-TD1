import re
from nosqlcalc import NoSQLDatabaseCalculator
from typing import Dict, Optional

# --- 1. SQL QUERY PARSER FUNCTION ---

def parse_sql_query(sql_query: str) -> Dict:
    """
    Parses a simple SQL query (TD type) to extract entry collection, target 
    collection (JOIN), and filter key.
    """
    
    def normalize_coll_name(name):
        name = name.lower()
        if 'product' in name: return "Prod"
        if 'stock' in name: return "St"
        if 'orderline' in name: return "OL"
        if 'client' in name: return "Cl"
        if 'warehouse' in name: return "Wa"
        return name

    # Regex 1: Extract FROM and JOIN collections
    from_match = re.search(r'FROM\s+(\w+)\s+\w+', sql_query, re.IGNORECASE)
    join_match = re.search(r'JOIN\s+(\w+)\s+\w+', sql_query, re.IGNORECASE)
    
    entry_coll_name = normalize_coll_name(from_match.group(1)) if from_match else None
    target_coll_name = normalize_coll_name(join_match.group(1)) if join_match else None

    # Regex 2: Extract filter key (WHERE)
    filter_match = re.search(r'WHERE\s+([\w\.]+)\s*(=|>|<|!=)', sql_query, re.IGNORECASE)
    filter_key_full = filter_match.group(1) if filter_match else None

    filter_key = filter_key_full.split('.')[-1] if filter_key_full and '.' in filter_key_full else (filter_key_full if filter_key_full else None)

    return {
        "ENTRY": entry_coll_name,
        "FILTER": filter_key,
        "TARGET": target_coll_name, 
        "SQL": sql_query.strip()
    }


# --- 2. DEFINITIONS AND CONSTANTS ---

STATS = {
    'clients': 10**7,
    'products': 10**5,
    'order_lines': 4 * 10**9,
    'warehouses': 200,
    'servers': 1000,
    'avg_categories_per_product': 2,
    'brands': 5000,
    'apple_products': 50,
    'orders_per_client': 100,
    'products_per_order': 20,
    'dates_per_year': 365
}

# Schémas JSON pour les collections
SCHEMA_PRODUCT = {
    "type": "object",
    "properties": {
        "IDP": {"type": "integer"},
        "name": {"type": "string"},
        "price": {"type": "number"},
        "brand": {"type": "string"},
        "description": {"type": "string"},
        "image_url": {"type": "string"},
        "categories": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"}
                }
            }
        },
        "supplier": {
            "type": "object",
            "properties": {
                "IDS": {"type": "integer"},
                "name": {"type": "string"},
                "SIRET": {"type": "integer"},
                "headOffice": {"type": "string"},
                "revenue": {"type": "number"}
            }
        }
    }
}

SCHEMA_STOCK = {
    "type": "object",
    "properties": {
        "IDW": {"type": "integer"},
        "IDP": {"type": "integer"},
        "quantity": {"type": "integer"},
        "location": {"type": "string"}
    }
}

SCHEMA_ORDERLINE = {
    "type": "object",
    "properties": {
        "IDP": {"type": "integer"},
        "IDC": {"type": "integer"},
        "date": {"type": "date"},
        "deliveryDate": {"type": "date"},
        "quantity": {"type": "integer"},
        "comment": {"type": "string"},
        "grade": {"type": "integer"}
    }
}

SCHEMA_CLIENT = {
    "type": "object",
    "properties": {
        "IDC": {"type": "integer"},
        "ln": {"type": "string"},
        "fn": {"type": "string"},
        "address": {"type": "string"},
        "nationality": {"type": "string"},
        "birthDate": {"type": "date"},
        "email": {"type": "string"}
    }
}

SCHEMA_WAREHOUSE = {
    "type": "object",
    "properties": {
        "IDW": {"type": "integer"},
        "address": {"type": "string"},
        "capacity": {"type": "integer"}
    }
}

# DB2: Prod avec Stock intégré
SCHEMA_DB2_PRODUCT = {
    "type": "object",
    "properties": {
        "IDP": {"type": "integer"},
        "name": {"type": "string"},
        "price": {"type": "number"},
        "brand": {"type": "string"},
        "description": {"type": "string"},
        "image_url": {"type": "string"},
        "categories": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"}
                }
            }
        },
        "supplier": {
            "type": "object",
            "properties": {
                "IDS": {"type": "integer"},
                "name": {"type": "string"},
                "SIRET": {"type": "integer"},
                "headOffice": {"type": "string"},
                "revenue": {"type": "number"}
            }
        },
        "stock": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "IDW": {"type": "integer"},
                    "quantity": {"type": "integer"},
                    "location": {"type": "string"}
                }
            }
        }
    }
}

# DB3: Stock avec Product intégré
SCHEMA_DB3_STOCK = {
    "type": "object",
    "properties": {
        "IDW": {"type": "integer"},
        "quantity": {"type": "integer"},
        "location": {"type": "string"},
        "product": {
            "type": "object",
            "properties": {
                "IDP": {"type": "integer"},
                "name": {"type": "string"},
                "price": {"type": "number"},
                "brand": {"type": "string"},
                "description": {"type": "string"},
                "image_url": {"type": "string"},
                "categories": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"}
                        }
                    }
                },
                "supplier": {
                    "type": "object",
                    "properties": {
                        "IDS": {"type": "integer"},
                        "name": {"type": "string"},
                        "SIRET": {"type": "integer"},
                        "headOffice": {"type": "string"},
                        "revenue": {"type": "number"}
                    }
                }
            }
        }
    }
}

SCHEMAS_CONFIG = {
    "DB1": {},  # Normalisé
    "DB2": {"Prod": ["St"]},  # Stock dans Product
    "DB3": {"St": ["Prod"]},  # Product dans Stock
}

QUERIES = {
    "Q1": "SELECT S.quantity, S.location FROM Stock S WHERE S.IDP = $IDP AND S.IDW = $IDW;",
    "Q2": "SELECT P.name, P.price FROM Product P WHERE P.brand = $brand;", 
    "Q3": "SELECT O.IDP, O.quantity FROM OrderLine O WHERE O.date = $date;",
    "Q4": "SELECT P.name, S.quantity FROM Stock S JOIN Product P ON S.IDP = P.IDP WHERE S.IDW = $IDW;",
    "Q5": "SELECT P.name, P.price, S.IDW, S.quantity FROM Product P JOIN Stock S ON P.IDP = S.IDP WHERE P.brand = 'Apple';",
}

SHARDING_STRATEGIES = {
    "R1.1": {"St": "IDW"}, 
    "R1.2": {"St": "IDP"},
    "R2.1": {"Prod": "brand"}, 
    "R2.2": {"Prod": "IDP"},
    "R3.1": {"OL": "IDC"},
    "R3.2": {"OL": "IDP"},
    "R4.1": {"St": "IDW", "Prod": "IDP"}, 
    "R4.2": {"St": "IDP", "Prod": "IDP"}, 
    "R5.1": {"Prod": "brand", "St": "IDP"}, 
    "R5.2": {"Prod": "IDP", "St": "IDP"},   
}


# --- 3. UTILITY TEST FUNCTION ---

def run_test_case(calculator: NoSQLDatabaseCalculator, query_name: str, strategy_name: str):
    """
    Executes a Qx/R.x test case using the query parser and generic resolver.
    """
    
    sql_query = QUERIES[query_name]
    query_params = parse_sql_query(sql_query)
    sharding_config = SHARDING_STRATEGIES[strategy_name]
    
    print("\n" + "~"*80)
    print(f"SIMULATION [{calculator.current_schema}] {query_name} ({strategy_name})")
    print(f"  Query: {query_params['SQL']}")
    print(f"  Sharding: {sharding_config}")
    print("~"*80)
    
    entry_coll_name = query_params["ENTRY"]
    entry_filter_key = query_params["FILTER"]
    target_coll_name = query_params["TARGET"]

    if target_coll_name is None:
        # Simple Filter Case (Q1, Q2, Q3)
        result = calculator.compute_filter_query_vt(
            collection_name=entry_coll_name,
            filter_key=entry_filter_key,
            collection_sharding_key=sharding_config.get(entry_coll_name, "N/A")
        )
    else:
        # Join Case (Q4, Q5): Use the generic solver
        result = calculator.resolve_query_strategy(
            entry_coll_name=entry_coll_name,
            entry_filter_key=entry_filter_key,
            target_coll_name=target_coll_name,
            sharding_config=sharding_config
        )

    # Output Summary
    print("\n" + "="*80)
    print(f"[RÉSUMÉ DES COÛTS]")
    print("="*80)
    print(f"C1 ({entry_coll_name}) : {result['C1_sharding_strategy']}")
    
    if result.get("C2_volume", 0) > 0:
        print(f"C2 ({target_coll_name}) : {result['C2_sharding_strategy']} (Loops: {result['Loops']:,})")
    
    print(f"\n→ Vt TOTAL : {result['Vt_total']:,} B = {result['Vt_total']/(1024**2):.2f} MB")
    print("="*80)
    
    return result


def setup_database(schema_name: str) -> NoSQLDatabaseCalculator:
    """
    Configure une base de données avec ses collections.
    
    Args:
        schema_name: "DB1", "DB2", ou "DB3"
    
    Returns:
        NoSQLDatabaseCalculator configuré et prêt
    """
    calc = NoSQLDatabaseCalculator(STATS, current_schema=schema_name)
    
    if schema_name == "DB1":
        # DB1: Modèle normalisé
        calc.add_collection("Prod", SCHEMA_PRODUCT)
        calc.add_collection("St", SCHEMA_STOCK)
        calc.add_collection("OL", SCHEMA_ORDERLINE)
        calc.add_collection("Cl", SCHEMA_CLIENT)
        calc.add_collection("Wa", SCHEMA_WAREHOUSE)
        
    elif schema_name == "DB2":
        # DB2: Stock intégré dans Product
        calc.add_collection("Prod", SCHEMA_DB2_PRODUCT)
        calc.add_collection("OL", SCHEMA_ORDERLINE)
        calc.add_collection("Cl", SCHEMA_CLIENT)
        calc.add_collection("Wa", SCHEMA_WAREHOUSE)
        
    elif schema_name == "DB3":
        # DB3: Product intégré dans Stock
        calc.add_collection("St", SCHEMA_DB3_STOCK)
        calc.add_collection("OL", SCHEMA_ORDERLINE)
        calc.add_collection("Cl", SCHEMA_CLIENT)
        calc.add_collection("Wa", SCHEMA_WAREHOUSE)
    
    # IMPORTANT: Calculer et stocker les tailles
    calc.compute_and_store_sizes()
    
    return calc


# --- 4. HOMEWORK EXECUTION ---

if __name__ == "__main__":
    
    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█" + " "*15 + "HOMEWORK 3.3 - QUERY COST ANALYSIS (Vt)" + " "*22 + "█")
    print("█" + " "*25 + "Big Data Structure Course" + " "*29 + "█")
    print("█" + " "*78 + "█")
    print("█"*80)
    
    # ====================================================================
    # PART 1: DB1 TESTS (Normalized Model, JOIN everywhere)
    # ====================================================================
    
    print("\n" + "="*80)
    print("PART 1 : SIMULATIONS ON DB1 (Normalized Model)")
    print("="*80)
    
    calc_db1 = setup_database("DB1")
    
    print("\n" + "-"*80)
    print("A. FILTER QUERIES (Q1, Q2)")
    print("-"*80)
    
    # Q1 Tests
    run_test_case(calc_db1, "Q1", "R1.1")
    run_test_case(calc_db1, "Q1", "R1.2")
    
    # Q2 Tests
    run_test_case(calc_db1, "Q2", "R2.1")
    run_test_case(calc_db1, "Q2", "R2.2")
    
    print("\n" + "-"*80)
    print("B. JOIN QUERIES - Q4 (Stock → Product)")
    print("-"*80)
    
    run_test_case(calc_db1, "Q4", "R4.1") 
    run_test_case(calc_db1, "Q4", "R4.2")
    
    print("\n" + "-"*80)
    print("C. JOIN QUERIES - Q5 (Product → Stock)")
    print("-"*80)
    
    run_test_case(calc_db1, "Q5", "R5.1") 
    run_test_case(calc_db1, "Q5", "R5.2")

    # ====================================================================
    # PART 2: DENORMALIZATION IMPACT (Generic Q4 Tests)
    # The solver automatically switches to FILTER when embedding is detected.
    # ====================================================================
    
    print("\n" + "="*80)
    print("PART 2 : IMPACT OF DENORMALIZATION ON QUERY Q4")
    print("="*80)
    
    SHARDING_TEST = "R4.2"  # St(#IDP), Prod(#IDP)
    
    # A. DB1 (Normalized - for comparison)
    print("\n" + "-"*80)
    print("A. DB1 (Normalized Model) - BASELINE")
    print("-"*80)
    run_test_case(calc_db1, "Q4", SHARDING_TEST)
    
    # B. DB2 (Stock embedded in Product)
    print("\n" + "-"*80)
    print("B. DB2 (Stock EMBEDDED in Product)")
    print("-"*80)
    calc_db2 = setup_database("DB2")
    run_test_case(calc_db2, "Q4", SHARDING_TEST) 
    
    # C. DB3 (Product embedded in Stock)
    print("\n" + "-"*80)
    print("C. DB3 (Product EMBEDDED in Stock)")
    print("-"*80)
    calc_db3 = setup_database("DB3")
    run_test_case(calc_db3, "Q4", SHARDING_TEST)
    
    # ====================================================================
    # PART 3: COMPARISON SUMMARY
    # ====================================================================
    
    print("\n" + "="*80)
    print("PART 3 : COMPARISON SUMMARY FOR Q4 with R4.2")
    print("="*80)
    
    print("\nDatabase Model Impact on Query Cost:")
    print("-"*80)
    print(f"{'Database':<15} {'Model':<30} {'Join Required?':<20} {'Expected Cost':<20}")
    print("-"*80)
    print(f"{'DB1':<15} {'Normalized':<30} {'YES (2 collections)':<20} {'HIGH (C1+C2)':<20}")
    print(f"{'DB2':<15} {'St in Prod':<30} {'NO (embedded)':<20} {'MEDIUM (Filter)':<20}")
    print(f"{'DB3':<15} {'Prod in St':<30} {'NO (embedded)':<20} {'LOW (Filter)':<20}")
    print("-"*80)
    
    print("\nKey Insights:")
    print("  • DB1: Requires JOIN → C1 (filter Stock) + C2 (loop on Product)")
    print("  • DB2: Stock embedded in Product → Filter on Product only")
    print("  • DB3: Product embedded in Stock → Filter on Stock only (BEST for Q4)")
    print("  • Denormalization eliminates joins but increases document size")
    
    # ====================================================================
    # OPTIONAL: Q3 Test (if needed)
    # ====================================================================
    
    print("\n" + "="*80)
    print("OPTIONAL: Q3 TEST (OrderLine filter by date)")
    print("="*80)
    
    run_test_case(calc_db1, "Q3", "R3.1")
    run_test_case(calc_db1, "Q3", "R3.2")
    
    # ====================================================================
    # FINAL MESSAGE
    # ====================================================================
    
    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█" + " "*25 + "ALL TESTS COMPLETED!" + " "*33 + "█")
    print("█" + " "*78 + "█")
    print("█"*80 + "\n")
    
    print("\n📊 Summary:")
    print("  ✓ Filter queries tested (Q1, Q2, Q3)")
    print("  ✓ Join queries tested (Q4, Q5)")
    print("  ✓ Different sharding strategies analyzed")
    print("  ✓ Denormalization impact demonstrated")
    print("  ✓ Real document sizes used (not mocked)")
    print("\n🎓 Ready for evaluation!\n")