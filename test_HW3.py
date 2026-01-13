import re
from nosqlcalc import NoSQLDatabaseCalculator
from typing import Dict, Optional

# --- 1. SQL QUERY PARSER FUNCTION ---

def parse_sql_query(sql_query: str) -> Dict:
    """
    Parse les requêtes SQL (simples et agrégées) pour extraire les paramètres
    nécessaires au simulateur NoSQL (Entry, Filter, GroupBy, Limit, Target).
    """
    
    def normalize_coll_name(name):
        if not name: return None
        name = name.lower()
        if 'product' in name: return "Prod"
        if 'stock' in name: return "St"
        if 'orderline' in name: return "OL"
        if 'client' in name: return "Cl"
        if 'warehouse' in name: return "Wa"
        return name
    all_froms = re.findall(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
    
    if len(all_froms) > 1:
        # Cas Q6/Q7 : La collection d'entrée est dans la sous-requête (la dernière citée)
        entry_coll_name = normalize_coll_name(all_froms[-1])
        target_coll_name = normalize_coll_name(all_froms[0])
        print("ICIIIII")
    else:
        entry_coll_name = normalize_coll_name(all_froms[0]) if all_froms else None
        join_match = re.search(r'JOIN\s+(\w+)', sql_query, re.IGNORECASE)
        target_coll_name = normalize_coll_name(join_match.group(1)) if join_match else None
        print("LA")
    # 2. Extraction du filtre (WHERE)
    filter_match = re.search(r'WHERE\s+([\w\.]+)\s*(=|>|<|!=)', sql_query, re.IGNORECASE)
    filter_key = filter_match.group(1).split('.')[-1] if filter_match else target_coll_name if target_coll_name else entry_coll_name

    # 3. NOUVEAU : Extraction du Group By (détermine le Shuffle)
    group_match = re.search(r'GROUP\s+BY\s+([\w\.]+)', sql_query, re.IGNORECASE)
    group_key = group_match.group(1).split('.')[-1] if group_match else None

    # 4. NOUVEAU : Extraction de l'agrégat (ex: SUM(quantity))
    agg_match = re.search(r'(SUM|AVG|COUNT|MAX|MIN)\(([\w\.\*]+)\)', sql_query, re.IGNORECASE)
    agg_type = agg_match.group(1).upper() if agg_match else None
    agg_field = agg_match.group(2).split('.')[-1] if agg_match else None

    # 5. NOUVEAU : Extraction du LIMIT (détermine les Loops de la phase 2)
    limit_match = re.search(r'LIMIT\s+(\d+)', sql_query, re.IGNORECASE)
    limit_val = int(limit_match.group(1)) if limit_match else None

    return {
        "ENTRY": entry_coll_name,      # Ex: OL
        "TARGET": target_coll_name,    # Ex: Prod
        "FILTER": filter_key,          # Ex: idClient
        "GROUP_BY": group_key,         # Ex: IDP
        "AGG_TYPE": agg_type,          # Ex: SUM
        "AGG_FIELD": agg_field,        # Ex: quantity
        "LIMIT": limit_val,            # Ex: 100 ou 1
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
    "Q6": """SELECT P.name, P.price, OL.NB FROM Product P JOIN (
                SELECT O.IDP, SUM(O.quantity) AS NB FROM OrderLine O 
                GROUP BY O.IDP
             ) OL ON P.IDP = O.IDP ORDER BY OL.NB DESC LIMIT 100;""",
             
    "Q7": """SELECT P.name, P.price, OL.NB FROM Product P JOIN (
                SELECT O.IDP, SUM(O.quantity) AS NB FROM OrderLine O 
                WHERE O.IDC = 125 GROUP BY O.IDP
             ) OL ON P.IDP = OL.IDP ORDER BY OL.NB DESC LIMIT 1;"""
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
    "R6.1": {"OL": "IDC", "Prod": "IDP"},   # Shuffle car GroupBy(IDP) != Shard(IDC)
    "R6.2": {"OL": "IDP", "Prod": "brand"}, # Local car GroupBy(IDP) == Shard(IDP)
    "R7.1": {"OL": "IDC", "Prod": "IDP"},   # Local car Filter(IDC=125) == Shard(IDC)
    "R7.2": {"OL": "IDP", "Prod": "IDP"},   
}


# --- 3. UTILITY TEST FUNCTION ---

def run_test_case(calculator: NoSQLDatabaseCalculator, query_name: str, strategy_name: str):
    sql_query = QUERIES[query_name]
    query_params = parse_sql_query(sql_query)
    sharding_config = SHARDING_STRATEGIES[strategy_name]
    
    print("\n" + "~"*80)
    print(f"SIMULATION [{calculator.current_schema}] {query_name} ({strategy_name})")
    print(f"  SQL: {query_params['SQL'][:100]}...") # Tronqué pour lisibilité
    print(f"  Sharding: {sharding_config}")
    print("~"*80)

    # DETECTION DE L'ALGORITHME
    if query_params.get("GROUP_BY"):
        # CAS AGGREGATE (Q6, Q7)
        result = calculator.compute_aggregate_query_vt(
            entry_coll_name=query_params["ENTRY"],
            group_key=query_params["GROUP_BY"],
            filter_key=query_params["FILTER"],
            limit=query_params["LIMIT"],
            target_coll_name=query_params["TARGET"],
            sharding_config=sharding_config,
            sql_query=sql_query  # Pass the real SQL query
        )
    elif query_params["TARGET"] is None:
        # CAS FILTER SIMPLE (Q1, Q2, Q3)
        result = calculator.compute_filter_query_vt(
            collection_name=query_params["ENTRY"],
            filter_key=query_params["FILTER"],
            collection_sharding_key=sharding_config.get(query_params["ENTRY"], "N/A"),
            sql_query=sql_query  
        )
    else:
        # CAS JOIN CLASSIQUE (Q4, Q5)
        result = calculator.resolve_query_strategy(
            entry_coll_name=query_params["ENTRY"],
            entry_filter_key=query_params["FILTER"],
            target_coll_name=query_params["TARGET"],
            sharding_config=sharding_config,
            sql_query=sql_query  
        )

    # Affichage du résumé
    print(f"\n[COÛTS {query_name}]")
    print(f"  Phase C1 ({query_params['ENTRY']}) : {result.get('C1_sharding_strategy', 'N/A')}")
    if result.get("Shuffle_volume"):
        print(f"  SHUFFLE : {result['Shuffle_volume']:,} B")
    if query_params.get("TARGET"):
        print(f"  Phase C2 ({query_params['TARGET']}) : {result.get('C2_sharding_strategy', 'N/A')} (Loops: {result.get('Loops', 1)})")
    
    print(f"\n→ TOTAL Vt : {result['Vt_total']:,} B")
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
    # PART 4: AGGREGATE QUERIES (Q6, Q7)
    # ====================================================================
    print("\n" + "="*80)
    print("PART 4 : AGGREGATE QUERIES (Shuffle & Lookups)")
    print("="*80)

    calc_db1 = setup_database("DB1")

    # Tests Q6 : Top 100 Products
    run_test_case(calc_db1, "Q6", "R6.1") # Cas coûteux (Shuffle)
    run_test_case(calc_db1, "Q6", "R6.2") # Cas optimisé (No shuffle)

    # Tests Q7 : Favorite product of Client 125
    run_test_case(calc_db1, "Q7", "R7.1") # Cas optimisé (C125 sur 1 serveur)
    run_test_case(calc_db1, "Q7", "R7.2") # Cas coûteux (Scan global)
    
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