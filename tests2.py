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

STATS = {'clients': 10**7, 'products': 10**5, 'order_lines': 4 * 10**9, 'warehouses': 200}

SCHEMAS_CONFIG = {
    "DB1": {}, 
    "DB2": {"Prod": ["St"]}, 
    "DB3": {"St": ["Prod"]}, 
}

QUERIES = {
    "Q2": "SELECT P.name, P.price FROM Product P WHERE P.brand = $brand;", 
    "Q4": "SELECT P.name, S.quantity FROM Stock S JOIN Product P ON S.IDP = P.IDP WHERE S.IDW = $IDW;",
    "Q5": "SELECT P.name, P.price, S.IDW, S.quantity FROM Product P JOIN Stock S ON P.IDP = S.IDP WHERE P.brand = 'Apple';",
}

SHARDING_STRATEGIES = {
    "R2.1": {"Prod": "brand"}, "R2.2": {"Prod": "IDP"},   
    "R4.1": {"St": "IDW", "Prod": "IDP"}, "R4.2": {"St": "IDP", "Prod": "IDP"}, 
    "R5.1": {"Prod": "brand", "St": "IDP"}, "R5.2": {"Prod": "IDP", "St": "IDP"},   
}

# --- 3. UTILITY TEST FUNCTION ---

def run_test_case(calculator: NoSQLDatabaseCalculator, query_name: str, strategy_name: str):
    """
    Executes a Qx/R.x test case using the query parser and generic resolver.
    """
    
    sql_query = QUERIES[query_name]
    query_params = parse_sql_query(sql_query)
    sharding_config = SHARDING_STRATEGIES[strategy_name]
    
    print("\n" + "~"*50)
    print(f"SIMULATION [DB: {calculator.current_schema}] {query_name} ({strategy_name})")
    print(f"  Query: {query_params['SQL']}")
    print(f"  Sharding: {sharding_config}")
    print("~"*50)
    
    entry_coll_name = query_params["ENTRY"]
    entry_filter_key = query_params["FILTER"]
    target_coll_name = query_params["TARGET"]

    if target_coll_name is None:
        # Simple Filter Case (Q2)
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
    print("\n[COST AND STRATEGY SUMMARY]")
    print(f"C1 ({entry_coll_name}) Operator: {result['C1_sharding_strategy']}")
    
    if result.get("C2_volume", 0) > 0:
        print(f"C2 ({target_coll_name}) Operator: {result['C2_sharding_strategy']} (Loops: {result['Loops']:,})")
    
    print(f"\nVt TOTAL : {result['Vt_total']:,} B")
    print("-" * 50)
    return result

# --- 4. HOMEWORK EXECUTION ---

if __name__ == "__main__":
    
    # 1. Base Initialization (No 'current_schema' argument)
    calc = NoSQLDatabaseCalculator(STATS)
    
    # 2. MANUAL INJECTION of required attributes to fix 'AttributeError'
    calc.SCHEMAS = SCHEMAS_CONFIG
    
    # ====================================================================
    # PART 1: DB1 TESTS (Normalized Model, JOIN everywhere)
    # ====================================================================
    
    # Set current schema context
    calc.current_schema = "DB1" 
    calc.schema_map = calc.SCHEMAS.get(calc.current_schema, {}) 
    
    print("\n" + "="*80)
    print("PART 1 : SIMULATION ON DB1 (Normalized Model)")
    print("="*80)
    
    # A. Filter Tests (Q2)
    run_test_case(calc, "Q2", "R2.1")
    run_test_case(calc, "Q2", "R2.2")

    # B. Join Tests Q4 (St -> Prod)
    run_test_case(calc, "Q4", "R4.1") 
    run_test_case(calc, "Q4", "R4.2")

    # C. Join Tests Q5 (Prod -> St)
    run_test_case(calc, "Q5", "R5.1") 
    run_test_case(calc, "Q5", "R5.2")


    # ====================================================================
    # PART 2: DENORMALIZATION IMPACT (Generic Q4 Tests)
    # The solver automatically switches to FILTER when embedding is detected.
    # ====================================================================
    
    print("\n" + "="*80)
    print("PART 2 : IMPACT OF SCHEMA (DB2, DB3) ON QUERY Q4")
    print("="*80)
    
    SHARDING_TEST = "R4.2" 

    # B. DB2 (Stock is in Product: S in P)
    calc.current_schema = "DB2" 
    calc.schema_map = calc.SCHEMAS.get(calc.current_schema, {})
    run_test_case(calc, "Q4", SHARDING_TEST) 

    # C. DB3 (Product is in Stock: P in S)
    calc.current_schema = "DB3" 
    calc.schema_map = calc.SCHEMAS.get(calc.current_schema, {})
    run_test_case(calc, "Q4", SHARDING_TEST)