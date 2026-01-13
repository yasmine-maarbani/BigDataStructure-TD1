from nosqlcalc import NoSQLDatabaseCalculator

# ============================================================
# STATISTICS FROM TD
# ============================================================

TD_STATISTICS = {
    'clients': 10**7,                    # 10 million clients
    'products': 10**5,                   # 100k products
    'order_lines': 4 * 10**9,            # 4 billion order lines
    'warehouses': 200,                   # 200 warehouses
    'servers': 1000,                     # 1000 servers for sharding
    'avg_categories_per_product': 2,     # 1-5 categories, avg 2
    'brands': 5000,                      # 5000 distinct brands
    'apple_products': 50,                # 50 Apple products
    'orders_per_client': 100,            # Average orders per client
    'products_per_order': 20,            # Average products per order
    'dates_per_year': 365                # Order lines balanced over 365 dates
}


# ============================================================
# JSON SCHEMAS FOR ALL COLLECTIONS
# ============================================================

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

SCHEMA_WAREHOUSE = {
    "type": "object",
    "properties": {
        "IDW": {"type": "integer"},
        "address": {"type": "string"},
        "capacity": {"type": "integer"}
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


# ============================================================
# DB2: Prod{[Cat], Supp, [St]}
# ============================================================

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


# ============================================================
# DB3: St{Prod{[Cat], Supp}}
# ============================================================

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


# ============================================================
# DB4: OL{Prod{[Cat], Supp}}
# ============================================================

SCHEMA_DB4_ORDERLINE = {
    "type": "object",
    "properties": {
        "IDC": {"type": "integer"},
        "date": {"type": "date"},
        "deliveryDate": {"type": "date"},
        "quantity": {"type": "integer"},
        "comment": {"type": "string"},
        "grade": {"type": "integer"},
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


# ============================================================
# DB5: Prod{[Cat], Supp, [OL]}
# ============================================================

SCHEMA_DB5_PRODUCT = {
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
        "orderline": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "IDC": {"type": "integer"},
                    "date": {"type": "date"},
                    "deliveryDate": {"type": "date"},
                    "quantity": {"type": "integer"},
                    "comment": {"type": "string"},
                    "grade": {"type": "integer"}
                }
            }
        }
    }
}


# ============================================================
# DATABASE CREATION CONFIGURATIONS
# ============================================================

DATABASES = {
    "DB1": {
        "signature": "Prod{[Cat],Supp}, St, Wa, OL, Cl",
        "collections": {
            "Product": SCHEMA_PRODUCT,
            "Stock": SCHEMA_STOCK,
            "Warehouse": SCHEMA_WAREHOUSE,
            "OrderLine": SCHEMA_ORDERLINE,
            "Client": SCHEMA_CLIENT
        }
    },
    "DB2": {
        "signature": "Prod{[Cat],Supp,[St]}, Wa, OL, Cl",
        "collections": {
            "Product": SCHEMA_DB2_PRODUCT,
            "Warehouse": SCHEMA_WAREHOUSE,
            "OrderLine": SCHEMA_ORDERLINE,
            "Client": SCHEMA_CLIENT
        }
    },
    "DB3": {
        "signature": "St{Prod{[Cat],Supp}}, Wa, OL, Cl",
        "collections": {
            "Stock": SCHEMA_DB3_STOCK,
            "Warehouse": SCHEMA_WAREHOUSE,
            "OrderLine": SCHEMA_ORDERLINE,
            "Client": SCHEMA_CLIENT
        }
    },
    "DB4": {
        "signature": "St, Wa, OL{Prod{[Cat],Supp}}, Cl",
        "collections": {
            "Stock": SCHEMA_STOCK,
            "Warehouse": SCHEMA_WAREHOUSE,
            "OrderLine": SCHEMA_DB4_ORDERLINE,
            "Client": SCHEMA_CLIENT
        }
    },
    "DB5": {
        "signature": "Prod{[Cat],Supp,[OL]}, St, Wa, Cl",
        "collections": {
            "Product": SCHEMA_DB5_PRODUCT,
            "Stock": SCHEMA_STOCK,
            "Warehouse": SCHEMA_WAREHOUSE,
            "Client": SCHEMA_CLIENT
        }
    }
}


# ============================================================
# SHARDING TEST CASES
# ============================================================

SHARDING_TESTS = [
    {
        "collection": "Stock",
        "key": "IDP",
        "distinct_values": TD_STATISTICS['products']
    },
    {
        "collection": "Stock",
        "key": "IDW",
        "distinct_values": TD_STATISTICS['warehouses']
    },
    {
        "collection": "OrderLine",
        "key": "IDC",
        "distinct_values": TD_STATISTICS['clients']
    },
    {
        "collection": "OrderLine",
        "key": "IDP",
        "distinct_values": TD_STATISTICS['products']
    },
    {
        "collection": "Product",
        "key": "IDP",
        "distinct_values": TD_STATISTICS['products']
    },
    {
        "collection": "Product",
        "key": "brand",
        "distinct_values": TD_STATISTICS['brands']
    }
]


# ============================================================
# TEST FUNCTIONS
# ============================================================

def test_single_database(db_name: str, db_config: dict, calc: NoSQLDatabaseCalculator):
    """Test a single database configuration."""
    print("\n" + "="*80)
    print(f"DATABASE: {db_name}")
    print(f"Signature: {db_config['signature']}")
    print("="*80)

    for coll_name, schema in db_config['collections'].items():
        calc.add_collection(coll_name, schema)

    calc.print_database_summary()

    print(f"\n{'─'*80}")
    print(f"DETAILED COLLECTION ANALYSIS")
    print(f"{'─'*80}")
    for coll_name in db_config['collections'].keys():
        calc.print_collection_analysis(coll_name)


def test_all_databases():
    """Test all database configurations (DB1-DB5)."""
    print("\n" + "█"*80)
    print("█" + "█"*78 + "█")
    print("█" + "█"*20 + "TD COMPLETE TEST SUITE" + "█"*37 + "█")
    print("█" + "█"*78 + "█")
    print("█"*80)

    # Test each database
    for db_name, db_config in DATABASES.items():
        # Create fresh calculator for each DB
        calc = NoSQLDatabaseCalculator(TD_STATISTICS)
        test_single_database(db_name, db_config, calc)


def compare_databases():
    """Compare sizes of all databases."""
    print("\n" + "="*80)
    print("DATABASE SIZE COMPARISON")
    print("="*80)

    results = []

    for db_name, db_config in DATABASES.items():
        calc = NoSQLDatabaseCalculator(TD_STATISTICS)

        for coll_name, schema in db_config['collections'].items():
            calc.add_collection(coll_name, schema)

        total_gb, details = calc.compute_database_size_gb()
        results.append({
            'name': db_name,
            'signature': db_config['signature'],
            'size_gb': total_gb,
            'collections': len(db_config['collections'])
        })

    print(f"\n{'Database':<10} {'Collections':<15} {'Size (GB)':<15} {'Signature':<50}")
    print("-"*90)
    for r in results:
        print(f"{r['name']:<10} {r['collections']:<15} {r['size_gb']:<15.4f} {r['signature']:<50}")

    print("\n" + "="*80)


def test_sharding():
    """Test all sharding strategies from Section 2.6."""
    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█" + " "*20 + "SHARDING" + " "*24 + "█")
    print("█" + " "*78 + "█")
    print("█"*80)

    print(f"\nConfiguration: {TD_STATISTICS['servers']:,} servers in cluster")
    print(f"Total data: {TD_STATISTICS['order_lines']:,} order lines\n")

    # Use DB1 for sharding tests
    calc = NoSQLDatabaseCalculator(TD_STATISTICS)
    for coll_name, schema in DATABASES["DB1"]['collections'].items():
        calc.add_collection(coll_name, schema)

    # Test each sharding strategy with detailed output
    print("="*90)
    print(f"{'Strategy':<25} {'Docs/Server':<20} {'Distinct Values/Server':<25}")
    print("="*90)

    for test in SHARDING_TESTS:
        stats = calc.compute_sharding_stats(
            test['collection'],
            test['key'],
            test['distinct_values']
        )
        strategy = f"{test['collection']}-#{test['key']}"


        print(f"{strategy:<25} {stats['avg_docs_per_server']:<20,.2f} "
              f"{stats['avg_distinct_values_per_server']:<25,.2f} ")

    print("="*90)


def test_specific_collection_details():
    """Show detailed breakdown for interesting collections."""
    print("\n" + "="*80)
    print("DETAILED EXAMPLES: Key Collections")
    print("="*80)

    calc = NoSQLDatabaseCalculator(TD_STATISTICS)

    examples = [
        ("DB1 Product", "DB1", "Product"),
        ("DB3 Stock (with embedded Product)", "DB3", "Stock"),
        ("DB5 Product (with OrderLines)", "DB5", "Product"),
    ]

    for label, db_name, coll_name in examples:
        print(f"\n{'─'*80}")
        print(f"{label}")
        print(f"{'─'*80}")

        calc = NoSQLDatabaseCalculator(TD_STATISTICS)
        db_config = DATABASES[db_name]
        calc.add_collection(coll_name, db_config['collections'][coll_name])
        calc.print_collection_analysis(coll_name)


# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    print("\n")
    print("█"*80)
    print("█" + " "*78 + "█")
    print("█" + " "*15 + "NoSQL Database Calculator - TD Test Suite" + " "*22 + "█")
    print("█" + " "*25 + "Big Data Structure Course" + " "*29 + "█")
    print("█" + " "*78 + "█")
    print("█"*80)

    # Menu
    print("\nTEST MENU:")
    print("  1. Test ALL databases (DB1-DB5) - Complete analysis")
    print("  2. Compare database sizes")
    print("  3. Test sharding strategies")
    print("  4. Show detailed examples")
    print("  5. Run EVERYTHING")

    choice = input("\nChoose test (1-5) or press Enter for all: ").strip()

    if choice == "1":
        test_all_databases()
    elif choice == "2":
        compare_databases()
    elif choice == "3":
        test_sharding()
    elif choice == "4":
        test_specific_collection_details()
    else:
        test_all_databases()
        compare_databases()
        test_sharding()
        test_specific_collection_details()

    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█" + " "*30 + "TESTS COMPLETED!" + " "*28 + "█")
    print("█" + " "*78 + "█")
    print("█"*80 + "\n")