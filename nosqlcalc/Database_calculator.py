from typing import Dict, List, Tuple, Optional
import re

class NoSQLDatabaseCalculator:
    """
    Size calculator for NoSQL databases.

    Usage:
        stats = {'clients': 10**7, 'products': 10**5, ...}
        calc = NoSQLDatabaseCalculator(stats)
        calc.add_collection("Product", schema)
        calc.print_collection_analysis("Product")
    """

    SIZE_NUMBER = 8
    SIZE_STRING = 80
    SIZE_DATE = 20
    SIZE_LONG_STRING = 200
    SIZE_KEY_VALUE = 12

    def __init__(self, statistics: Dict, current_schema: str = "DB1"):
        """Initializes the calculator with schema support."""
        self.statistics = statistics
        self.collections = {}
        self.current_schema = current_schema
        self.schema_map = {}
        self.num_shards = statistics.get('servers', 1000) # Number of servers for sharding
        
        # Mapping of denormlization schemas
        self.SCHEMAS = {
            "DB1": {}, 
            "DB2": {"Prod": ["St"]},  
            "DB3": {"St": ["Prod"]}, 
            "DB4": {"OL": ["Prod"]},  
            "DB5": {"Prod": ["OL"]},  
        }
        self.schema_map = self.SCHEMAS.get(current_schema, {})
        
        # Number of documents per collection
        self.nb_docs = {
            "Cl": statistics.get('clients', 10**7),
            "Prod": statistics.get('products', 10**5),
            "OL": statistics.get('order_lines', 4 * 10**9),
            "Wa": statistics.get('warehouses', 200),
        }
        self.nb_docs["St"] = self.nb_docs["Prod"] * self.nb_docs["Wa"]
        
        # Storage of calculated document sizes
        self.computed_sizes = {}

        # Relationship matrix: how many children per parent
        self.avg_length = {
            "Cl": {
                "Prod": 20,
                "OL": self.nb_docs["OL"] / self.nb_docs["Cl"],  # 400
            },
            "Prod": {
                "Cat": statistics.get('avg_categories_per_product', 2),
                "St": self.nb_docs["Wa"],  # 200
                "Wa": self.nb_docs["Wa"],  # 200
                "Supp": 1,
                "OL": self.nb_docs["OL"] / self.nb_docs["Prod"],  # 40,000
                "Cl": self.nb_docs["Cl"] / self.nb_docs["Prod"],  # 100
            },
            "OL": {
                "Prod": 1, "Cl": 1, "Wa": 1, "St": 1,
            },
            "Wa": {
                "St": self.nb_docs["Prod"],  # 100k
                "Prod": self.nb_docs["Prod"] / self.nb_docs["Wa"],  # 500
                "OL": self.nb_docs["OL"] / self.nb_docs["Wa"],  # 20M
            },
            "St": {
                "Prod": 1, "Wa": 1,
            },
        }

        # Mapping: array name -> collection type
        self.array_to_collection = {
            "categories": "Cat",
            "supplier": "Supp",
            "stock": "St",
            "orderline": "OL",
            "product": "Prod",
            "client": "Cl",
            "warehouse": "Wa"
        }

    # ================================================================
    # AUTOMATIC COLLECTION DETECTION
    # ================================================================

    def guess_collection_name(self, schema: Dict) -> str:
        """
        Detects the collection type from the schema.

        Rules:
            - Prod: has IDP and price
            - Cat: has title
            - Supp: has IDS and SIRET
            - St: has location and quantity
            - Cl: has IDC and email
            - OL: has date and deliveryDate
            - Wa: has IDW and capacity
        """
        if not isinstance(schema, dict):
            return "Unknown"

        props = set(schema.get("properties", {}).keys())

        if {"IDP", "price"}.issubset(props): return "Prod"
        if {"title"}.issubset(props): return "Cat"
        if {"IDS", "SIRET"}.issubset(props): return "Supp"
        if {"location", "quantity"}.issubset(props): return "St"
        if {"IDC", "email"}.issubset(props): return "Cl"
        if {"date", "deliveryDate"}.issubset(props): return "OL"
        if {"IDW", "capacity"}.issubset(props): return "Wa"

        return "Unknown"

    # ================================================================
    # SCALAR COUNTING WITH PARENT TRACKING
    # ================================================================

    def count_scalars_with_arrays(self, schema: Dict) -> Tuple[Dict, Dict]:
        """
        Counts scalars, distinguishing those inside/outside arrays.
        Also tracks the parent collection of each array.

        Returns:
            (counts_outside, inside)

            counts_outside = {int, string, date, long}
            inside = {
                "array_name": {
                    "counts": {int, string, date, long},
                    "parent": "Prod"  ← Owning collection
                }
            }
        """
        counts_outside = {"int": 0, "string": 0, "date": 0, "long": 0}
        inside = {}

        def init_array(arr_name: str, parent_coll: str):
            """Initializes the counter for an array."""
            if arr_name not in inside:
                inside[arr_name] = {
                    "counts": {"int": 0, "string": 0, "date": 0, "long": 0},
                    "parent": parent_coll
                }

        def add_scalar(node_type: str, field_name: str, current_array: Optional[str]):
            """Adds a scalar to the correct counter."""
            target = inside[current_array]["counts"] if current_array else counts_outside

            if node_type in ["integer", "number"]:
                target["int"] += 1
            elif node_type == "string":
                if field_name in ["description", "comment"]:
                    target["long"] += 1
                else:
                    target["string"] += 1
            elif node_type == "date":
                target["date"] += 1

        def explore(node, current_coll: str, field_name: Optional[str] = None,
                   current_array: Optional[str] = None):
            """Recursively explores the schema."""
            if not isinstance(node, dict):
                return

            node_type = node.get("type")

            # ARRAY
            if node_type == "array":
                arr_name = field_name
                init_array(arr_name, current_coll)
                items = node.get("items")

                if isinstance(items, dict):
                    explore(items, current_coll, None, arr_name)
                elif isinstance(items, list):
                    for it in items:
                        explore(it, current_coll, None, arr_name)
                return

            # OBJECT (can change collection)
            if node_type == "object":
                detected = self.guess_collection_name(node)
                if detected != "Unknown":
                    current_coll = detected

                for k, sub in node.get("properties", {}).items():
                    explore(sub, current_coll, k, current_array)
                return

            # SCALAR
            add_scalar(node_type, field_name, current_array)

        root_coll = self.guess_collection_name(schema)
        explore(schema, root_coll)
        return counts_outside, inside

    # ================================================================
    # MERGE COUNTING (COLLECTION TRANSITIONS)
    # ================================================================

    def count_merges(self, schema: Dict, parent_coll: Optional[str] = None,
                     is_root: bool = True) -> int:
        """
        Counts transitions between collections (merges).

        A merge = transition from one collection to another
        Example: Prod → Cat, Prod → Supp
        """
        if not isinstance(schema, dict):
            return 0

        node_type = schema.get("type")
        merges = 0
        coll = None

        # Detects the collection of this node
        if node_type == "object":
            coll = self.guess_collection_name(schema)
        elif node_type == "array":
            items = schema.get("items")
            if isinstance(items, dict):
                coll = self.guess_collection_name(items)

        # Counts the merge if collection changes (not at root)
        if not is_root and coll not in (None, "Unknown") and coll != parent_coll:
            merges += 1
            parent_for_children = coll
        else:
            parent_for_children = parent_coll if parent_coll else coll

        # Recursion
        if node_type == "object":
            for sub in schema.get("properties", {}).values():
                merges += self.count_merges(sub, parent_for_children, False)
        elif node_type == "array":
            items = schema.get("items")
            if isinstance(items, dict):
                merges += self.count_merges(items, parent_for_children, False)
            elif isinstance(items, list):
                for it in items:
                    merges += self.count_merges(it, parent_for_children, False)

        return merges

    # ================================================================
    # DOCUMENT SIZE CALCULATION
    # ================================================================

    def compute_document_size(self, schema: Dict) -> Dict:
        """
        Calculates the full size of a document.

        Formula:
            doc_size = scalars_outside_arrays + scalars_inside_arrays + keys

        Uses realistic avg_length for arrays.
        """
        parent = self.guess_collection_name(schema)
        outside, inside = self.count_scalars_with_arrays(schema)
        avg_used = {}

        # Size of scalars outside arrays
        size_outside = (
            outside["int"] * self.SIZE_NUMBER +
            outside["string"] * self.SIZE_STRING +
            outside["date"] * self.SIZE_DATE +
            outside["long"] * self.SIZE_LONG_STRING
        )

        # Size of scalars inside arrays (with realistic averages)
        size_inside_total = 0
        for array_name, info in inside.items():
            counts = info["counts"]
            parent_for_avg = info["parent"] or parent
            child = self.array_to_collection.get(array_name, "Unknown")

            # Gets the average from the relationship matrix
            avg = self.avg_length.get(parent_for_avg, {}).get(child, 1)
            if avg is None:
                avg = 1

            avg_used[array_name] = avg

            size_arr = (
                counts["int"] * self.SIZE_NUMBER +
                counts["string"] * self.SIZE_STRING +
                counts["date"] * self.SIZE_DATE +
                counts["long"] * self.SIZE_LONG_STRING
            ) * avg

            size_inside_total += size_arr

        # Size of keys
        keys_outside = sum(outside.values())
        keys_arrays = sum(
            sum(info["counts"].values()) * avg_used.get(name, 1)
            for name, info in inside.items()
        )
        merges = self.count_merges(schema)
        size_keys_total = (keys_outside + keys_arrays + merges) * self.SIZE_KEY_VALUE

        # Total
        doc_size = size_outside + size_inside_total + size_keys_total
        nb = self.nb_docs.get(parent, 1)
        collection_size = doc_size * nb

        return {
            "collection": parent,
            "nb_docs": nb,
            "avg_lengths": avg_used,
            "size_outside": size_outside,
            "size_inside": size_inside_total,
            "size_keys": size_keys_total,
            "merges": merges,
            "doc_size": doc_size,
            "collection_size": collection_size
        }

    # ================================================================
    # COLLECTION MANAGEMENT
    # ================================================================

    def add_collection(self, name: str, schema: Dict, doc_count: Optional[int] = None):
        """
        Adds a collection.

        Args:
            name: Collection name (e.g., "Product")
            schema: JSON Schema
            doc_count: Number of docs (auto-detected if None)
        """
        if doc_count is None:
            detected = self.guess_collection_name(schema)
            doc_count = self.nb_docs.get(detected, 1)

        self.collections[name] = {
            'schema': schema,
            'doc_count': doc_count
        }

    def compute_collection_size_gb(self, collection_name: str) -> float:
        """Calculates the size of a collection in GB."""
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' not found")

        coll = self.collections[collection_name]
        result = self.compute_document_size(coll['schema'])
        return result['collection_size'] / (10 ** 9)

    def compute_database_size_gb(self) -> Tuple[float, Dict[str, float]]:
        """
        Calculates the total size of the database.

        Returns:
            (total_gb, {collection_name: size_gb})
        """
        total_gb = 0
        details = {}

        for coll_name in self.collections:
            size = self.compute_collection_size_gb(coll_name)
            details[coll_name] = size
            total_gb += size

        return total_gb, details

    def analyze_collection(self, collection_name: str) -> Dict:
        """Comprehensive analysis of a collection."""
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' not found")

        schema = self.collections[collection_name]['schema']
        result = self.compute_document_size(schema)
        outside, inside = self.count_scalars_with_arrays(schema)

        return {
            'collection_name': collection_name,
            'detected_type': result['collection'],
            'document_count': result['nb_docs'],
            'scalars_outside': outside,
            'scalars_inside': inside,
            'array_averages': result['avg_lengths'],
            'merge_count': result['merges'],
            'document_size_bytes': result['doc_size'],
            'size_breakdown': {
                'outside': result['size_outside'],
                'inside': result['size_inside'],
                'keys': result['size_keys']
            },
            'collection_size_gb': round(result['collection_size'] / 10**9, 4)
        }

    # ================================================================
    # SHARDING
    # ================================================================

    def compute_sharding_stats(self, collection_name: str, sharding_key: str,
                              distinct_key_values: int, num_servers: int = 1000) -> Dict:
        """
        Calculates distribution statistics with sharding.

        Args:
            collection_name: Collection name
            sharding_key: Sharding key (e.g., 'IDP', 'IDC')
            distinct_key_values: Number of distinct values
            num_servers: Number of servers (default: 1000)
        """
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' not found")

        total_docs = self.collections[collection_name]['doc_count']

        return {
            'collection': collection_name,
            'sharding_key': sharding_key,
            'total_docs': total_docs,
            'distinct_values': distinct_key_values,
            'num_servers': num_servers,
            'avg_docs_per_server': round(total_docs / num_servers, 2),
            'avg_distinct_values_per_server': round(distinct_key_values / num_servers, 2)
        }

    # ================================================================
    # PRINT
    # ================================================================

    def print_collection_analysis(self, collection_name: str):
        """Prints the analysis of a collection."""
        analysis = self.analyze_collection(collection_name)

        print("\n" + "="*70)
        print(f"COLLECTION: {analysis['collection_name']}")
        print(f"Detected type: {analysis['detected_type']}")
        print("="*70)

        print(f"\nSTATISTICS:")
        print(f"  • Documents: {analysis['document_count']:,}")
        print(f"  • Merges: {analysis['merge_count']}")

        print(f"\nSCALARS OUTSIDE ARRAYS:")
        for key, val in analysis['scalars_outside'].items():
            if val > 0:
                print(f"  • {key}: {val}")

        print(f"\nSCALARS INSIDE ARRAYS:")
        if analysis['scalars_inside']:
            for array_name, info in analysis['scalars_inside'].items():
                avg = analysis['array_averages'].get(array_name, 1)
                print(f"  • Array '{array_name}' (average: {avg:,.0f}):")
                for key, val in info['counts'].items():
                    if val > 0:
                        print(f"    - {key}: {val} × {avg:,.0f} = {val * avg:,.0f}")
        else:
            print("  (none)")

        breakdown = analysis['size_breakdown']
        print(f"\nSIZE:")
        print(f"  • Scalars (outside arrays): {breakdown['outside']:,} B")
        print(f"  • Scalars (inside arrays): {breakdown['inside']:,} B")
        print(f"  • Keys: {breakdown['keys']:,} B")
        print(f"  • DOCUMENT: {analysis['document_size_bytes']:,} B")
        print(f"  • COLLECTION: {analysis['collection_size_gb']:.4f} GB")
        print("="*70)

    def print_database_summary(self):
        """Prints the database summary."""
        total_gb, details = self.compute_database_size_gb()

        print(f"\n{'='*70}")
        print(f"DATABASE SUMMARY")
        print(f"{'='*70}")

        print(f"\nCOLLECTIONS:")
        for coll_name, size_gb in details.items():
            doc_count = self.collections[coll_name]['doc_count']
            print(f"  • {coll_name:15s}: {size_gb:10.4f} GB  ({doc_count:,} docs)")

        print(f"\nTOTAL: {total_gb:.4f} GB")
        print(f"{'='*70}\n")

    def print_sharding_stats(self, collection_name: str, sharding_key: str,
                            distinct_values: int):
        """Prints sharding statistics."""
        stats = self.compute_sharding_stats(collection_name, sharding_key, distinct_values)

        print(f"\nSHARDING: {stats['collection']}-#{stats['sharding_key']}")
        print(f"  • Total documents: {stats['total_docs']:,}")
        print(f"  • Distinct values: {stats['distinct_values']:,}")
        print(f"  • Servers: {stats['num_servers']:,}")
        print(f"  • Docs/server: {stats['avg_docs_per_server']:,.2f}")
        print(f"  • Distinct values/server: {stats['avg_distinct_values_per_server']:,.2f}")





    # ================================================================
    # QUERY COST (VT) CALCULATION - HOMEWORK 3.3
    # ================================================================
    
    def compute_and_store_sizes(self):
        """    
        Calculates the sizes of all documents and stores them.    
        To be called after adding all collections.    
        """
        for coll_name, coll_data in self.collections.items():
            schema = coll_data['schema']
            result = self.compute_document_size(schema)
            
            detected_type = result['collection']
            
            self.computed_sizes[coll_name] = {
                'type': detected_type,
                'doc_size': result['doc_size'],
                'size_outside': result['size_outside'],
                'size_inside': result['size_inside'],
                'size_keys': result['size_keys']
            }
            
            print(f"✓ Stored size for {coll_name} ({detected_type}): {result['doc_size']} B")

    def compute_filter_query_vt(self, collection_name: str, filter_key: str, 
                               collection_sharding_key: str,
                               sql_query: str) -> Dict:
        """
        Computes the Vt cost for a simple filter query (Vt = C1).
        
        Args:
            collection_name: Collection to query
            filter_key: Key used in the WHERE filter
            collection_sharding_key: Collection sharding key
            sql_query: Complete SQL query
        
        Returns:
            Dict containing cost details
        """
        # Determine whether the query is sharded
        is_sharded = (filter_key == collection_sharding_key)
        
        # Calculate #S1 (number of shards contacted)
        S1 = 1 if is_sharded else self.num_shards
        
        # Call get_query_stats with the SQL query to obtain size_S and size_O.
        num_O1, size_S1, size_O1 = self.get_query_stats(
            collection_name, 
            filter_key, 
            "C1",
            sql_query
        )
        
        # Calculation of volume C1
        C1_volume = S1 * size_S1 + num_O1 * size_O1
        
        # Name of the operation
        op_name = f"Filter {'with' if is_sharded else 'without'} sharding"
        
        # Displaying results
        print(f"\n--- Filter Cost ({op_name}) ---")
        print(f"Collection: {collection_name}")
        print(f"Filter on: {filter_key}")
        print(f"Sharding on: {collection_sharding_key}")
        print(f"\nFormula: C1 = #S1 × size_S1 + #O1 × size_O1")
        print(f"        C1 = {S1} × {size_S1} + {num_O1} × {size_O1}")
        print(f"        C1 = {S1 * size_S1} + {num_O1 * size_O1}")
        print(f"        C1 = {C1_volume:,} B")
        
        print(f"\nDetails:")
        print(f"  • #S1 (servers contacted) = {S1}")
        print(f"  • size_S1 (query size) = {size_S1} B")
        print(f"  • #O1 (returned documents) = {num_O1:,}")
        print(f"  • size_O1 (size per document) = {size_O1} B")
        
        return {
            "query_type": "Filter",
            "Vt_total": C1_volume,
            "C1_volume": C1_volume,
            "C1_sharding_strategy": op_name,
            "C2_volume": 0,
            "Loops": 0,
            "details": {
                "S1": S1,
                "size_S1": size_S1,
                "num_O1": num_O1,
                "size_O1": size_O1,
                "volume": C1_volume,
                "is_sharded": is_sharded
            }
        }
    
    def compute_join_query_vt(self, coll1_name: str, coll1_filter_key: str, 
                            coll1_sharding_key: str, coll2_name: str, 
                            coll2_join_key: str, coll2_sharding_key: str,
                            sql_query: str) -> Dict:
        """
        Computes the Vt cost for a join query (Vt = C1 + loops * C2).
        
        Args:
            coll1_name: Collection 1 (input)
            coll1_filter_key: Filter key for C1
            coll1_sharding_key: Sharding key for C1
            coll2_name: Collection 2 (join target)
            coll2_join_key: Join key for C2
            coll2_sharding_key: Sharding key for C2
            sql_query: Complete SQL query
        """
        # === C1: Initial query on collection 1 ===
        is_sharded_C1 = (coll1_filter_key == coll1_sharding_key)
        S1 = 1 if is_sharded_C1 else self.num_shards
        
        # Calculate size_S1, size_O1, and #O1 with the complete SQL query.
        num_O1, size_S1, size_O1 = self.get_query_stats(
            coll1_name, 
            coll1_filter_key, 
            "C1",
            sql_query
        )
        
        C1_volume = S1 * size_S1 + num_O1 * size_O1
        loops = num_O1  # Number of loops = number of documents returned by C1
        
        # === C2: Join query on collection 2 ===
        is_sharded_C2 = (coll2_join_key == coll2_sharding_key)
        S2 = 1 if is_sharded_C2 else self.num_shards
        
        print(f"\n  → Computing C2 sizes:")
        
        num_O2, size_S2, _ = self.get_query_stats(
            coll2_name, 
            coll2_join_key, 
            "C2",
            sql_query
        )
        
        # size_O2: PROJECTION sans JOIN (uniquement les champs SELECT de coll2)
        # On retire le JOIN pour ne garder que les champs projetés
        query_projection_c2 = self._create_projection_query(sql_query, coll2_name, remove_join=True)
        print(f"    Projection C2 : {query_projection_c2}")
        counts_o2 = self.analyze_schema_fields(coll2_name, query=query_projection_c2)
        size_O2 = self.compute_size_from_counts(counts_o2)
        
        C2_per_loop = S2 * size_S2 + num_O2 * size_O2
        C2_volume = loops * C2_per_loop
        
        Vt_total = C1_volume + C2_volume
        
        # === Affichage des résultats ===
        c1_op = f"Filter {'with' if is_sharded_C1 else 'without'} sharding"
        c2_op = f"Loop {'with' if is_sharded_C2 else 'without'} sharding"

        print(f"\n--- Join cost ---")
        print(f"Collection 1: {coll1_name} (filter on {coll1_filter_key}, sharding on {coll1_sharding_key})")
        print(f"Collection 2: {coll2_name} (join on {coll2_join_key}, sharding on {coll2_sharding_key})")
        
        print(f"\n[C1] {c1_op}")
        print(f"  Formula: C1 = #S1 × size_S1 + #O1 × size_O1")
        print(f"          C1 = {S1} × {size_S1} + {num_O1} × {size_O1}")
        print(f"          C1 = {C1_volume:,} B")
        print(f"  → Loops (O1) = {loops:,}")
        
        print(f"\n[C2] {c2_op} (×{loops:,} loops)")
        print(f"  Formula per loop: C2 = #S2 × size_S2 + #O2 × size_O2")
        print(f"                   C2 = {S2} × {size_S2} + {num_O2} × {size_O2}")
        print(f"  C2 per loop = {C2_per_loop:,} B")
        print(f"  C2 total = {loops:,} × {C2_per_loop:,} = {C2_volume:,} B")
        
        print(f"\n[Vt] Formula: Vt = C1 + loops × C2")
        print(f"            Vt = {C1_volume:,} + {C2_volume:,}")
        print(f"            Vt = {Vt_total:,} B ({Vt_total / (1024**2):.2f} MB)")
        
        return {
            "query_type": "Join",
            "Vt_total": Vt_total,
            "C1_volume": C1_volume,
            "C2_volume": C2_volume,
            "C1_sharding_strategy": c1_op,
            "C2_sharding_strategy": c2_op,
            "Loops": loops,
            "details_C1": {
                "S1": S1,
                "size_S1": size_S1,
                "num_O1": num_O1,
                "size_O1": size_O1,
                "volume": C1_volume,
                "is_sharded": is_sharded_C1,
                "loops": loops
            },
            "details_C2": {
                "S2": S2,
                "size_S2": size_S2,
                "num_O2": num_O2,
                "size_O2": size_O2,
                "volume": C2_volume,
                "is_sharded": is_sharded_C2
            }
        }

    
    def resolve_query_strategy(self, entry_coll_name: str, entry_filter_key: str, 
                               target_coll_name: str, sharding_config: Dict,
                               sql_query: str) -> Dict:
        """
        Determines if the query is solved as a simple FILTER or requires a JOIN
        based on the current denormalization schema (DB1/DB2/DB3).
        
        Args:
            entry_coll_name: Collection d'entrée
            entry_filter_key: Clé de filtre
            target_coll_name: Collection cible (pour le join)
            sharding_config: Configuration du sharding
            sql_query: Requête SQL complète
        """
        
        # 1. Check for embedding in the entry collection (e.g., P in S -> DB3 for Q4)
        if target_coll_name in self.schema_map.get(entry_coll_name, []):
            print(f"\n[{self.current_schema}] Denormalization detected: {target_coll_name} EMBEDDED in {entry_coll_name}. No JOIN required.")
            
            return self.compute_filter_query_vt(
                collection_name=entry_coll_name,
                filter_key=entry_filter_key,
                collection_sharding_key=sharding_config.get(entry_coll_name, "N/A"),
                sql_query=sql_query
            )

        # 2. Check for embedding in the target collection (e.g., S in P -> DB2 for Q4)
        elif entry_coll_name in self.schema_map.get(target_coll_name, []):
            print(f"\n[{self.current_schema}] Denormalization detected: {entry_coll_name} EMBEDDED in {target_coll_name}. No JOIN required.")
            
            # Rewrite query as a filter on the HOST collection (target_coll_name)
            return self.compute_filter_query_vt(
                collection_name=target_coll_name,
                filter_key=entry_filter_key,      
                collection_sharding_key=sharding_config.get(target_coll_name, "N/A"),
                sql_query=sql_query
            )

        # 3. Default case: JOIN is required (DB1 or non-embedded model)
        else:
            print(f"\n[{self.current_schema}] Normalized Model (DB1) or non-embedded configuration: JOIN required.")
            
            return self.compute_join_query_vt(
                coll1_name=entry_coll_name, 
                coll1_filter_key=entry_filter_key, 
                coll1_sharding_key=sharding_config.get(entry_coll_name, "N/A"),
                coll2_name=target_coll_name, 
                coll2_join_key=sharding_config.get(target_coll_name, "N/A"), 
                coll2_sharding_key=sharding_config.get(target_coll_name, "N/A"),
                sql_query=sql_query
            )

        
    def extract_query_context(self, query: str, collection_name: str) -> Dict[str, List[str]]:
        """
        Extracts the fields used in SELECT, WHERE, and JOIN for a given collection.

        Args:
        query: Complete SQL query
        collection_name: Name of the collection (to filter aliases)

        Returns:
        Dict with 'select', 'where', 'join' containing the lists of fields
        """
        context = {'select': [], 'where': [], 'join': []}
        
        # Clean the query
        query = query.strip().replace('\n', ' ')
        query = re.sub(r'\s+', ' ', query)
        
        # Find the alias of the collection
        # Ex : "FROM Products P" -> alias = "P"
        alias_pattern = rf'\b(?:FROM|JOIN)\s+{collection_name}\s+(\w+)'
        alias_match = re.search(alias_pattern, query, re.IGNORECASE)
        alias = alias_match.group(1) if alias_match else collection_name[0]
        
        # 1. Extract fields from SELECT
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, query, re.IGNORECASE)
        if select_match:
            select_clause = select_match.group(1)
            # Ex: "P.name, P.price" -> ['name', 'price']
            field_pattern = rf'{alias}\.(\w+)'
            context['select'] = re.findall(field_pattern, select_clause)
        
        # 2. Extract fields from WHERE
        where_pattern = r'WHERE\s+(.*?)(?:;|$)'
        where_match = re.search(where_pattern, query, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1)
            field_pattern = rf'{alias}\.(\w+)'
            context['where'] = re.findall(field_pattern, where_clause)
        
        # 3. Extract fields from JOIN (ON clause)
        join_pattern = rf'ON\s+(?:{alias}\.(\w+)\s*=\s*\w+\.\w+|\w+\.\w+\s*=\s*{alias}\.(\w+))'
        join_matches = re.finditer(join_pattern, query, re.IGNORECASE)
        for match in join_matches:
            field = match.group(1) if match.group(1) else match.group(2)
            if field:
                context['join'].append(field)
        
        return context

   
    def analyze_schema_fields(self, collection_name: str, 
                             field_list: Optional[List[str]] = None,
                             query: Optional[str] = None) -> Dict:
        """    
        Analyzes a schema to count the types of first-level scalar fields.
                
                Args:
                    collection_name: Name of the collection to analyze
                    field_list: List of fields to include (for simple projection)
                    query: Complete SQL query (for automatic context extraction)
                
                Returns:
                    Dict containing counts by field type
        """
        if collection_name not in self.collections:
            return {'num_int': 0, 'num_string': 0, 'num_date': 0, 
                    'num_longstring': 0, 'num_keys': 0}

        schema = self.collections[collection_name]['schema']
        all_fields = {}

        # 1. Extracting first-level scalar fields with their type
        for field_name, field_schema in schema.get("properties", {}).items():
            node_type = field_schema.get("type")
            if node_type in ["integer", "number"]:
                all_fields[field_name] = "int"
            elif node_type == "string":
                if field_name in ["description", "comment"]:
                    all_fields[field_name] = "longstring"
                else:
                    all_fields[field_name] = "string"
            elif node_type == "date":
                all_fields[field_name] = "date"

        # 2. Determine which fields to count based on the context
        fields_to_count = set()
        context_info = ""
        
        if query:
            # Automatic extraction of context from the query
            context = self.extract_query_context(query, collection_name)
            for clause_fields in context.values():
                if clause_fields:
                    fields_to_count.update(clause_fields)
            context_info = f" | Context: {context}"
        elif field_list:
            # Simple projection or explicit list
            fields_to_count = set(field_list)
            context_info = f" | Fields: {field_list}"
        else:
            # Complete document
            fields_to_count = set(all_fields.keys())
            context_info = " | Full document"

        # 3. Counting scalars for selected fields
        num_int, num_string, num_date, num_longstring = 0, 0, 0, 0
        fields_counted = []

        for field_name in fields_to_count:
            field_type = all_fields.get(field_name)
            if field_type == "int":
                num_int += 1
            elif field_type == "string":
                num_string += 1
            elif field_type == "date":
                num_date += 1
            elif field_type == "longstring":
                num_longstring += 1
            
            if field_type:
                fields_counted.append(f"{field_name} ({field_type})")

        # 4. Calculating the number of keys
        if query or field_list:
            # For projection/query: #keys = #fields counted
            num_keys = len(fields_counted)
        else:
            # For complete document: #keys = #fields + 1 (_id)
            num_keys = len(all_fields) + 1
            
            # Specific adjustment for St (DB1)
            if collection_name == "St":
                num_keys = len(all_fields)

        # 5. Log pour debugging
        print(f"  [ANALYSIS] {collection_name} (Fields: {len(fields_counted)}){context_info}:")
        print(f"    - Champs comptés: {', '.join(fields_counted) if fields_counted else 'none'}")
        print(f"    - Counts: I:{num_int}, S:{num_string}, D:{num_date}, L:{num_longstring}, K:{num_keys}")
            
        return {
            'num_int': num_int, 
            'num_string': num_string, 
            'num_date': num_date, 
            'num_longstring': num_longstring, 
            'num_keys': num_keys
        }

    def compute_size_from_counts(self, counts: Dict) -> int:
        """
        Calculates the size in bytes from the scalar field and key counters.

        FORMULA FROM TD: (int*8) + (string*80) + (date*20) + (longstring*200) + (keys*12)
        """
        return (
            counts.get('num_int', 0) * self.SIZE_NUMBER + 
            counts.get('num_string', 0) * self.SIZE_STRING + 
            counts.get('num_date', 0) * self.SIZE_DATE + 
            counts.get('num_longstring', 0) * self.SIZE_LONG_STRING + 
            counts.get('num_keys', 0) * self.SIZE_KEY_VALUE 
        )
  
    def get_query_stats(self, collection_name: str, query_key: str, phase: str, 
                       sql_query: str) -> Tuple[int, int, int]:
        """
        Returns (#OutputDocs, size_S, size_O) by READING the schemas.
        
        Args:
            collection_name: Name of the collection to analyze
            query_key: Query key (e.g., "IDP_IDW," "brand," etc.)
            phase: Query phase ("C1" or "C2")
            sql_query: Complete SQL query (provided by QUERIES[query_name])

        Returns:
            Tuple (num_output_docs, size_S, size_O)
        """
        
        print(f"\n[GET_QUERY_STATS] {collection_name} | {query_key} | {phase}")
        print(f"  SQL Query: {sql_query}")
        
        # ====================================================================
        # PARTIE 1: #O (nb of documents)
        # ====================================================================
        print("QUERYYYYY"+str(query_key))
        if phase == "C1":
            group_match = re.search(r"GROUP BY\s+(?:\w+\.)?(\w+)", sql_query, re.IGNORECASE)
            group_key_sql = group_match.group(1) if group_match else None
            
            if group_key_sql:
                print("  → Computing #O (GROUP BY detected):"+group_key_sql)
                # Mapping de la clé SQL (ex: IDP) vers le type interne (ex: Prod)
                clean_key = group_key_sql.lower().replace("id", "")
                id_map = {"p": "Prod", "c": "Cl", "w": "Wa", "s": "Supp", "st": "St", "ol": "OL"}
                target_type = id_map.get(clean_key, self.array_to_collection.get(clean_key, "Prod"))

                # 2. Vérification de la présence d'un filtre WHERE
                where_match = re.search(r"WHERE\s+(?:\w+\.)?(\w+)\s*=", sql_query, re.IGNORECASE)
                
                if where_match:
                    print("  → Computing #O (WITH filter detected):"+where_match.group(1))
                    # CAS AVEC FILTRE (ex: Q7 - Group by IDP pour UN client)
                    # On cherche la relation : ex: Nb de Prod par Client (avg_length['Cl']['Prod'])
                    filter_field = where_match.group(1).lower().replace("id", "")
                    filter_source = id_map.get(filter_field, "Cl")
                    
                    num_output_docs = self.avg_length.get(filter_source, {}).get(target_type, 1)
                    print(f"  -> Aggregation with filter: avg {target_type} per {filter_source} = {num_output_docs}")
                else:
                    print("  → Computing #O (NO filter detected):")
                    # CAS SANS FILTRE (ex: Q6 - Group by IDP sur toute la table)
                    # Le nombre de groupes est le nombre total d'entités distinctes
                    num_output_docs = self.nb_docs.get(target_type, 1)
                    print(f"  -> Full Aggregation: total {target_type} = {num_output_docs}")
            elif query_key == "IDP_IDW": 
                num_output_docs = 1
            elif query_key == "brand": 
                num_output_docs = self.statistics.get('apple_products', 50)
            elif query_key == "date": 
                num_output_docs = int(self.nb_docs["OL"] / self.statistics.get('dates_per_year', 365))
            elif query_key == "IDW": 
                num_output_docs = self.nb_docs["Prod"]
            elif query_key == "IDP": 
                num_output_docs = 1
            elif query_key == "IDC": 
                num_output_docs = int(self.nb_docs["OL"] / self.nb_docs["Cl"])
            else: 
                num_output_docs = 1
        elif phase == "C2":
            if collection_name == "Prod" and query_key == "IDP": 
                num_output_docs = 1
            elif collection_name == "St" and query_key == "IDP": 
                num_output_docs = self.nb_docs["Wa"]
            else: 
                num_output_docs = 1
        else: 
            num_output_docs = 1
        
        # ====================================================================
        # PARTIE 2: size_S (size of the query with WHERE + JOIN)
        # ====================================================================
        
        print(f"\n  → Computing size_S :")
        counts_s = self.analyze_schema_fields(collection_name, query=sql_query)
        size_S = self.compute_size_from_counts(counts_s)
        
        # ====================================================================
        # PARTIE 3: size_O (PROJECTION size - SELECT only)
        # ====================================================================
        
        # Créer une version de la requête sans WHERE pour la projection
        query_projection = self._create_projection_query(sql_query, collection_name, remove_join=False)
        
        print(f"\n  → Computing size_O (projection - SELECT only):")
        print(f"    Projection query: {query_projection}")
        counts_o = self.analyze_schema_fields(collection_name, query=query_projection)
        size_O = self.compute_size_from_counts(counts_o)
        
        # ====================================================================
        # Special cases
        # ====================================================================
        
        # Cas spécial Q4 DB3 (Projection agrégée : name + quantity)
        if collection_name == "St" and query_key == "IDW" and self.current_schema == "DB3":
            print("\n  [SPECIAL CASE] Q4 DB3 - Aggregated projection (name + quantity)")
            counts_name = self.analyze_schema_fields("Prod", field_list=["name"]) #Analyze Product fields separately
            counts_qty = self.analyze_schema_fields("St", field_list=["quantity"]) #Analyze Stock fields separately
            
            counts_o = {
                'num_int': counts_qty['num_int'], 
                'num_string': counts_name['num_string'], 
                'num_date': 0, 
                'num_longstring': 0, 
                'num_keys': 2
            }
            size_O = self.compute_size_from_counts(counts_o)
        
        print(f"\n  ✓ Results: #O={num_output_docs}, size_S={size_S}B, size_O={size_O}B")
        
        return (num_output_docs, size_S, size_O)
    
    def _create_projection_query(self, sql_query: str, collection_name: str, 
                                 remove_join: bool = False) -> str:
        """
        Creates a projection query by removing WHERE (and optionally JOIN).
        
        Args:
            sql_query: Complete SQL query
            collection_name: Name of the collection
            remove_join: If True, also removes the JOIN clause (for size_O in phase C2)

        Returns:
            SQL query without WHERE clause (and without JOIN if remove_join=True)
        """
        # Remove the WHERE clause
        query_without_where = re.sub(r'\s+WHERE\s+.*?(;|$)', r'\1', sql_query, flags=re.IGNORECASE)
        
        if remove_join:
            # Also remove the JOIN clause (for C2 projection)
            query_without_join = re.sub(r'\s+JOIN\s+.*?ON\s+.*?(?=WHERE|;|$)', '', query_without_where, flags=re.IGNORECASE)
            return query_without_join.strip()
        
        return query_without_where.strip()


    def compute_aggregate_query_vt(self, entry_coll_name: str, group_key: str,
                                   sharding_config: Dict,
                                   filter_key: Optional[str] = None,
                                   limit: Optional[int] = None,
                                   target_coll_name: Optional[str] = None,
                                   sql_query: Optional[str] = None) -> Dict:
        """
        Computes the Vt cost for aggregate queries with GROUP BY.
        
        The formula is: Vt = C1 + SHUFFLE + C2 (+ optional C3 for final JOIN)
        
        Args:
            entry_coll_name: Collection to query (e.g., "OL")
            group_key: Key used in GROUP BY clause (e.g., "IDP")
            sharding_config: Dict with sharding keys for all collections
            filter_key: Optional key used in WHERE filter (e.g., "IDC")
            limit: Optional LIMIT value (affects C2 loops)
            target_coll_name: Optional target collection for final JOIN (e.g., "Prod")
            sql_query: Optional complete SQL query (will be extracted if not provided)
        
        Returns:
            Dict containing cost details for all phases
        """
        collection_sharding_key = sharding_config.get(entry_coll_name, "N/A")
        
        print(f"\n{'='*80}")
        print(f"AGGREGATE QUERY ANALYSIS")
        print(f"{'='*80}")
        print(f"Collection: {entry_coll_name}")
        print(f"Filter: {filter_key or 'None (Full scan)'}")
        print(f"Group By: {group_key}")
        print(f"Sharding: {collection_sharding_key}")
        print(f"Aggregation: SUM(quantity)")
        if limit:
            print(f"Limitttt: {limit}")
        if target_coll_name:
            print(f"Final JOIN with: {target_coll_name}")
        
        # ====================================================================
        # PHASE C1: Initial Filter/Scan
        # ====================================================================
        print(f"\n{'─'*80}")
        print("[PHASE C1] Initial Filter/Scan")
        print(f"{'─'*80}")
        
        # Determine sharding for C1
        if filter_key:
            is_sharded_C1 = (filter_key == collection_sharding_key)
            S1 = 1 if is_sharded_C1 else self.num_shards
        else:
            # No filter = full scan across all shards
            is_sharded_C1 = False
            S1 = self.num_shards
        
        # Use get_query_stats to determine num_O1, size_S1, size_O1
        # Extract the subquery from the real SQL if provided
        if sql_query:
            # Extract subquery for aggregation (the part inside parentheses)
            subquery_match = re.search(r'\(\s*(SELECT.*?)\s*\)\s+(?:AS\s+)?\w+\s+ON', sql_query, re.IGNORECASE | re.DOTALL)
            if subquery_match:
                c1_sql_query = subquery_match.group(1).strip()
                print(f"  [EXTRACTED SUBQUERY]: {c1_sql_query}")
            else:
                # No subquery found, use the original query
                c1_sql_query = sql_query
        else:
            # Build a simplified SQL query for aggregate (fallback)
            c1_sql_query = f"SELECT {group_key}, SUM(quantity) FROM {entry_coll_name}"
            if filter_key:
                c1_sql_query += f" WHERE {filter_key} = $value"
            c1_sql_query += f" GROUP BY {group_key}"
            if limit:
                c1_sql_query += f" LIMIT {limit}"
            c1_sql_query += ";"
        
        num_O1, size_S1, size_O1 = self.get_query_stats(
            entry_coll_name,
            filter_key,
            "C1",
            c1_sql_query
        )
        print(f"\n  → C1 FILTERRRR: {filter_key or 'None (Full scan)'}")
        needs_shuffle = False
        if group_key and group_key != collection_sharding_key:
            # Si on filtre sur la clé de sharding avec un égalité (ex: WHERE IDC = 125)
            # Alors toutes les données sont sur 1 serveur, pas de shuffle réseau nécessaire.
            if filter_key == collection_sharding_key and " = " in sql_query:
                needs_shuffle = False
            else:
                needs_shuffle = True
        
        
        if needs_shuffle:
            # SHUFFLE is needed: data must be redistributed
            # shuffle1 = number of documents that need redistribution
            shuffle1 = (num_O1 -1)*S1
            size_shuffle1 = size_O1
        else:
            # NO SHUFFLE needed: data is already partitioned correctly
            shuffle1 = 0
            size_shuffle1 = 0
        
        # C1 = S1 * size_S1 + shuffle1 * size_shuffle1 + O1 * size_O1
        C1_volume = S1 * size_S1 + shuffle1 * size_shuffle1 + num_O1 * size_O1
        
        print(f"\nC1 Formula: C1 = S1 × size_S1 + shuffle1 × size_shuffle1 + O1 × size_O1")
        print(f"           C1 = {S1} × {size_S1} + {shuffle1:,} × {size_shuffle1} + {num_O1:,} × {size_O1}")
        print(f"           C1 = {C1_volume:,} B")
        print(f"\nDetails:")
        print(f"  • Sharding strategy: {'WITH sharding' if is_sharded_C1 else 'WITHOUT sharding (broadcast/full scan)'}")
        print(f"  • #S1 (servers contacted) = {S1}")
        print(f"  • #O1 (documents to process) = {num_O1:,}")
        print(f"  • size_O1 (document size) = {size_O1} B")
        
        if needs_shuffle:
            print(f"\n  ⚠️  SHUFFLE REQUIRED!")
            print(f"      Reason: Group By key ({group_key}) ≠ Sharding key ({collection_sharding_key})")
            print(f"      • shuffle1 (documents to redistribute) = {shuffle1:,}")
            print(f"      • size_shuffle1 (shuffle data size) = {size_shuffle1} B")
            print(f"      • Shuffle cost = {shuffle1 * size_shuffle1:,} B")
            print(f"      • This requires transferring all filtered data across the network")
        else:
            print(f"\n  ✓ NO SHUFFLE NEEDED!")
            print(f"      Reason: Group By key ({group_key}) = Sharding key ({collection_sharding_key})")
            print(f"      • Data is already partitioned by {group_key}")
            print(f"      • Each server can perform local aggregation without network transfer")
        
        # ====================================================================
        # PHASE C2: Aggregation and Result Collection
        # ====================================================================
        print(f"\n{'─'*80}")
        print("[PHASE C2] Aggregation & Result Collection")
        print(f"{'─'*80}")
        
        # Number of groups after GROUP BY
        # This depends on the cardinality of the group_key
        # For aggregations, num_groups represents the maximum possible distinct values
        if group_key == "IDP":
            # Maximum: number of distinct products
            max_possible_groups = self.nb_docs["Prod"]
        elif group_key == "IDC":
            max_possible_groups = self.nb_docs["Cl"]
        elif group_key == "IDW":
            max_possible_groups = self.nb_docs["Wa"]
        elif group_key == "date":
            max_possible_groups = self.statistics.get('dates_per_year', 365)
        else:
            # Default: assume high cardinality
            max_possible_groups = 1000
        
        # Actual groups: limited by data processed OR by cardinality
        num_groups = min(num_O1, max_possible_groups)
        
        # Apply LIMIT if specified
        # num_O2 = number of groups we actually return (after LIMIT and ORDER BY)
        # When LIMIT is specified, we return AT MOST limit groups, regardless of num_groups
        if limit:
            # If limit is specified, we assume we can get up to 'limit' groups
            # (even if num_groups calculation suggests fewer, because that might be wrong)
            num_O2 = min(limit, max_possible_groups)
        else:
            num_O2 = num_groups
        
        # Size of aggregated result: group_key + aggregated_field + keys
        # For example: IDP (int) + SUM(quantity) (int) + 2 keys = 8 + 8 + 24 = 40 B
        size_O2 = self.SIZE_NUMBER * 2 + self.SIZE_KEY_VALUE * 2
        
        # C2: Collecting results from servers
        target_sharding_key = sharding_config.get(target_coll_name, "N/A")
        if target_coll_name:
            # On vérifie si on joint sur la clé de partitionnement de la table cible
            is_join_on_sharding_key = (group_key == target_sharding_key)
            S2 = 1 if is_join_on_sharding_key else self.num_shards
        else:
            S2 = self.num_shards if needs_shuffle else S1
                
        # shuffle2 represents communication between servers for collecting aggregated results
        # When results are distributed across servers, we need to collect them
        # Si les deux tables sont shardées sur la même clé, elles sont colocalisées
        if target_coll_name:
            shuffle2 = 0
            size_shuffle2 = 0
        else:
            shuffle2 = num_O2 if needs_shuffle else 0
            size_shuffle2 = size_O2 if needs_shuffle else 0
        
        # C2 = S2 * size_S2 + shuffle2 * size_shuffle2 + O2 * size_O2
        size_S2 = size_O2
        C2_volume = S2 * size_S2 + shuffle2 * size_shuffle2 + num_O2 * size_O2
        
        print(f"\nC2 Formula: C2 = S2 × size_S2 + shuffle2 × size_shuffle2 + O2 × size_O2")
        print(f"           C2 = {S2} × {size_S2} + {shuffle2:,} × {size_shuffle2} + {num_O2:,} × {size_O2}")
        print(f"           C2 = {C2_volume:,} B")
        print(f"\nDetails:")
        print(f"  • #S2 (servers with results) = {S2}")
        print(f"  • #O2 (groups to collect) = {num_O2:,}")
        if limit:
            print(f"    (Limited from {num_groups:,} total groups)")
        print(f"  • size_O2 (aggregated result size) = {size_O2} B")
        print(f"    ({group_key} + SUM(quantity) + keys)")
        if needs_shuffle:
            print(f"  • shuffle2 (communication between servers) = {shuffle2:,}")
            print(f"  • Shuffle cost for C2 = {shuffle2 * size_shuffle2:,} B")
        
        # ====================================================================
        # PHASE C3: Optional JOIN with target collection
        # ====================================================================
        C3_volume = 0
        if target_coll_name:
            print(f"\n{'─'*80}")
            print(f"[PHASE C3] Final JOIN with {target_coll_name}")
            print(f"{'─'*80}")
            
            target_sharding_key = sharding_config.get(target_coll_name, "N/A")
            
            # For each aggregated result, we need to fetch the corresponding document
            # from the target collection
            is_sharded_C3 = (group_key == target_sharding_key)
            S3 = 1 if is_sharded_C3 else self.num_shards
            
            # Get target document size
            if target_coll_name in self.computed_sizes:
                size_target = self.computed_sizes[target_coll_name]['doc_size']
            else:
                size_target = 980  # Default for Prod
            
            # For each of the num_O2 results, we do a lookup
            C3_volume = num_O2 * (S3 * size_O2 + size_target)
            
            print(f"\nC3 Formula: C3 = #O2 × (#S3 × size_query + size_target)")
            print(f"           C3 = {num_O2:,} × ({S3} × {size_O2} + {size_target})")
            print(f"           C3 = {C3_volume:,} B")
            print(f"\nDetails:")
            print(f"  • Lookup strategy: {'WITH sharding' if is_sharded_C3 else 'WITHOUT sharding (broadcast)'}")
            print(f"  • #S3 (servers per lookup) = {S3}")
            print(f"  • Loops = {num_O2:,}")
        
        # ====================================================================
        # TOTAL COST
        # ====================================================================
        # Formula: Vcom = C1 + loops*C2
        # - For queries without JOIN: loops = 1
        # - For queries with JOIN (C3): loops = num_O2 (number of groups returned)
        
        if target_coll_name:
            # C3 represents lookups for each group result
            # loops = number of groups we lookup (which is num_O2)
            loops = num_O2
            Vcom_total = C1_volume + C3_volume
        else:
            # Simple aggregation: loops = 1
            loops = 1
            Vcom_total = C1_volume + loops * C2_volume
        
        print(f"\n{'='*80}")
        print("[TOTAL COST]")
        print(f"{'='*80}")
        
        if target_coll_name:
            print(f"\nFormula: Vcom = C1 + loops*C2")
            print(f"              = C1 + C3  (where C3 = {loops:,} lookups)")
            print(f"        Vcom = {C1_volume:,} + {C3_volume:,}")
        else:
            print(f"\nFormula: Vcom = C1 + loops*C2")
            print(f"        Vcom = {C1_volume:,} + {loops} × {C2_volume:,}")
        
        print(f"        Vcom = {Vcom_total:,} B ({Vcom_total / (1024**2):.2f} MB)")
        
        print(f"\nCost Breakdown:")
        print(f"  • C1 (Filter + Shuffle):  {C1_volume:>15,} B ({C1_volume/Vcom_total*100:>5.1f}%)")
        if target_coll_name:
            print(f"  • C3 (loops={loops:,} × C2):  {C3_volume:>15,} B ({C3_volume/Vcom_total*100:>5.1f}%)")
        else:
            print(f"  • C2 (loops={loops} × Aggregate):  {C2_volume:>15,} B ({C2_volume/Vcom_total*100:>5.1f}%)")
        print(f"  {'─'*50}")
        print(f"  • TOTAL:                   {Vcom_total:>15,} B")
        
        return {
            "query_type": "Aggregate",
            "Vt_total": Vcom_total,
            "C1_volume": C1_volume,
            "shuffle1_volume": shuffle1 * size_shuffle1,
            "C2_volume": C2_volume,
            "shuffle2_volume": shuffle2 * size_shuffle2,
            "C3_volume": C3_volume,
            "needs_shuffle": needs_shuffle,
            "Loops": loops,
            "C1_sharding_strategy": f"{'Filter with' if is_sharded_C1 else 'Full scan without'} sharding",
            "C2_sharding_strategy": f"Aggregate {'without' if needs_shuffle else 'with'} shuffle",
            "details_C1": {
                "S1": S1,
                "size_S1": size_S1,
                "num_O1": num_O1,
                "size_O1": size_O1,
                "shuffle1": shuffle1,
                "size_shuffle1": size_shuffle1,
                "volume": C1_volume,
                "is_sharded": is_sharded_C1
            },
            "details_SHUFFLE": {
                "needed": needs_shuffle,
                "shuffle1_volume": shuffle1 * size_shuffle1,
                "shuffle2_volume": shuffle2 * size_shuffle2,
                "reason": f"Group By ({group_key}) {'!=' if needs_shuffle else '=='} Sharding ({collection_sharding_key})"
            },
            "details_C2": {
                "S2": S2,
                "num_groups": num_groups,
                "num_O2": num_O2,
                "size_O2": size_O2,
                "volume": C2_volume
            }
        }
    

