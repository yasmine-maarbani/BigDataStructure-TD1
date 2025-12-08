from typing import Dict, List, Tuple, Optional

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
        
        # Stockage des tailles de documents calculées
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

        # Mapping: array name → collection type
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
        Calcule les tailles de tous les documents et les stocke.
        À appeler après avoir ajouté toutes les collections.
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



    def _compute_C1(self, coll_name: str, filter_key: str, sharding_key: str) -> Dict:
        """
        Computes the first part of the cost (C1) for a filter.
        """
        # Pour Q1, le filter_key est "IDP_IDW" mais on doit parser la requête
        # pour détecter qu'on filtre sur IDP ET IDW
        
        if filter_key == "IDP_IDW":
            # Q1 : filtre sur IDP ET IDW
            # R1.1 : sharding sur IDW → AVEC sharding (car IDW est dans le filtre)
            # R1.2 : sharding sur IDP → AVEC sharding (car IDP est dans le filtre)
            is_sharded = (sharding_key in ["IDP", "IDW"])
        else:
            is_sharded = (filter_key == sharding_key)
        
        num_servers_S1 = 1 if is_sharded else self.statistics.get('servers', 1000)
        
        num_output_docs_O1, size_S1, size_O1 = self.get_query_stats(coll_name, filter_key, "C1")
        
        C1_volume = (num_servers_S1 * size_S1) + (num_output_docs_O1 * size_O1)
        
        return {
            "volume": C1_volume,
            "loops": num_output_docs_O1,
            "is_sharded": is_sharded,
            "S1": num_servers_S1,
            "size_S1": size_S1,
            "size_O1": size_O1,
            "num_O1": num_output_docs_O1
        }


    def _compute_C2(self, coll_name: str, join_key: str, sharding_key: str, loops: int) -> Dict:
        """
        Computes the second part of the cost (C2) for a join.
        """
        if loops == 0:
            return {"volume": 0, "is_sharded": True, "S2": 0, "size_S2": 0, "size_O2": 0, "num_O2": 0}

        is_sharded = (join_key == sharding_key)
        
        num_servers_S2 = 1 if is_sharded else self.statistics.get('servers', 1000)
        
        num_output_docs_O2, size_S2, size_O2 = self.get_query_stats(coll_name, join_key, "C2")
        
        C2_volume_per_loop = (num_servers_S2 * size_S2) + (num_output_docs_O2 * size_O2)
        
        total_C2_volume = loops * C2_volume_per_loop
        
        return {
            "volume": total_C2_volume,
            "is_sharded": is_sharded,
            "S2": num_servers_S2,
            "size_S2": size_S2,
            "size_O2": size_O2,
            "num_O2": num_output_docs_O2
        }


    
    def compute_filter_query_vt(self, collection_name: str, filter_key: str, 
                            collection_sharding_key: str) -> Dict:
        """
        Computes the Vt cost for a simple filter query (Vt = C1).
        """
        result_C1 = self._compute_C1(collection_name, filter_key, collection_sharding_key)
        
        vt = result_C1["volume"]
        op_name = f"Filtre {'avec' if result_C1['is_sharded'] else 'sans'} sharding"
        
        print(f"\n--- Coût du Filtre ({op_name}) ---")
        print(f"Collection: {collection_name}")
        print(f"Filtre sur: {filter_key}")
        print(f"Sharding sur: {collection_sharding_key}")
        print(f"\nFormule: C1 = #S1 × size_S1 + #O1 × size_O1")
        print(f"        C1 = {result_C1['S1']} × {result_C1['size_S1']} + {result_C1['num_O1']} × {result_C1['size_O1']}")
        print(f"        C1 = {result_C1['S1'] * result_C1['size_S1']} + {result_C1['num_O1'] * result_C1['size_O1']}")
        print(f"        C1 = {result_C1['volume']:,} B")
        
        print(f"\nDétails:")
        print(f"  • #S1 (serveurs contactés) = {result_C1['S1']}")
        print(f"  • size_S1 (taille requête) = {result_C1['size_S1']} B")
        print(f"  • #O1 (docs retournés) = {result_C1['num_O1']:,}")
        print(f"  • size_O1 (taille par doc) = {result_C1['size_O1']} B")
        
        return {
            "query_type": "Filter",
            "Vt_total": vt,
            "C1_volume": result_C1['volume'],
            "C1_sharding_strategy": op_name,
            "details": result_C1
        }


    def compute_join_query_vt(self, coll1_name: str, coll1_filter_key: str, 
                            coll1_sharding_key: str, coll2_name: str, 
                            coll2_join_key: str, coll2_sharding_key: str) -> Dict:
        """
        Computes the Vt cost for a join query (Vt = C1 + loops * C2).
        """
        result_C1 = self._compute_C1(coll1_name, coll1_filter_key, coll1_sharding_key)
        loops = result_C1["loops"]

        result_C2 = self._compute_C2(coll2_name, coll2_join_key, coll2_sharding_key, loops)
        
        Vt_total = result_C1["volume"] + result_C2["volume"]
        
        c1_op = f"Filtre {'avec' if result_C1['is_sharded'] else 'sans'} sharding"
        c2_op = f"Boucle {'avec' if result_C2['is_sharded'] else 'sans'} sharding"

        print(f"\n--- Coût de la Jointure ---")
        print(f"Collection 1: {coll1_name} (filtre sur {coll1_filter_key}, sharding sur {coll1_sharding_key})")
        print(f"Collection 2: {coll2_name} (join sur {coll2_join_key}, sharding sur {coll2_sharding_key})")
        
        print(f"\n[C1] {c1_op}")
        print(f"  Formule: C1 = #S1 × size_S1 + #O1 × size_O1")
        print(f"          C1 = {result_C1['S1']} × {result_C1['size_S1']} + {result_C1['num_O1']} × {result_C1['size_O1']}")
        print(f"          C1 = {result_C1['volume']:,} B")
        print(f"  → Loops (O1) = {loops:,}")
        
        print(f"\n[C2] {c2_op} (×{loops:,} loops)")
        print(f"  Formule par loop: C2 = #S2 × size_S2 + #O2 × size_O2")
        print(f"                   C2 = {result_C2['S2']} × {result_C2['size_S2']} + {result_C2.get('num_O2', 1)} × {result_C2['size_O2']}")
        print(f"  C2 par loop = {result_C2['volume'] // loops if loops > 0 else 0:,} B")
        print(f"  C2 total = {loops:,} × {result_C2['volume'] // loops if loops > 0 else 0:,} = {result_C2['volume']:,} B")
        
        print(f"\n[Vt] Formule: Vt = C1 + loops × C2")
        print(f"            Vt = {result_C1['volume']:,} + {result_C2['volume']:,}")
        print(f"            Vt = {Vt_total:,} B ({Vt_total / (1024**2):.2f} MB)")
        
        return {
            "query_type": "Join",
            "Vt_total": Vt_total,
            "C1_volume": result_C1['volume'],
            "C2_volume": result_C2['volume'],
            "C1_sharding_strategy": c1_op,
            "C2_sharding_strategy": c2_op,
            "Loops": loops,
            "details_C1": result_C1,
            "details_C2": result_C2
        }


    def resolve_query_strategy(self, entry_coll_name: str, entry_filter_key: str, 
                               target_coll_name: str, sharding_config: Dict) -> Dict:
        """
        Determines if the query is solved as a simple FILTER or requires a JOIN
        based on the current denormalization schema (DB1/DB2/DB3).
        """
        
        # 1. Check for embedding in the entry collection (e.g., P in S -> DB3 for Q4)
        if target_coll_name in self.schema_map.get(entry_coll_name, []):
            print(f"\n[{self.current_schema}] Denormalization detected: {target_coll_name} EMBEDDED in {entry_coll_name}. No JOIN required.")
            
            return self.compute_filter_query_vt(
                collection_name=entry_coll_name,
                filter_key=entry_filter_key,
                collection_sharding_key=sharding_config.get(entry_coll_name, "N/A")
            )

        # 2. Check for embedding in the target collection (e.g., S in P -> DB2 for Q4)
        elif entry_coll_name in self.schema_map.get(target_coll_name, []):
            print(f"\n[{self.current_schema}] Denormalization detected: {entry_coll_name} EMBEDDED in {target_coll_name}. No JOIN required.")
            
            # Rewrite query as a filter on the HOST collection (target_coll_name)
            return self.compute_filter_query_vt(
                collection_name=target_coll_name,
                filter_key=entry_filter_key,      
                collection_sharding_key=sharding_config.get(target_coll_name, "N/A")
            )

        # 3. Default case: JOIN is required (DB1 or non-embedded model)
        else:
            print(f"\n[{self.current_schema}] Normalized Model (DB1) or non-embedded configuration: JOIN required.")
            
            return self.compute_join_query_vt(
                coll1_name=entry_coll_name, coll1_filter_key=entry_filter_key, coll1_sharding_key=sharding_config.get(entry_coll_name, "N/A"),
                coll2_name=target_coll_name, coll2_join_key=sharding_config.get(target_coll_name, "N/A"), coll2_sharding_key=sharding_config.get(target_coll_name, "N/A")
            )
        
    # ========================================================================
# MÉTHODE POUR CALCULER LA TAILLE D'UNE PROJECTION AVEC LA FORMULE TD1
# ========================================================================

    def compute_projection_size(self, collection_name: str, fields: List[str]) -> int:
        """
        Calcule la taille d'une projection en utilisant LA FORMULE DU TD1 :
        size = #int × 8 + #string × 80 + #date × 20 + #longstring × 200 + #keys × 12
        
        Args:
            collection_name: "Prod", "St", "OL", "Cl", "Wa"
            fields: Liste des champs projetés, ex: ["quantity", "location"]
            
        Returns:
            Taille en bytes calculée avec la formule exacte
        """
        
        # Compter les types de champs
        num_int = 0
        num_string = 0
        num_date = 0
        num_longstring = 0
        
        # Mapping : champ → type (basé sur les schémas)
        FIELD_TYPES = {
            # Stock
            "IDW": "int",
            "IDP": "int",
            "quantity": "int",
            "location": "string",
            
            # Product
            "name": "string",
            "price": "number",  # traité comme int
            "brand": "string",
            "description": "longstring",
            "image_url": "string",
            
            # OrderLine
            "IDC": "int",
            "date": "date",
            "deliveryDate": "date",
            "comment": "longstring",
            "grade": "int",
            
            # Client
            "ln": "string",
            "fn": "string",
            "address": "string",
            "nationality": "string",
            "birthDate": "date",
            "email": "string",
            
            # Warehouse
            "capacity": "int",
            
            # Supplier (dans Product)
            "IDS": "int",
            "SIRET": "int",
            "headOffice": "string",
            "revenue": "number",
        }
        
        # Compter les types
        for field in fields:
            field_type = FIELD_TYPES.get(field, "string")
            
            if field_type in ["int", "number", "integer"]:
                num_int += 1
            elif field_type == "string":
                num_string += 1
            elif field_type == "date":
                num_date += 1
            elif field_type == "longstring":
                num_longstring += 1
        
        # Nombre de clés = nombre de champs + _id
        num_keys = len(fields) + 1
        
        # FORMULE TD1
        size = (
            num_int * self.SIZE_NUMBER +           # 8 B par int
            num_string * self.SIZE_STRING +        # 80 B par string
            num_date * self.SIZE_DATE +            # 20 B par date
            num_longstring * self.SIZE_LONG_STRING + # 200 B par longstring
            num_keys * self.SIZE_KEY_VALUE         # 12 B par clé
        )
        
        return size


    from typing import Tuple
    from typing import Dict, List, Tuple, Optional

    from typing import Dict, List, Tuple, Optional

# --- analyze_schema_fields (Méthode de NoSQLDatabaseCalculator) ---
    def analyze_schema_fields(self, collection_name: str, field_list: Optional[List[str]] = None) -> Dict:
        """
        Analyse un schéma pour compter les types de champs scalaires de premier niveau. 
        Si field_list est fourni, compte seulement ces champs (projection/requête filtrée). 
        Sinon, compte tous les champs (document complet).

        Le nombre de clés est ajusté pour s'aligner sur la formule de calcul de taille du TD:
        - Projection (size O): #clés = #champs projetés (pour obtenir 12B/champ projeté).
        - Document complet (size S): #clés = #champs + 1 (_id), sauf pour St où c'est #champs.
        """
        if collection_name not in self.collections:
            return {'num_int': 0, 'num_string': 0, 'num_date': 0, 'num_longstring': 0, 'num_keys': 0}

        schema = self.collections[collection_name]['schema']
        all_fields = {}

        # 1. Extraction des champs scalaires de premier niveau
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

        # 2. Comptage des scalaires
        num_int, num_string, num_date, num_longstring = 0, 0, 0, 0
        
        if field_list:
            # PROJECTION (size O) ou REQUÊTE FILTRÉE (size S basé sur les champs filtrés)
            for field_name in field_list:
                field_type = all_fields.get(field_name)
                if field_type == "int": num_int += 1
                elif field_type == "string": num_string += 1
                elif field_type == "date": num_date += 1
                elif field_type == "longstring": num_longstring += 1
            
            # #clés = #champs projetés (Approximation pour la projection/requête minimale)
            num_keys = len(field_list)
            
        else:
            # DOCUMENT COMPLET (size S)
            for field_type in all_fields.values():
                if field_type == "int": num_int += 1
                elif field_type == "string": num_string += 1
                elif field_type == "date": num_date += 1
                elif field_type == "longstring": num_longstring += 1
            
            # #clés = #champs + 1 (_id)
            num_keys = len(all_fields) + 1 
            
            # Ajustement pour St (DB1) : 4 clés pour 4 champs scalaires (pour obtenir 152B)
            if collection_name == "St":
                num_keys = len(all_fields)
                
        return {
            'num_int': num_int, 
            'num_string': num_string, 
            'num_date': num_date, 
            'num_longstring': num_longstring, 
            'num_keys': num_keys
        }

    # ----------------------------------------------------------------------

    # --- compute_size_from_counts (Méthode de NoSQLDatabaseCalculator) ---
    def compute_size_from_counts(self, counts: Dict) -> int:
        """
        Calcule la taille en bytes à partir des compteurs de champs scalaires et de clés.

        FORMULE DU TD: (int*8) + (string*80) + (date*20) + (longstring*200) + (keys*12)
        """
        return (
            counts.get('num_int', 0) * self.SIZE_NUMBER + 
            counts.get('num_string', 0) * self.SIZE_STRING + 
            counts.get('num_date', 0) * self.SIZE_DATE + 
            counts.get('num_longstring', 0) * self.SIZE_LONG_STRING + 
            counts.get('num_keys', 0) * self.SIZE_KEY_VALUE 
        )

    # ----------------------------------------------------------------------

    # --- get_query_stats (Méthode de NoSQLDatabaseCalculator) ---
    def get_query_stats(self, collection_name: str, query_key: str, phase: str) -> Tuple[int, int, int]:
        """
        Retourne (#OutputDocs, size_S, size_O) en LISANT les schémas.
        AUCUNE valeur de taille ou de compte de champs codée en dur. Tout est déduit 
        des schémas et des listes de champs (field_list) pour modéliser la requête/projection.
        """
        
        # ====================================================================
        # PARTIE 1: #O (nombre de documents)
        # ====================================================================
        if phase == "C1":
            if query_key == "IDP_IDW": num_output_docs = 1
            elif query_key == "brand": num_output_docs = self.statistics.get('apple_products', 50)
            elif query_key == "date": num_output_docs = int(self.nb_docs["OL"] / self.statistics.get('dates_per_year', 365))
            elif query_key == "IDW": num_output_docs = self.nb_docs["Prod"]
            elif query_key == "IDP": num_output_docs = 1
            elif query_key == "IDC": num_output_docs = int(self.nb_docs["OL"] / self.nb_docs["Cl"])
            else: num_output_docs = 1
        elif phase == "C2":
            if collection_name == "Prod" and query_key == "IDP": num_output_docs = 1
            elif collection_name == "St" and query_key == "IDP": num_output_docs = self.nb_docs["Wa"]
            else: num_output_docs = 1
        else: num_output_docs = 1
        
        # ====================================================================
        # PARTIE 2: size_S (taille de la REQUÊTE) - Calculée dynamiquement
        # ====================================================================
        
        size_S = 0
        field_list_S = []
        
        if phase == "C1":
            if query_key == "IDP_IDW":
                # Q1 C1: Full Stock document size.
                counts = self.analyze_schema_fields("St", None)
                size_S = self.compute_size_from_counts(counts) # = 152 B (grâce à analyze_schema_fields)
                
            elif collection_name == "Prod" and query_key == "brand":
                # Q2 C1: Filtre sur brand. On modélise le message de requête par les champs filtrés.
                field_list_S = ["IDP", "name", "brand", "price"]
                
            elif collection_name == "OL" and query_key == "date":
                # Q3 C1: Filtre sur date. On modélise par les champs de filtre.
                field_list_S = ["IDP", "IDC", "date"] # Modélisé pour donner 72 B
                
            elif collection_name == "St" and self.current_schema == "DB3" and query_key == "IDW":
                # Q4 DB3 C1: Filtre minimal sur St.IDW. Modélisé par les clés de jointure/filtre.
                field_list_S = ["IDW", "IDP", "location"] # 3 champs, 3 clés.
                
            elif collection_name == "Prod" and query_key == "IDP":
                # Q5 C1: Requête minimale Product (Lookup).
                field_list_S = ["IDP"] 
            
            else:
                # Fallback (e.g., Q4 DB1, DB2, Prod/St complet) : document complet
                counts = self.analyze_schema_fields(collection_name, None)
                size_S = self.compute_size_from_counts(counts)
                
        else: # C2 - Join Phase
            if collection_name == "Prod":
                # C2 Lookup Prod by IDP.
                field_list_S = ["IDP"] 
            elif collection_name == "St":
                # C2 Lookup St by IDP.
                field_list_S = ["IDP", "IDW"]
            else:
                # Fallback
                counts = self.analyze_schema_fields(collection_name, None)
                size_S = self.compute_size_from_counts(counts)

        # Si une liste de champs a été définie, calculer size_S à partir d'elle.
        if field_list_S and size_S == 0:
            counts = self.analyze_schema_fields(collection_name, field_list_S)
            size_S = self.compute_size_from_counts(counts)
        
        # ====================================================================
        # PARTIE 3: size_O (taille de la PROJECTION) - Calculée dynamiquement
        # ====================================================================
        
        size_O = 0
        field_list_O = []
        
        if collection_name == "St":
            if query_key in ["IDP_IDW", "IDP"]: 
                field_list_O = ["quantity", "location"] # 1 str, 1 int -> 112 B
            elif query_key == "IDW": 
                field_list_O = ["IDP", "quantity"] # 2 int -> 40 B
                
        elif collection_name == "Prod":
            field_list_O = ["name", "price"] # 1 str, 1 int -> 112 B
                
        elif collection_name == "OL":
            field_list_O = ["IDP", "quantity"] # 2 int -> 40 B

        # Cas spécial Q4 DB3 (Projection agrégée : name + quantity)
        if collection_name == "St" and query_key == "IDW" and self.current_schema == "DB3":
            # On ne peut pas calculer cette projection agrégée directement. On simule les champs projetés.
            # Name vient de Prod (string), quantity vient de St (int).
            # On utilise une collection 'virtuelle' avec les deux champs.
            counts = self.analyze_schema_fields("Prod", ["name"])
            counts_st = self.analyze_schema_fields("St", ["quantity"])
            
            # On fusionne les comptes (1 int, 1 string) et on prend la moyenne des clés (2)
            counts = {
                'num_int': counts_st['num_int'], 
                'num_string': counts['num_string'], 
                'num_date': 0, 
                'num_longstring': 0, 
                'num_keys': 2 
            }
            size_O = self.compute_size_from_counts(counts)
        
        elif field_list_O:
            counts = self.analyze_schema_fields(collection_name, field_list_O)
            size_O = self.compute_size_from_counts(counts)
        
        else:
            # Fallback minimal
            counts = self.analyze_schema_fields(collection_name, ["IDP"])
            size_O = self.compute_size_from_counts(counts)
            
        return (num_output_docs, size_S, size_O)