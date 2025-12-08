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
        self.num_shards = statistics.get('servers', 1000) # a vérifier
        
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

    def compute_filter_query_vt(self, collection_name: str, filter_key: str, 
                               collection_sharding_key: str,
                               sql_query: str) -> Dict:
        """
        Computes the Vt cost for a simple filter query (Vt = C1).
        
        Args:
            collection_name: Collection à interroger
            filter_key: Clé utilisée dans le filtre WHERE
            collection_sharding_key: Clé de sharding de la collection
            sql_query: Requête SQL complète
        
        Returns:
            Dict contenant les détails du coût
        """
        # Déterminer si la requête est shardée
        is_sharded = (filter_key == collection_sharding_key)
        
        # Calculer #S1 (nombre de shards contactés)
        S1 = 1 if is_sharded else self.num_shards
        
        # Appeler get_query_stats avec la requête SQL pour obtenir size_S et size_O
        num_O1, size_S1, size_O1 = self.get_query_stats(
            collection_name, 
            filter_key, 
            "C1",
            sql_query
        )
        
        # Calcul du volume C1
        C1_volume = S1 * size_S1 + num_O1 * size_O1
        
        # Nom de l'opération
        op_name = f"Filtre {'avec' if is_sharded else 'sans'} sharding"
        
        # Affichage des résultats
        print(f"\n--- Coût du Filtre ({op_name}) ---")
        print(f"Collection: {collection_name}")
        print(f"Filtre sur: {filter_key}")
        print(f"Sharding sur: {collection_sharding_key}")
        print(f"\nFormule: C1 = #S1 × size_S1 + #O1 × size_O1")
        print(f"        C1 = {S1} × {size_S1} + {num_O1} × {size_O1}")
        print(f"        C1 = {S1 * size_S1} + {num_O1 * size_O1}")
        print(f"        C1 = {C1_volume:,} B")
        
        print(f"\nDétails:")
        print(f"  • #S1 (serveurs contactés) = {S1}")
        print(f"  • size_S1 (taille requête) = {size_S1} B")
        print(f"  • #O1 (docs retournés) = {num_O1:,}")
        print(f"  • size_O1 (taille par doc) = {size_O1} B")
        
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
            coll1_name: Collection 1 (entrée)
            coll1_filter_key: Clé de filtre pour C1
            coll1_sharding_key: Clé de sharding de C1
            coll2_name: Collection 2 (cible du join)
            coll2_join_key: Clé de jointure pour C2
            coll2_sharding_key: Clé de sharding de C2
            sql_query: Requête SQL complète
        """
        # === C1 : Requête initiale sur collection 1 ===
        is_sharded_C1 = (coll1_filter_key == coll1_sharding_key)
        S1 = 1 if is_sharded_C1 else self.num_shards
        
        # Calculer size_S1, size_O1, et #O1 avec la requête SQL complète
        num_O1, size_S1, size_O1 = self.get_query_stats(
            coll1_name, 
            coll1_filter_key, 
            "C1",
            sql_query
        )
        
        C1_volume = S1 * size_S1 + num_O1 * size_O1
        loops = num_O1  # Nombre de loops = nombre de documents retournés par C1
        
        # === C2 : Requête de jointure sur collection 2 ===
        is_sharded_C2 = (coll2_join_key == coll2_sharding_key)
        S2 = 1 if is_sharded_C2 else self.num_shards
        
        # Pour C2, on calcule différemment size_S2 et size_O2
        print(f"\n  → Computing C2 sizes:")
        
        # size_S2: requête de lookup avec JOIN (SELECT + JOIN + WHERE converti)
        # On utilise la requête complète pour extraire les champs nécessaires au lookup
        num_O2, size_S2, _ = self.get_query_stats(
            coll2_name, 
            coll2_join_key, 
            "C2",
            sql_query
        )
        
        # size_O2: PROJECTION sans JOIN (uniquement les champs SELECT de coll2)
        # On retire le JOIN pour ne garder que les champs projetés
        query_projection_c2 = self._create_projection_query(sql_query, coll2_name, remove_join=True)
        print(f"    Projection C2 (without JOIN): {query_projection_c2}")
        counts_o2 = self.analyze_schema_fields(coll2_name, query=query_projection_c2)
        size_O2 = self.compute_size_from_counts(counts_o2)
        
        C2_per_loop = S2 * size_S2 + num_O2 * size_O2
        C2_volume = loops * C2_per_loop
        
        Vt_total = C1_volume + C2_volume
        
        # === Affichage des résultats ===
        c1_op = f"Filtre {'avec' if is_sharded_C1 else 'sans'} sharding"
        c2_op = f"Boucle {'avec' if is_sharded_C2 else 'sans'} sharding"

        print(f"\n--- Coût de la Jointure ---")
        print(f"Collection 1: {coll1_name} (filtre sur {coll1_filter_key}, sharding sur {coll1_sharding_key})")
        print(f"Collection 2: {coll2_name} (join sur {coll2_join_key}, sharding sur {coll2_sharding_key})")
        
        print(f"\n[C1] {c1_op}")
        print(f"  Formule: C1 = #S1 × size_S1 + #O1 × size_O1")
        print(f"          C1 = {S1} × {size_S1} + {num_O1} × {size_O1}")
        print(f"          C1 = {C1_volume:,} B")
        print(f"  → Loops (O1) = {loops:,}")
        
        print(f"\n[C2] {c2_op} (×{loops:,} loops)")
        print(f"  Formule par loop: C2 = #S2 × size_S2 + #O2 × size_O2")
        print(f"                   C2 = {S2} × {size_S2} + {num_O2} × {size_O2}")
        print(f"  C2 par loop = {C2_per_loop:,} B")
        print(f"  C2 total = {loops:,} × {C2_per_loop:,} = {C2_volume:,} B")
        
        print(f"\n[Vt] Formule: Vt = C1 + loops × C2")
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
        Extrait les champs utilisés dans SELECT, WHERE et JOIN pour une collection donnée.
        
        Args:
            query: Requête SQL complète
            collection_name: Nom de la collection (pour filtrer les alias)
        
        Returns:
            Dict avec 'select', 'where', 'join' contenant les listes de champs
        """
        context = {'select': [], 'where': [], 'join': []}
        
        # Nettoyer la requête
        query = query.strip().replace('\n', ' ')
        query = re.sub(r'\s+', ' ', query)
        
        # Trouver l'alias de la collection
        # Ex: "FROM Product P" ou "JOIN Stock S"
        alias_pattern = rf'\b(?:FROM|JOIN)\s+{collection_name}\s+(\w+)'
        alias_match = re.search(alias_pattern, query, re.IGNORECASE)
        alias = alias_match.group(1) if alias_match else collection_name[0]
        
        # 1. Extraire les champs du SELECT
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, query, re.IGNORECASE)
        if select_match:
            select_clause = select_match.group(1)
            # Extraire les champs avec l'alias de notre collection
            # Ex: "P.name, P.price" -> ['name', 'price']
            field_pattern = rf'{alias}\.(\w+)'
            context['select'] = re.findall(field_pattern, select_clause)
        
        # 2. Extraire les champs du WHERE
        where_pattern = r'WHERE\s+(.*?)(?:;|$)'
        where_match = re.search(where_pattern, query, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1)
            field_pattern = rf'{alias}\.(\w+)'
            context['where'] = re.findall(field_pattern, where_clause)
        
        # 3. Extraire les champs du JOIN (ON clause)
        # Ex: "ON S.IDP = P.IDP" ou "ON P.IDP = S.IDP"
        join_pattern = rf'ON\s+(?:{alias}\.(\w+)\s*=\s*\w+\.\w+|\w+\.\w+\s*=\s*{alias}\.(\w+))'
        join_matches = re.finditer(join_pattern, query, re.IGNORECASE)
        for match in join_matches:
            # Le champ peut être dans le groupe 1 ou 2 selon l'ordre
            field = match.group(1) if match.group(1) else match.group(2)
            if field:
                context['join'].append(field)
        
        return context


    
    # --- analyze_schema_fields (Méthode de NoSQLDatabaseCalculator) --- context=queries     
    def analyze_schema_fields(self, collection_name: str, 
                             field_list: Optional[List[str]] = None,
                             query: Optional[str] = None) -> Dict:
        """
        Analyse un schéma pour compter les types de champs scalaires de premier niveau.
        
        Args:
            collection_name: Nom de la collection à analyser
            field_list: Liste des champs à inclure (pour projection simple)
            query: Requête SQL complète (pour extraction automatique du contexte)
        
        Returns:
            Dict contenant les comptages par type de champ
        """
        if collection_name not in self.collections:
            return {'num_int': 0, 'num_string': 0, 'num_date': 0, 
                    'num_longstring': 0, 'num_keys': 0}

        schema = self.collections[collection_name]['schema']
        all_fields = {}

        # 1. Extraction des champs scalaires de premier niveau avec leur type
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

        # 2. Déterminer les champs à compter selon le contexte
        fields_to_count = set()
        context_info = ""
        
        if query:
            # Extraction automatique du contexte depuis la requête
            context = self.extract_query_context(query, collection_name)
            for clause_fields in context.values():
                if clause_fields:
                    fields_to_count.update(clause_fields)
            context_info = f" | Context: {context}"
        elif field_list:
            # Projection simple ou liste explicite
            fields_to_count = set(field_list)
            context_info = f" | Fields: {field_list}"
        else:
            # Document complet
            fields_to_count = set(all_fields.keys())
            context_info = " | Full document"

        # 3. Comptage des scalaires pour les champs sélectionnés
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

        # 4. Calcul du nombre de clés
        if query or field_list:
            # Pour projection/requête : #clés = #champs comptés
            num_keys = len(fields_counted)
        else:
            # Pour document complet : #clés = #champs + 1 (_id)
            num_keys = len(all_fields) + 1
            
            # Ajustement spécifique pour St (DB1)
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
    def get_query_stats(self, collection_name: str, query_key: str, phase: str, 
                       sql_query: str) -> Tuple[int, int, int]:
        """
        Retourne (#OutputDocs, size_S, size_O) en LISANT les schémas.
        
        Args:
            collection_name: Nom de la collection à analyser
            query_key: Clé de la requête (ex: "IDP_IDW", "brand", etc.)
            phase: Phase de la requête ("C1" ou "C2")
            sql_query: Requête SQL complète (fournie par QUERIES[query_name])
        
        Returns:
            Tuple (num_output_docs, size_S, size_O)
        """
        
        print(f"\n[GET_QUERY_STATS] {collection_name} | {query_key} | {phase}")
        print(f"  SQL Query: {sql_query}")
        
        # ====================================================================
        # PARTIE 1: #O (nombre de documents) - Inchangé
        # ====================================================================
        if phase == "C1":
            if query_key == "IDP_IDW": 
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
        # PARTIE 2: size_S (taille de la REQUÊTE avec WHERE + JOIN)
        # ====================================================================
        
        print(f"\n  → Computing size_S (full query with WHERE + JOIN):")
        counts_s = self.analyze_schema_fields(collection_name, query=sql_query)
        size_S = self.compute_size_from_counts(counts_s)
        
        # ====================================================================
        # PARTIE 3: size_O (taille de la PROJECTION - SELECT uniquement)
        # ====================================================================
        
        # Créer une version de la requête sans WHERE pour la projection
        query_projection = self._create_projection_query(sql_query, collection_name, remove_join=False)
        
        print(f"\n  → Computing size_O (projection - SELECT only):")
        print(f"    Projection query: {query_projection}")
        counts_o = self.analyze_schema_fields(collection_name, query=query_projection)
        size_O = self.compute_size_from_counts(counts_o)
        
        # ====================================================================
        # CAS SPÉCIAUX
        # ====================================================================
        
        # Cas spécial Q4 DB3 (Projection agrégée : name + quantity)
        if collection_name == "St" and query_key == "IDW" and self.current_schema == "DB3":
            print("\n  [SPECIAL CASE] Q4 DB3 - Aggregated projection (name + quantity)")
            counts_name = self.analyze_schema_fields("Prod", field_list=["name"])
            counts_qty = self.analyze_schema_fields("St", field_list=["quantity"])
            
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
        Crée une requête de projection en retirant WHERE (et optionnellement JOIN).
        
        Args:
            sql_query: Requête SQL complète
            collection_name: Nom de la collection
            remove_join: Si True, retire aussi la clause JOIN (pour size_O en phase C2)
        
        Returns:
            Requête SQL sans clause WHERE (et sans JOIN si remove_join=True)
        """
        # Retirer la clause WHERE
        query_without_where = re.sub(r'\s+WHERE\s+.*?(;|$)', r'\1', sql_query, flags=re.IGNORECASE)
        
        if remove_join:
            # Retirer aussi la clause JOIN (pour C2 projection - on ne garde que SELECT)
            query_without_join = re.sub(r'\s+JOIN\s+.*?ON\s+.*?(?=WHERE|;|$)', '', query_without_where, flags=re.IGNORECASE)
            return query_without_join.strip()
        
        return query_without_where.strip()

    

