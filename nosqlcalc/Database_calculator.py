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

    # Sizes of types (in Bytes)
    SIZE_NUMBER = 8
    SIZE_STRING = 80
    SIZE_DATE = 20
    SIZE_LONG_STRING = 200
    SIZE_KEY_VALUE = 12

    def __init__(self, statistics: Dict):
        """
        Initializes the calculator.

        Args:
            statistics: dict with 'clients', 'products', 'order_lines',
                       'warehouses', 'avg_categories_per_product'
        """
        self.statistics = statistics
        self.collections = {}

        # Number of documents per collection
        self.nb_docs = {
            "Cl": statistics.get('clients', 10**7),
            "Prod": statistics.get('products', 10**5),
            "OL": statistics.get('order_lines', 4 * 10**9),
            "Wa": statistics.get('warehouses', 200),
        }
        self.nb_docs["St"] = self.nb_docs["Prod"] * self.nb_docs["Wa"]

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