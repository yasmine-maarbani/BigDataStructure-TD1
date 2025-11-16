from typing import Dict, List, Tuple, Optional


class NoSQLDatabaseCalculator:
    """
    Calculateur de taille pour bases de données NoSQL.
    
    Utilisation:
        stats = {'clients': 10**7, 'products': 10**5, ...}
        calc = NoSQLDatabaseCalculator(stats)
        calc.add_collection("Product", schema)
        calc.print_collection_analysis("Product")
    """
    
    # Tailles des types (en Bytes)
    SIZE_NUMBER = 8
    SIZE_STRING = 80
    SIZE_DATE = 20
    SIZE_LONG_STRING = 200
    SIZE_KEY_VALUE = 12
    
    def __init__(self, statistics: Dict):
        """
        Initialise le calculateur.
        
        Args:
            statistics: dict avec 'clients', 'products', 'order_lines', 
                       'warehouses', 'avg_categories_per_product'
        """
        self.statistics = statistics
        self.collections = {}
        
        # Nombre de documents par collection
        self.nb_docs = {
            "Cl": statistics.get('clients', 10**7),
            "Prod": statistics.get('products', 10**5),
            "OL": statistics.get('order_lines', 4 * 10**9),
            "Wa": statistics.get('warehouses', 200),
        }
        self.nb_docs["St"] = self.nb_docs["Prod"] * self.nb_docs["Wa"]
        
        # Matrice de relations : combien d'enfants par parent
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
        
        # Mapping: nom d'array → type de collection
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
    # DÉTECTION AUTOMATIQUE DE COLLECTION
    # ================================================================
    
    def guess_collection_name(self, schema: Dict) -> str:
        """
        Détecte le type de collection depuis le schéma.
        
        Règles:
            - Prod: a IDP et price
            - Cat: a title
            - Supp: a IDS et SIRET
            - St: a location et quantity
            - Cl: a IDC et email
            - OL: a date et deliveryDate
            - Wa: a IDW et capacity
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
    # COMPTAGE DES SCALAIRES AVEC TRACKING DES PARENTS
    # ================================================================
    
    def count_scalars_with_arrays(self, schema: Dict) -> Tuple[Dict, Dict]:
        """
        Compte les scalaires en distinguant ceux dans/hors des arrays.
        Track aussi la collection parente de chaque array.
        
        Returns:
            (counts_outside, inside)
            
            counts_outside = {int, string, date, long}
            inside = {
                "array_name": {
                    "counts": {int, string, date, long},
                    "parent": "Prod"  ← Collection propriétaire
                }
            }
        """
        counts_outside = {"int": 0, "string": 0, "date": 0, "long": 0}
        inside = {}
        
        def init_array(arr_name: str, parent_coll: str):
            """Initialise le compteur pour un array."""
            if arr_name not in inside:
                inside[arr_name] = {
                    "counts": {"int": 0, "string": 0, "date": 0, "long": 0},
                    "parent": parent_coll
                }
        
        def add_scalar(node_type: str, field_name: str, current_array: Optional[str]):
            """Ajoute un scalaire au bon compteur."""
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
            """Explore récursivement le schéma."""
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
            
            # OBJECT (peut changer de collection)
            if node_type == "object":
                detected = self.guess_collection_name(node)
                if detected != "Unknown":
                    current_coll = detected
                
                for k, sub in node.get("properties", {}).items():
                    explore(sub, current_coll, k, current_array)
                return
            
            # SCALAIRE
            add_scalar(node_type, field_name, current_array)
        
        root_coll = self.guess_collection_name(schema)
        explore(schema, root_coll)
        return counts_outside, inside
    
    # ================================================================
    # COMPTAGE DES MERGES (TRANSITIONS DE COLLECTIONS)
    # ================================================================
    
    def count_merges(self, schema: Dict, parent_coll: Optional[str] = None, 
                     is_root: bool = True) -> int:
        """
        Compte les transitions entre collections (merges).
        
        Un merge = passage d'une collection à une autre
        Exemple: Prod → Cat, Prod → Supp
        """
        if not isinstance(schema, dict):
            return 0
        
        node_type = schema.get("type")
        merges = 0
        coll = None
        
        # Détecte la collection de ce nœud
        if node_type == "object":
            coll = self.guess_collection_name(schema)
        elif node_type == "array":
            items = schema.get("items")
            if isinstance(items, dict):
                coll = self.guess_collection_name(items)
        
        # Compte le merge si changement de collection (pas à la racine)
        if not is_root and coll not in (None, "Unknown") and coll != parent_coll:
            merges += 1
            parent_for_children = coll
        else:
            parent_for_children = parent_coll if parent_coll else coll
        
        # Récursion
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
    # CALCUL DE TAILLE D'UN DOCUMENT
    # ================================================================
    
    def compute_document_size(self, schema: Dict) -> Dict:
        """
        Calcule la taille complète d'un document.
        
        Formule:
            doc_size = scalaires_hors_arrays + scalaires_dans_arrays + keys
        
        Utilise les avg_length réalistes pour les arrays.
        """
        parent = self.guess_collection_name(schema)
        outside, inside = self.count_scalars_with_arrays(schema)
        avg_used = {}
        
        # Taille des scalaires hors arrays
        size_outside = (
            outside["int"] * self.SIZE_NUMBER +
            outside["string"] * self.SIZE_STRING +
            outside["date"] * self.SIZE_DATE +
            outside["long"] * self.SIZE_LONG_STRING
        )
        
        # Taille des scalaires dans arrays (avec moyennes réalistes)
        size_inside_total = 0
        for array_name, info in inside.items():
            counts = info["counts"]
            parent_for_avg = info["parent"] or parent
            child = self.array_to_collection.get(array_name, "Unknown")
            
            # Récupère la moyenne depuis la matrice de relations
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
        
        # Taille des keys
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
    # GESTION DES COLLECTIONS
    # ================================================================
    
    def add_collection(self, name: str, schema: Dict, doc_count: Optional[int] = None):
        """
        Ajoute une collection.
        
        Args:
            name: Nom de la collection (ex: "Product")
            schema: JSON Schema
            doc_count: Nombre de docs (auto-détecté si None)
        """
        if doc_count is None:
            detected = self.guess_collection_name(schema)
            doc_count = self.nb_docs.get(detected, 1)
        
        self.collections[name] = {
            'schema': schema,
            'doc_count': doc_count
        }
    
    def compute_collection_size_gb(self, collection_name: str) -> float:
        """Calcule la taille d'une collection en GB."""
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' introuvable")
        
        coll = self.collections[collection_name]
        result = self.compute_document_size(coll['schema'])
        return result['collection_size'] / (10 ** 9)
    
    def compute_database_size_gb(self) -> Tuple[float, Dict[str, float]]:
        """
        Calcule la taille totale de la base.
        
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
        """Analyse complète d'une collection."""
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' introuvable")
        
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
        Calcule les statistiques de distribution avec sharding.
        
        Args:
            collection_name: Nom de la collection
            sharding_key: Clé de sharding (ex: 'IDP', 'IDC')
            distinct_key_values: Nombre de valeurs distinctes
            num_servers: Nombre de serveurs (défaut: 1000)
        """
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' introuvable")
        
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
    # AFFICHAGE
    # ================================================================
    
    def print_collection_analysis(self, collection_name: str):
        """Affiche l'analyse d'une collection."""
        analysis = self.analyze_collection(collection_name)
        
        print("\n" + "="*70)
        print(f"COLLECTION: {analysis['collection_name']}")
        print(f"Type détecté: {analysis['detected_type']}")
        print("="*70)
        
        print(f"\nSTATISTIQUES:")
        print(f"  • Documents: {analysis['document_count']:,}")
        print(f"  • Merges: {analysis['merge_count']}")
        
        print(f"\nSCALAIRES HORS ARRAYS:")
        for key, val in analysis['scalars_outside'].items():
            if val > 0:
                print(f"  • {key}: {val}")
        
        print(f"\nSCALAIRES DANS ARRAYS:")
        if analysis['scalars_inside']:
            for array_name, info in analysis['scalars_inside'].items():
                avg = analysis['array_averages'].get(array_name, 1)
                print(f"  • Array '{array_name}' (moyenne: {avg:,.0f}):")
                for key, val in info['counts'].items():
                    if val > 0:
                        print(f"    - {key}: {val} × {avg:,.0f} = {val * avg:,.0f}")
        else:
            print("  (aucun)")
        
        breakdown = analysis['size_breakdown']
        print(f"\nTAILLE:")
        print(f"  • Scalaires (hors arrays): {breakdown['outside']:,} B")
        print(f"  • Scalaires (dans arrays): {breakdown['inside']:,} B")
        print(f"  • Keys: {breakdown['keys']:,} B")
        print(f"  • DOCUMENT: {analysis['document_size_bytes']:,} B")
        print(f"  • COLLECTION: {analysis['collection_size_gb']:.4f} GB")
        print("="*70)
    
    def print_database_summary(self):
        """Affiche le résumé de la base."""
        total_gb, details = self.compute_database_size_gb()
        
        print(f"\n{'='*70}")
        print(f"RÉSUMÉ BASE DE DONNÉES")
        print(f"{'='*70}")
        
        print(f"\nCOLLECTIONS:")
        for coll_name, size_gb in details.items():
            doc_count = self.collections[coll_name]['doc_count']
            print(f"  • {coll_name:15s}: {size_gb:10.4f} GB  ({doc_count:,} docs)")
        
        print(f"\nTOTAL: {total_gb:.4f} GB")
        print(f"{'='*70}\n")
    
    def print_sharding_stats(self, collection_name: str, sharding_key: str, 
                            distinct_values: int):
        """Affiche les stats de sharding."""
        stats = self.compute_sharding_stats(collection_name, sharding_key, distinct_values)
        
        print(f"\nSHARDING: {stats['collection']}-#{stats['sharding_key']}")
        print(f"  • Documents totaux: {stats['total_docs']:,}")
        print(f"  • Valeurs distinctes: {stats['distinct_values']:,}")
        print(f"  • Serveurs: {stats['num_servers']:,}")
        print(f"  • Docs/serveur: {stats['avg_docs_per_server']:,.2f}")
        print(f"  • Valeurs distinctes/serveur: {stats['avg_distinct_values_per_server']:,.2f}")



if __name__ == "__main__":
    
    # Statistiques du TD
    stats = {
        'clients': 10**7,
        'products': 10**5,
        'order_lines': 4 * 10**9,
        'warehouses': 200,
        'avg_categories_per_product': 2,
        'brands': 5000,
    }
    
    # Créer le calculateur
    calc = NoSQLDatabaseCalculator(stats)
    
    # Schéma Product avec Categories et Supplier
    product_schema = {
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
    
    # Ajouter et analyser
    calc.add_collection("Product", product_schema)
    calc.print_collection_analysis("Product")
    
    # Stats de sharding
    print("\n" + "="*70)
    print("SHARDING")
    print("="*70)
    calc.print_sharding_stats("Product", "IDP", stats['products'])
    calc.print_sharding_stats("Product", "brand", stats['brands'])
    
    # Résumé
    calc.print_database_summary()