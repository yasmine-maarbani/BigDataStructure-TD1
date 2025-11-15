import json
from typing import Dict, List, Tuple

class NoSQLDatabaseCalculator:
    """
    Classe pour automatiser les calculs de taille et de distribution 
    pour des bases de données NoSQL denormalisées.
    
    Utilise les fonctions existantes:
    - count_scalars_with_arrays
    - count_merges
    - compute_document_size
    """
    
    # Tailles des types de données (en Bytes)
    size_number = 8
    size_string = 80
    size_date = 20
    size_longString = 200
    size_keyValue = 12
    
    def __init__(self, statistics: Dict):
        """
        Initialise le calculateur avec des statistiques.
        
        Args:
            statistics: Dictionnaire contenant les statistiques 
                       (nb clients, produits, warehouses, etc.)
        """
        self.statistics = statistics
        self.collections = {}  # Stocke les schémas des collections
        self.avg_length_cat = statistics.get('avg_categories_per_product', 2)
    
    def add_collection(self, name: str, schema: Dict, doc_count: int):
        """
        Ajoute une collection à gérer.
        
        Args:
            name: Nom de la collection (ex: "Product", "Stock", "OrderLine")
            schema: JSON Schema de la collection
            doc_count: Nombre de documents dans cette collection
        """
        self.collections[name] = {
            'schema': schema,
            'doc_count': doc_count
        }
    
    # ====================
    # FONCTIONS EXISTANTES 
    # ====================
    
    def count_scalars_with_arrays(self, schema):
        """
        Analyse un JSON Schema et renvoie :

        - counts_outside : scalaires hors tableaux
        - counts_inside  : scalaires dans des tableaux

        Règles :
        integer + number → "int"
        string → "string" sauf "description" ou "comment" → "long"
        date → "date"
        long string = champs 'description' ou 'comment'
        """
        counts_outside = {"int": 0, "string": 0, "date": 0, "long": 0}
        counts_inside = {}  

        def init_array_counter(name):
            if name not in counts_inside:
                counts_inside[name] = {"int": 0, "string": 0, "date": 0, "long": 0}

        def add_scalar(node_type, field_name, inside_array):
            """
            inside_array = None si hors array
            inside_array = nom du tableau si dedans
            """
            if inside_array is None:
                target = counts_outside
            else:
                init_array_counter(inside_array)
                target = counts_inside[inside_array]

            if node_type in ["integer", "number"]:
                target["int"] += 1

            elif node_type == "string":
                if field_name in ["description", "comment"]:
                    target["long"] += 1
                else:
                    target["string"] += 1

            elif node_type == "date":
                target["date"] += 1

        def explore(node, field_name=None, inside_array=None):
            """Explore récursivement le JSON Schema."""
            if not isinstance(node, dict):
                return

            node_type = node.get("type")

            # ARRAY → inside_array devient le nom du tableau
            if node_type == "array":
                items = node.get("items")
                array_name = field_name  # nom du tableau
                init_array_counter(array_name)

                if isinstance(items, dict):
                    explore(items, inside_array=array_name)

                elif isinstance(items, list):
                    for it in items:
                        explore(it, inside_array=array_name)
                return

            # OBJECT
            if node_type == "object":
                props = node.get("properties", {})
                for name, subnode in props.items():
                    explore(subnode, field_name=name, inside_array=inside_array)
                return

            # SCALAR
            add_scalar(node_type, field_name, inside_array)

        explore(schema)
        return counts_outside, counts_inside
    
    def count_merges(self, schema):
        """
        Un merge = au premier niveau :
            - un objet (type: object)
            - un tableau (type: array)
        Les scalaires ne comptent pas.
        """
        merges = 0

        if schema.get("type") != "object":
            return 0

        properties = schema.get("properties", {})

        for name, prop in properties.items():
            t = prop.get("type")

            if t == "object":
                merges += 1

            elif t == "array":
                merges += 1

        return merges
    
    def compute_document_size(self, schema):
        """
        Calcule la taille d'un document en utilisant count_scalars_with_arrays
        et count_merges.
        """
        # 1. Comptage scalaires
        outside, inside = self.count_scalars_with_arrays(schema)

        # 2. Taille scalaires hors tableau
        size_outside = (
            outside["int"]    * self.size_number +
            outside["string"] * self.size_string +
            outside["date"]   * self.size_date +
            outside["long"]   * self.size_longString
        )

        # 3. Taille scalaires DANS tableaux (par tableau)
        size_inside_total = 0

        for array_name, counts in inside.items():
            if array_name == "categories":
                avg = self.avg_length_cat
            else:
                avg = 1 

            size_array = (
                counts["int"]    * self.size_number +
                counts["string"] * self.size_string +
                counts["date"]   * self.size_date +
                counts["long"]   * self.size_longString
            ) * avg

            size_inside_total += size_array

        # 4. KEYS
        keys_outside = sum(outside.values())

        keys_arrays = 0
        for array_name, counts in inside.items():
            total_scalars = (
                counts["int"] + counts["string"] + counts["date"] + counts["long"]
            )

            if array_name == "categories":
                avg = self.avg_length_cat
            else:
                avg = 1

            keys_arrays += total_scalars * avg

        # merges auto detectés
        merges = self.count_merges(schema)

        # total keys
        keys_total = keys_outside + keys_arrays + merges
        size_keys_total = keys_total * self.size_keyValue

        # 6. total
        doc_size = size_outside + size_inside_total + size_keys_total 

        return {
            "outside_scalars": size_outside,
            "inside_scalars": size_inside_total,
            "nb_keys": keys_total,
            "keys": size_keys_total,
            "doc_size": doc_size
        }
    
    # ====================
    # NOUVELLES FONCTIONS 
    # ====================
    
    def compute_collection_size_gb(self, collection_name: str) -> float:
        """
        Calcule la taille d'une collection en GB.
        Args:
            collection_name: Nom de la collection
        Returns:
            Taille en GB
        """
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' non trouvée")
        
        coll = self.collections[collection_name]
        schema = coll['schema']
        doc_count = coll['doc_count']
        
        # Calcul taille document
        doc_info = self.compute_document_size(schema)
        doc_size_bytes = doc_info['doc_size']
        
        # Taille totale
        total_bytes = doc_size_bytes * doc_count
        total_gb = total_bytes / (10 ** 9)  # Conversion en GB
        
        return total_gb
    
    def compute_database_size_gb(self) -> Tuple[float, Dict[str, float]]:
        """
        Calcule la taille totale de la base de données (toutes collections).
        Returns:
            Tuple (taille_totale_gb, dict_par_collection)
        """
        total_gb = 0
        details = {}
        
        for coll_name in self.collections:
            coll_size = self.compute_collection_size_gb(coll_name)
            details[coll_name] = coll_size
            total_gb += coll_size
        
        return total_gb, details
    
    def analyze_collection(self, collection_name: str) -> Dict:
        """
        Analyse complète d'une collection.
        Returns:
            Dict avec toutes les métriques
        """
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' non trouvée")
        
        coll = self.collections[collection_name]
        schema = coll['schema']
        doc_count = coll['doc_count']
        
        # Comptages
        outside, inside = self.count_scalars_with_arrays(schema)
        merges = self.count_merges(schema)
        doc_info = self.compute_document_size(schema)
        
        # Taille collection
        collection_size_gb = self.compute_collection_size_gb(collection_name)
        
        return {
            'collection_name': collection_name,
            'document_count': doc_count,
            'scalars_outside': outside,
            'scalars_inside': inside,
            'merge_count': merges,
            'document_size_bytes': doc_info['doc_size'],
            'document_size_breakdown': doc_info,
            'collection_size_gb': round(collection_size_gb, 4)
        }
    
    def compute_sharding_stats(self, collection_name: str,sharding_key: str,distinct_key_values: int,num_servers: int = 1000) -> Dict:
        """
        Calcule les statistiques de distribution avec sharding.
        Args:
            collection_name: Nom de la collection
            sharding_key: Clé de sharding (ex: 'IDP', 'IDW', 'IDC', 'brand')
            distinct_key_values: Nombre de valeurs distinctes pour la clé
            num_servers: Nombre de serveurs (défaut: 1000)
        Returns:
            Dict avec les statistiques de distribution
        """
        if collection_name not in self.collections:
            raise ValueError(f"Collection '{collection_name}' non trouvée")
        
        total_docs = self.collections[collection_name]['doc_count']
        
        avg_docs_per_server = total_docs / num_servers
        avg_distinct_values_per_server = distinct_key_values / num_servers
        
        return {
            'collection': collection_name,
            'sharding_key': sharding_key,
            'total_docs': total_docs,
            'distinct_values': distinct_key_values,
            'num_servers': num_servers,
            'avg_docs_per_server': round(avg_docs_per_server, 2),
            'avg_distinct_values_per_server': round(avg_distinct_values_per_server, 2)
        }
    
# ======================
# FONCTIONS D'AFFICHAGE 
# ======================
    
    def print_collection_analysis(self, collection_name: str):
        """Affiche l'analyse d'une collection de manière formatée."""
        analysis = self.analyze_collection(collection_name)
        print("\n" + "="*70)
        print(f"ANALYSE DE LA COLLECTION: {analysis['collection_name']}")
        
        print(f"\n STATISTIQUES GÉNÉRALES:")
        print(f"  - Nombre de documents: {analysis['document_count']:,}")
        print(f"  - Nombre de merges: {analysis['merge_count']}")
        
        print(f"\n  SCALAIRES HORS TABLEAUX:")
        for key, val in analysis['scalars_outside'].items():
            if val > 0:
                print(f"  - {key}: {val}")
        
        print(f"\n  SCALAIRES DANS TABLEAUX:")
        for array_name, counts in analysis['scalars_inside'].items():
            print(f"  - Array '{array_name}':")
            for key, val in counts.items():
                if val > 0:
                    print(f"    - {key}: {val}")
        
        print(f"\n  TAILLE:")
        breakdown = analysis['document_size_breakdown']
        print(f"  - Scalaires (hors arrays): {breakdown['outside_scalars']} B")
        print(f"  - Scalaires (dans arrays): {breakdown['inside_scalars']} B")
        print(f"  - Keys: {breakdown['keys']} B ({breakdown['nb_keys']} keys)")
        print(f"  - TAILLE DOCUMENT: {breakdown['doc_size']} B")
        print(f"  - TAILLE COLLECTION: {analysis['collection_size_gb']:.4f} GB")

    
    def print_database_summary(self):
        """Affiche un résumé de toute la base de données."""
        total_gb, details = self.compute_database_size_gb()
        
        print(f"\n{'='*70}")
        print(f"RÉSUMÉ DE LA BASE DE DONNÉES")
        
        print(f"\n  COLLECTIONS:")
        for coll_name, size_gb in details.items():
            doc_count = self.collections[coll_name]['doc_count']
            print(f"  - {coll_name:15s}: {size_gb:10.4f} GB  ({doc_count:,} docs)")
        
        print(f"\n  TAILLE TOTALE: {total_gb:.4f} GB")
        print(f"{'='*70}\n")
    
    def print_sharding_stats(self, collection_name: str, 
                            sharding_key: str, 
                            distinct_values: int):
        """Affiche les statistiques de sharding."""
        stats = self.compute_sharding_stats(collection_name, sharding_key, distinct_values)
        
        print(f"SHARDING: {stats['collection']}-#{stats['sharding_key']}")

        print(f"  - Documents totaux: {stats['total_docs']:,}")
        print(f"  - Valeurs distinctes: {stats['distinct_values']:,}")
        print(f"  - Nombre de serveurs: {stats['num_servers']:,}")
        print(f"  - Moyenne docs/serveur: {stats['avg_docs_per_server']:,.2f}")
        print(f"  - Moyenne valeurs distinctes/serveur: {stats['avg_distinct_values_per_server']:,.2f}")


# ============================================
# EXEMPLE D'UTILISATION
# ============================================

if __name__ == "__main__":
    
    # Statistiques du projet
    statistics = {
        'clients': 10**7,
        'products': 10**5,
        'order_lines': 4 * 10**9,
        'warehouses': 200,
        'servers': 1000,
        'orders_per_client': 100,
        'products_per_order': 20,
        'avg_categories_per_product': 2,
        'brands': 5000,
        'apple_products': 50,
    }
    
    # Créer le calculateur
    calc = NoSQLDatabaseCalculator(statistics)
    
    # Exemple: Schema Product avec Categories et Supplier (DB1)
    product_schema = {
        "type": "object",
        "properties": {
            "IDP": {"type": "int"},
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
                    "Revenue": {"type": "number"}
                }
            }
        }
    }
    
    
    # Ajouter la collection Product
    calc.add_collection("Product", product_schema, statistics['products'])
    
    # Analyser la collection
    calc.print_collection_analysis("Product")
    

    # Statistiques de sharding
    print("\n" + "="*70)
    print("\n STATISTIQUES DE SHARDING:\n")
    
    # Prod-#IDP
    calc.print_sharding_stats("Product", "IDP", statistics['products'])
    
    # Prod-#brand
    calc.print_sharding_stats("Product", "brand", statistics['brands'])
    
    # Résumé de la base
    calc.print_database_summary()