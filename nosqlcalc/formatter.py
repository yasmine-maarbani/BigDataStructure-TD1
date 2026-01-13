from typing import Dict, Any, List, Tuple

class ResultFormatter:
    """Formats calculation results for display"""
    
    @staticmethod
    def format_collection_analysis(analysis: Dict[str, Any]) -> str:
        """
        Format collection analysis results.
        
        Args:
            analysis: Dictionary containing collection analysis data
            
        Returns:
            Formatted string ready for display
        """
        lines = []
        lines.append("\n" + "="*70)
        lines.append(f"COLLECTION: {analysis['collection_name']}")
        lines.append(f"Detected type: {analysis['detected_type']}")
        lines.append("="*70)
        
        lines.append(f"\nSTATISTICS:")
        lines.append(f"  • Documents: {analysis['document_count']:,}")
        lines.append(f"  • Merges: {analysis['merge_count']}")
        
        lines.append(f"\nSCALARS OUTSIDE ARRAYS:")
        for key, val in analysis['scalars_outside'].items():
            if val > 0:
                lines.append(f"  • {key}: {val}")
        
        lines.append(f"\nSCALARS INSIDE ARRAYS:")
        if analysis['scalars_inside']:
            for array_name, info in analysis['scalars_inside'].items():
                avg = analysis['array_averages'].get(array_name, 1)
                lines.append(f"  • Array '{array_name}' (average: {avg:,.0f}):")
                for key, val in info['counts'].items():
                    if val > 0:
                        lines.append(f"    - {key}: {val} × {avg:,.0f} = {val * avg:,.0f}")
        else:
            lines.append("  (none)")
        
        breakdown = analysis['size_breakdown']
        lines.append(f"\nSIZE:")
        lines.append(f"  • Scalars (outside arrays): {breakdown['outside']:,} B")
        lines.append(f"  • Scalars (inside arrays): {breakdown['inside']:,} B")
        lines.append(f"  • Keys: {breakdown['keys']:,} B")
        lines.append(f"  • DOCUMENT: {analysis['document_size_bytes']:,} B")
        lines.append(f"  • COLLECTION: {analysis['collection_size_gb']:.4f} GB")
        lines.append("="*70)
        
        return "\n".join(lines)
    
    @staticmethod
    def format_database_summary(total_gb: float, details: Dict[str, float], 
                               doc_counts: Dict[str, int]) -> str:
        """
        Format database summary.
        
        Args:
            total_gb: Total database size in GB
            details: Dictionary mapping collection names to sizes
            doc_counts: Dictionary mapping collection names to document counts
            
        Returns:
            Formatted string ready for display
        """
        lines = []
        lines.append(f"\n{'='*70}")
        lines.append(f"DATABASE SUMMARY")
        lines.append(f"{'='*70}")
        
        lines.append(f"\nCOLLECTIONS:")
        for coll_name, size_gb in details.items():
            doc_count = doc_counts.get(coll_name, 0)
            lines.append(f"  • {coll_name:15s}: {size_gb:10.4f} GB  ({doc_count:,} docs)")
        
        lines.append(f"\nTOTAL: {total_gb:.4f} GB")
        lines.append(f"{'='*70}\n")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_sharding_stats(stats: Dict[str, Any]) -> str:
        """
        Format sharding statistics.
        
        Args:
            stats: Dictionary containing sharding statistics
            
        Returns:
            Formatted string ready for display
        """
        lines = []
        lines.append(f"\nSHARDING: {stats['collection']}-#{stats['sharding_key']}")
        lines.append(f"  • Total documents: {stats['total_docs']:,}")
        lines.append(f"  • Distinct values: {stats['distinct_values']:,}")
        lines.append(f"  • Servers: {stats['num_servers']:,}")
        lines.append(f"  • Docs/server: {stats['avg_docs_per_server']:,.2f}")
        lines.append(f"  • Distinct values/server: {stats['avg_distinct_values_per_server']:,.2f}")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_database_comparison(results: List[Dict[str, Any]]) -> str:
        """
        Format database comparison table.
        
        Args:
            results: List of database results to compare
            
        Returns:
            Formatted comparison table
        """
        lines = []
        lines.append("\n" + "="*80)
        lines.append("DATABASE SIZE COMPARISON")
        lines.append("="*80)
        lines.append("")
        lines.append(f"{'Database':<10} {'Collections':<15} {'Size (GB)':<15} {'Signature':<50}")
        lines.append("-"*90)
        
        for r in results:
            lines.append(f"{r['name']:<10} {r['collections']:<15} {r['size_gb']:<15.4f} {r['signature']:<50}")
        
        lines.append("\n" + "="*80)
        
        return "\n".join(lines)
