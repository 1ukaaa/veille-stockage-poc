#!/usr/bin/env python3
"""
Script avancé de fusion avec déduplication et validation
Usage: python merge_advanced.py --input out/enriched/ --output data/combined.csv
"""
import sys
import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
from datetime import datetime

def merge_csvs_advanced(
    input_dir: str,
    output_path: str,
    pattern: str = "enriched_*.csv",
    on_duplicate: str = "keep_best"  # "keep_best", "keep_first", "flag"
) -> Dict:
    """
    Fusionne CSVs avec déduplication et validation
    
    on_duplicate:
    - keep_best: Garde l'entrée avec plus haute confiance
    - keep_first: Garde la première occurrence
    - flag: Garde tout + colonne 'duplicate_group'
    """
    
    input_path = Path(input_dir)
    if not input_path.exists():
        print(f"❌ Répertoire introuvable: {input_dir}")
        sys.exit(1)
    
    csv_files = sorted(input_path.glob(pattern))
    if not csv_files:
        print(f"❌ Aucun fichier CSV trouvé matching '{pattern}'")
        sys.exit(1)
    
    print(f"📂 {len(csv_files)} fichiers trouvés")
    
    # ========== ÉTAPE 1 : LIRE TOUS LES CSVs ==========
    dfs = []
    total_rows = 0
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, dtype=str)
            df["_source_file"] = csv_file.name  # Tracer l'origine
            print(f"  ✓ {csv_file.name}: {len(df)} lignes")
            total_rows += len(df)
            dfs.append(df)
        except Exception as e:
            print(f"  ✗ {csv_file.name}: {e}")
    
    if not dfs:
        print("❌ Aucun fichier à fusionner")
        sys.exit(1)
    
    # Fusion brute
    combined = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Total brut: {total_rows} lignes")
    
    # ========== ÉTAPE 2 : DÉDUPLICATION ==========
    print("\n🔍 Détection des doublons...")
    
    # Clé d'unicité : project_id + commune (pour identifier duplicatas)
    if "project_id" in combined.columns and "commune" in combined.columns:
        # Créer clé de duplication
        combined["_dedup_key"] = combined["project_id"] + "_" + combined["commune"]
        
        duplicates = combined[combined.duplicated(subset=["_dedup_key"], keep=False)]
        unique_groups = combined["_dedup_key"].nunique()
        
        print(f"  Groupes uniques: {unique_groups}")
        print(f"  Doublons trouvés: {len(duplicates)}")
        
        if len(duplicates) > 0:
            print(f"\n  Exemples de doublons:")
            for key in duplicates["_dedup_key"].unique()[:3]:
                group = duplicates[duplicates["_dedup_key"] == key]
                print(f"    {key}: {len(group)} occurrences")
                for idx, row in group.iterrows():
                    confidence = row.get("permit_confidence", "N/A")
                    print(f"      - {row['_source_file']}: confiance={confidence}")
        
        # Gérer les doublons selon stratégie
        if on_duplicate == "keep_best":
            # Garder celle avec plus haute confiance
            if "permit_confidence" in combined.columns:
                combined["permit_confidence"] = pd.to_numeric(combined["permit_confidence"], errors="coerce")
                deduped = combined.sort_values("permit_confidence", ascending=False).drop_duplicates("_dedup_key", keep="first")
                print(f"\n✓ Dédupication (keep_best): {len(combined)} → {len(deduped)}")
            else:
                deduped = combined.drop_duplicates("_dedup_key", keep="first")
                print(f"\n✓ Dédupication (keep_first): {len(combined)} → {len(deduped)}")
        
        elif on_duplicate == "keep_first":
            deduped = combined.drop_duplicates("_dedup_key", keep="first")
            print(f"\n✓ Dédupication (keep_first): {len(combined)} → {len(deduped)}")
        
        elif on_duplicate == "flag":
            # Ajouter colonne indiquant les doublons
            deduped = combined.copy()
            deduped["is_duplicate"] = deduped.duplicated(subset=["_dedup_key"], keep=False)
            deduped["duplicate_count"] = deduped.groupby("_dedup_key")["_dedup_key"].transform("count")
            print(f"\n✓ Doublons flaggés: {deduped['is_duplicate'].sum()} lignes")
        
        else:
            deduped = combined
    
    else:
        print("  ⚠️  Colonnes 'project_id' ou 'commune' manquantes → pas de déduplication")
        deduped = combined
    
    # ========== ÉTAPE 3 : VALIDATION ET STATISTIQUES ==========
    print("\n📈 Statistiques:")
    
    if "permit_confidence" in deduped.columns:
        deduped["permit_confidence"] = pd.to_numeric(deduped["permit_confidence"], errors="coerce")
        high_conf = (deduped["permit_confidence"] >= 0.6).sum()
        avg_conf = deduped["permit_confidence"].mean()
        print(f"  Permis avec confiance ≥ 0.6: {high_conf}/{len(deduped)} ({high_conf/len(deduped)*100:.1f}%)")
        print(f"  Confiance moyenne: {avg_conf:.2f}")
    
    if "permit_number" in deduped.columns:
        permits_found = deduped["permit_number"].notna().sum()
        print(f"  Permis trouvés: {permits_found}/{len(deduped)}")
    
    if "region" in deduped.columns:
        regions = deduped["region"].value_counts()
        print(f"  Par région:")
        for region, count in regions.items():
            print(f"    {region}: {count}")
    
    # ========== ÉTAPE 4 : SAUVEGARDER ==========
    print("\n💾 Sauvegarde...")
    
    # Nettoyer colonnes temporaires
    if "_dedup_key" in deduped.columns:
        deduped = deduped.drop(columns=["_dedup_key"])
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    deduped.to_csv(output_path, index=False, encoding="utf-8")
    
    # Générer rapport JSON
    report = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "input_dir": str(input_path),
            "output_file": str(output_path),
            "num_files": len(csv_files),
            "deduplication_strategy": on_duplicate
        },
        "statistics": {
            "rows_input": len(combined),
            "rows_output": len(deduped),
            "rows_removed_duplicates": len(combined) - len(deduped),
            "deduplication_rate": round((1 - len(deduped) / len(combined)) * 100, 1) if len(combined) > 0 else 0
        },
        "files_processed": [f.name for f in csv_files]
    }
    
    report_path = Path(output_path).parent / (Path(output_path).stem + "_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"✅ Résultat: {len(deduped)} lignes")
    print(f"📄 Fichier: {output_path}")
    print(f"📊 Rapport: {report_path}")
    
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fusion avancée de CSVs avec déduplication")
    parser.add_argument("--input", default="out/enriched", help="Répertoire d'entrée")
    parser.add_argument("--output", default="data/combined_all.csv", help="Fichier de sortie")
    parser.add_argument("--pattern", default="enriched_*.csv", help="Pattern de fichiers")
    parser.add_argument("--dedup", choices=["keep_best", "keep_first", "flag"], default="keep_best",
                        help="Stratégie de déduplication")
    args = parser.parse_args()
    
    merge_csvs_advanced(args.input, args.output, args.pattern, args.dedup)
