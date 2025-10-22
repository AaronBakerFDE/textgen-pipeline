#!/usr/bin/env python3
import pandas as pd
import numpy as np
import argparse

def normalize_upc(series: pd.Series) -> pd.Series:
    """Digits only, strip leading zeros; empty -> NA."""
    s = series.fillna("").str.replace(r"[^0-9]", "", regex=True).str.lstrip("0")
    return s.replace("", pd.NA)

def normalize_code(series: pd.Series) -> pd.Series:
    """Trim + uppercase; empty -> NA."""
    s = series.fillna("").str.strip().str.upper()
    return s.replace("", pd.NA)

def main(api_entities_csv, storis_csv, via_product_csv, translator_csv, out_csv):
    # ---- Load (all columns) ----
    api_entities = pd.read_csv(api_entities_csv, dtype=str, low_memory=False)
    storis_product = pd.read_csv(storis_csv, dtype=str, low_memory=False)
    via_product = pd.read_csv(via_product_csv, dtype=str, low_memory=False)
    via_tagging_translator = pd.read_csv(translator_csv, dtype=str, low_memory=False)

    # ---- Join keys (normalized) ----
    api_entities["_norm_upc"]      = normalize_upc(api_entities.get("upc"))
    api_entities["_norm_itemcode"] = normalize_code(api_entities.get("itemCode"))
    api_entities["_norm_sku"]      = normalize_code(api_entities.get("sku"))

    storis_product["_norm_upc"] = normalize_upc(storis_product.get("UPCNbr")).fillna(
        normalize_upc(storis_product.get("BarCodeNbr"))
    )
    via_product["_norm_upc"]  = normalize_upc(via_product.get("BarCodeNbr"))
    via_product["_norm_item"] = normalize_code(via_product.get("ItemNumber"))

    via_tagging_translator["_norm_item"]    = normalize_code(via_tagging_translator.get("ItemNumber"))
    via_tagging_translator["_norm_fde_sku"] = normalize_code(via_tagging_translator.get("FDE SKU"))

    # ---- Deduplicate per join key for faster joins (keep first occurrence) ----
    storis_dedup          = storis_product.drop_duplicates(subset=["_norm_upc"], keep="first")
    via_prod_dedup        = via_product.drop_duplicates(subset=["_norm_upc"], keep="first")
    translator_by_item    = via_tagging_translator.drop_duplicates(subset=["_norm_item"], keep="first")
    translator_by_fde_sku = via_tagging_translator.drop_duplicates(subset=["_norm_fde_sku"], keep="first")

    # ---- Master: start from API entities (keeps ALL its rows/cols) ----
    # Merge STORIS on UPC, then VIA on UPC (suffixes keep ALL columns)
    master = api_entities.set_index("_norm_upc")
    master = master.join(storis_dedup.set_index("_norm_upc"), how="left", rsuffix="_storis")
    master = master.join(via_prod_dedup.set_index("_norm_upc"), how="left", rsuffix="_viaUPC")

    # Merge VIA Tagging Translator twice: 
    #   1) by ItemNumber ↔ itemCode
    #   2) by FDE SKU    ↔ sku
    master = master.reset_index()
    master = (
        master.set_index("_norm_itemcode")
              .join(translator_by_item.set_index("_norm_item"), how="left", rsuffix="_translatorByItem")
              .reset_index()
    )
    master = (
        master.set_index("_norm_sku")
              .join(translator_by_fde_sku.set_index("_norm_fde_sku"), how="left", rsuffix="_translatorByFDE")
              .reset_index()
    )

    # ---- Drop completely empty columns ----
    master = master.dropna(axis=1, how="all")

    # ---- Output (ALL columns preserved except empty ones) ----
    master.to_csv(out_csv, index=False)

    # ---- Optional: quick coverage stats printed to console ----
    total = len(master)
    upc_hits_storis = master["UPCNbr"].notna().sum() if "UPCNbr" in master.columns else 0
    upc_hits_via    = master["BarCodeNbr"].notna().sum() if "BarCodeNbr" in master.columns else 0
    item_hits       = master.filter(like="Series ID").shape[0]  # crude indicator for translator hit

    print(f"Rows: {total:,}")
    print(f"Columns after drop: {len(master.columns):,}")
    print(f"UPC matches to STORIS: {upc_hits_storis:,}")
    print(f"UPC matches to VIA:    {upc_hits_via:,}")
    print("Done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build Product Information Master (all columns, dropping empty columns).")
    p.add_argument("--api_entities", required=True, help="Path to api_entities.csv")
    p.add_argument("--storis", required=True, help="Path to storis_product.csv")
    p.add_argument("--via_product", required=True, help="Path to via_product.csv")
    p.add_argument("--translator", required=True, help="Path to via_tagging_translator.csv")
    p.add_argument("--out", required=True, help="Output CSV path")
    args = p.parse_args()
    main(args.api_entities, args.storis, args.via_product, args.translator, args.out)
