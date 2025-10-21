import pandas as pd
import re

def clean_text(text: str) -> str:
    """
    Basic text cleaning:
    - Strip leading/trailing whitespace
    - Replace multiple spaces with single space
    - Remove unwanted characters
    """
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-zA-Z0-9.,!?\s]", "", text)
    return text

def silver_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms Bronze data into Silver layer:
    - Cleans text
    - Deduplicates products by name + description
    """
    # --- Step 1: Ensure columns exist ---
    for col in ["sku_id", "afi_product_description", "via_product_description"]:
        if col not in df.columns:
            df[col] = ""

    # Fill missing descriptions with empty string
    df["afi_product_description"] = df["afi_product_description"].fillna("")
    df["via_product_description"] = df["via_product_description"].fillna("")

    # --- Step 2: Clean text ---
    df["sku_id"] = df["sku_id"].apply(clean_text)
    df["afi_product_description"] = df["afi_product_description"].apply(clean_text)
    df["via_product_description"] = df["via_product_description"].apply(clean_text)

    return df

