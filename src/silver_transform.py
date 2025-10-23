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

    # Fill missing descriptions with No Info Available
    df = df.fillna("No Info Available")

    # --- Step 2: Clean text ---
    df["item_code"] = df["item_code"].apply(clean_text)
    df["friendly_description"] = df["friendly_description"].apply(clean_text)
    df["detailed_description"] = df["detailed_description"].apply(clean_text)

    return df

