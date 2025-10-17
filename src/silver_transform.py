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
    - Combines description_a and description_b into a single description column
    - Deduplicates products by name + description
    """
    # --- Step 1: Ensure columns exist ---
    for col in ["name", "description_a", "description_b"]:
        if col not in df.columns:
            df[col] = ""

    # Fill missing descriptions with empty string
    df["description_a"] = df["description_a"].fillna("")
    df["description_b"] = df["description_b"].fillna("")

    # --- Step 2: Clean text ---
    df["name"] = df["name"].apply(clean_text)
    df["description_a"] = df["description_a"].apply(clean_text)
    df["description_b"] = df["description_b"].apply(clean_text)

    return df

