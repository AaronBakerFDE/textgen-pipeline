import pandas as pd
from tqdm import tqdm
from src.utils import generate_description_ollama

tqdm.pandas()

def gold_enrich(df: pd.DataFrame, model: str, system_prompt: str, prompt_template: str) -> pd.DataFrame:
    for col in ["afi_product_description", "via_product_description"]:
        if col not in df.columns:
            df[col] = ""
    df["afi_product_description"] = df["afi_product_description"].fillna("")
    df["via_product_description"] = df["via_product_description"].fillna("")

    def generate_versions(row):
        desc_a = row["afi_product_description"]
        desc_b = row["via_product_description"]

        result_text = generate_description_ollama(
            desc_a=desc_a,
            desc_b=desc_b,
            model=model,
            system_prompt=system_prompt,
            prompt_template=prompt_template
        )

        # Parse into labeled columns
        parts = {"Creative": "", "Professional": "", "General": ""}
        for line in result_text.split("\n"):
            if ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                if key in parts:
                    parts[key] = value
        return pd.Series(parts)

    enriched_df = df.join(df.progress_apply(generate_versions, axis=1))
    return enriched_df
