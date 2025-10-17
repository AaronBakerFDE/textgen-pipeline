import pandas as pd
from tqdm import tqdm
from src.utils import generate_description_ollama

tqdm.pandas()

def gold_enrich(df: pd.DataFrame, model: str, system_prompt: str, prompt_template: str) -> pd.DataFrame:
    for col in ["description_a", "description_b"]:
        if col not in df.columns:
            df[col] = ""
    df["description_a"] = df["description_a"].fillna("")
    df["description_b"] = df["description_b"].fillna("")

    def generate_versions(row):
        desc_a = row["description_a"]
        desc_b = row["description_b"]

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
