import pandas as pd
from tqdm import tqdm
import json
from src.utils import generate_description_ollama

# Ensure tqdm integrates nicely with pandas
tqdm.pandas()

def gold_enrich(df: pd.DataFrame, model: str, system_prompt: str, prompt_template: str) -> pd.DataFrame:
    """
    Enriches a DataFrame of AFI Storis products by generating creative, professional,
    and general product descriptions using an Ollama model.
    """

    def generate_versions(row):
        # Safely extract fields from dataset (default to empty strings if missing)
        data = {
            "item_name": row.get("item_name", ""),
            "item_series_name": row.get("item_series_name", ""),
            "color": row.get("color", ""),
            "consumer_assembly": row.get("consumer_assembly", ""),
            "detailed_description": row.get("detailed_description", ""),
            "friendly_description": row.get("friendly_description", ""),
            "item_code": row.get("item_code", ""),
            "item_general_description": row.get("item_general_description", ""),
            "seo_features_and_keywords": row.get("seo_features_and_keywords", ""),
            "homestore_product_line": row.get("homestore_product_line", ""),
            "import_domestic": row.get("import_domestic", "")
        }

        # Fill the prompt template with row data
        user_prompt = prompt_template.format(**data)

        # Call your Ollama generation function
        try:
            result_text = generate_description_ollama(
                model=model,
                system_prompt=system_prompt,
                prompt_template=user_prompt
            )
        except Exception as e:
            print(f"Error generating description for item {row.get('item_name', 'UNKNOWN')}: {e}")
            return pd.Series({"Creative": "", "Professional": "", "General": ""})

        # Try to parse the result if it's valid JSON
        try:
            result_json = json.loads(result_text)
            return pd.Series({
                "Creative": result_json.get("creative_version", ""),
                "Professional": result_json.get("professional_version", ""),
                "General": result_json.get("general_version", "")
            })
        except json.JSONDecodeError:
            # Fallback if model returns text instead of JSON
            parts = {"Creative": "", "Professional": "", "General": ""}
            for line in result_text.split("\n"):
                if ":" in line:
                    key, value = line.split(":", 1)
                    key = key.strip().capitalize()
                    value = value.strip()
                    if key in parts:
                        parts[key] = value
            return pd.Series(parts)

    # Apply the generation function to each row with progress tracking
    enriched_df = df.join(df.progress_apply(generate_versions, axis=1))
    return enriched_df
