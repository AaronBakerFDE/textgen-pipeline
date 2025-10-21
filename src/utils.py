import pandas as pd
import yaml
from ollama import chat

# --- Config loader ---
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

# --- Save DataFrame utility ---
def save_df(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)

# --- Ollama AI call ---
def generate_description_ollama(desc_a: str, desc_b: str, model: str, system_prompt: str, prompt_template: str) -> str:
    final_prompt = prompt_template.replace("{{desc_a}}", desc_a).replace("{{desc_b}}", desc_b)
    response = chat(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": final_prompt}
        ]
    )
    return response["message"]["content"].strip()