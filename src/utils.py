import pandas as pd
import yaml
from ollama import chat


# --- Config loader ---
def load_config(path: str) -> dict:
    """Load a YAML configuration file (e.g., ollama.yaml)."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


# --- Save DataFrame utility ---
def save_df(df: pd.DataFrame, path: str):
    """Save a DataFrame to CSV without the index."""
    df.to_csv(path, index=False)


# --- Ollama AI call ---
def generate_description_ollama(model: str, system_prompt: str, prompt_template: str, **kwargs) -> str:
    """
    Generate a product description using an Ollama model.

    Args:
        model (str): The Ollama model name (e.g., 'llama3').
        system_prompt (str): The system role instructions.
        prompt_template (str): A text template with placeholders matching kwargs.
        **kwargs: Product data (from each DataFrame row) passed in dynamically.

    Returns:
        str: Model-generated text content.
    """

    # Fill in the placeholders (like {item_name}, {color}, etc.)
    try:
        final_prompt = prompt_template.format(**{k: str(v) if v is not None else "" for k, v in kwargs.items()})
    except KeyError as e:
        missing_field = str(e).strip("'")
        print(f"[WARN] Missing field '{missing_field}' in prompt template. Substituting empty string.")
        final_prompt = prompt_template

    # Send message to Ollama model
    response = chat(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": final_prompt}
        ]
    )

    # Extract and return model's text output
    try:
        return response["message"]["content"].strip()
    except (KeyError, TypeError):
        print("⚠️ Unexpected Ollama response format. Returning raw response.")
        return str(response)
