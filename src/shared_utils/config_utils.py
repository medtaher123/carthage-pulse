"""Configuration utilities - env var expansion and loading"""

import os
import re
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def expand_env_vars(value: Any) -> Any:
    """
    Recursively expand ${VAR_NAME} or ${VAR_NAME:-default} environment variables in config values.

    Examples:
        "${OPENROUTER_API_KEY}" → value of OPENROUTER_API_KEY env var
        "prefix-${VAR}" → "prefix-<value>"
    """
    if isinstance(value, str):
        # Replace ${VAR_NAME} with environment variable value
        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else match.group(0)
            return os.getenv(var_name, default_value)

        return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}", replace_var, value)

    elif isinstance(value, dict):
        return {k: expand_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [expand_env_vars(item) for item in value]

    return value


def validate_required_keys(config: dict, required_keys: list[str], section: str = ""):
    """
    Validate that required config keys are not placeholder values.

    Args:
        config: Configuration dict
        required_keys: List of dot-notation keys (e.g., ["llm.api_key", "kafka.bootstrap_servers"])
        section: Context for error messages

    Raises:
        ValueError: If any required key is missing or still has placeholder value
    """
    for key_path in required_keys:
        parts = key_path.split(".")
        value = config
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                value = None
                break

        # Check if value is missing, None, empty, or placeholder
        if not value or str(value).startswith("your-") or str(value).startswith("${"):
            section_str = f" in {section}" if section else ""
            raise ValueError(
                f"Missing or invalid config: {key_path}{section_str}. "
                f"Set it in config/dev.yaml or via environment variable."
            )
