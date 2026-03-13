"""Eksport specyfikacji OpenAPI do plików JSON i YAML.

Generuje statyczne pliki specyfikacji z instancji FastAPI:
  - apps/api/openapi.json
  - docs/api-spec.yaml

Uruchomienie:
    cd apps/api
    python -m scripts.export_openapi              # JSON + YAML do domyślnych ścieżek
    python -m scripts.export_openapi schema.json   # JSON do podanej ścieżki
"""
from __future__ import annotations

import json
import os
import sys

from app.main import app


def main() -> None:
    schema = app.openapi()
    output = json.dumps(schema, indent=2, ensure_ascii=False)

    api_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # apps/api/
    root_dir = os.path.dirname(os.path.dirname(api_dir))  # repo root

    if len(sys.argv) > 1:
        # Tryb jednoargumentowy: eksport do podanej ścieżki JSON
        path = sys.argv[1]
        with open(path, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"OpenAPI schema v{schema.get('info', {}).get('version', '?')} "
              f"-> {path} ({len(schema.get('paths', {}))} paths)")
    else:
        # Domyślny eksport: JSON + YAML
        json_path = os.path.join(api_dir, "openapi.json")
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"OpenAPI JSON → {json_path}")

        docs_dir = os.path.join(root_dir, "docs")
        os.makedirs(docs_dir, exist_ok=True)
        yaml_path = os.path.join(docs_dir, "api-spec.yaml")

        try:
            import yaml

            with open(yaml_path, "w", encoding="utf-8") as f:
                yaml.dump(schema, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            print(f"OpenAPI YAML → {yaml_path}")
        except ImportError:
            yaml_fallback = yaml_path.replace(".yaml", ".json")
            with open(yaml_fallback, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=2, ensure_ascii=False)
            print(f"PyYAML niedostępny — zapisano JSON → {yaml_fallback}")


if __name__ == "__main__":
    main()
