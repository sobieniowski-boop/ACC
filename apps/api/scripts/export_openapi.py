"""Export OpenAPI schema to JSON file without starting the server.

Usage:
    python -m scripts.export_openapi              # prints to stdout
    python -m scripts.export_openapi schema.json   # writes to file
"""
import json
import sys

from app.main import app


def main() -> None:
    schema = app.openapi()
    output = json.dumps(schema, indent=2, ensure_ascii=False)

    if len(sys.argv) > 1:
        path = sys.argv[1]
        with open(path, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"OpenAPI schema v{schema.get('info', {}).get('version', '?')} "
              f"-> {path} ({len(schema.get('paths', {}))} paths)")
    else:
        print(output)


if __name__ == "__main__":
    main()
