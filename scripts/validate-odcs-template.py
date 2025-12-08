import json
import yaml
import sys
from jsonschema import validate, Draft7Validator
from jsonschema.exceptions import ValidationError


def validate_yaml_with_schema(yaml_path: str, schema_path: str):
    # Load YAML
    with open(yaml_path, "r") as f:
        yaml_data = yaml.safe_load(f)
    # Load JSON Schema
    with open(schema_path, "r") as f:
        schema = json.load(f)
    # Create validator — useful for getting all errors not just first one
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(yaml_data), key=lambda e: e.path)
    if errors:
        print("\n Validation Failed! Errors:")
        for err in errors:
            print(f"• Path: {'/'.join([str(p) for p in err.path])} -> {err.message}")
        return False
    print("\n YAML is valid against the JSON Schema!")
    return True

if __name__ == "__main__":
    yaml_file = "contracts/odcs_template.yaml"
    schema_file = "mapping/yaml-validation.json"
    ok = validate_yaml_with_schema(yaml_file, schema_file)
    if not ok:
        sys.exit(1)
