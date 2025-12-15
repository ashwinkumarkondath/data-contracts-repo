import json
import yaml
import sys
import argparse
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
    parser = argparse.ArgumentParser(description="Validate a YAML file against a JSON Schema.")
    # Define arguments for the YAML file and the Schema file
    parser.add_argument("yaml_file", type=str, help="Path to the YAML file to validate (e.g., datacontract.yaml)")
    args = parser.parse_args()
#   yaml_file = "contracts/odcs_template.yaml"
    schema_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "yaml-validation.json")
#   schema_file = "mapping/yaml-validation.json"
    # Pass the command-line argument (YAML file) and the fixed path (Schema file)
    if not validate_yaml_with_schema(args.yaml_file, schema_file):
        exit(1)
