import yaml
import json
import os
import sys

def get_value(node, path):
    """Get values from a nested dict/list using path with [] notation"""
    parts = path.split(".")
    return _extract(node, parts, None)

def _extract(current, parts, idx):
    if not parts:
        return [(current, idx)]
    part = parts[0]
    key = part.replace("[]", "")
    results = []

    if isinstance(current, list):
        for i, item in enumerate(current):
            if isinstance(item, dict) and key in item:
                results.extend(_extract(item[key], parts[1:], i))
        return results
    if isinstance(current, dict) and key in current:
        return _extract(current[key], parts[1:], idx)
    return []

def set_value(target, path, value, idx=None):
    """Set a value in a nested dict/list using path with [] notation"""
    parts = path.split(".")
    current = target
    for p in parts[:-1]:
        is_list = p.endswith("[]")
        key = p.replace("[]", "")
        if is_list:
            if key not in current:
                current[key] = []
            pos = idx if idx is not None else len(current[key])
            while len(current[key]) <= pos:
                current[key].append({})
            current = current[key][pos]
        else:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
    last = parts[-1]
    is_list = last.endswith("[]")
    key = last.replace("[]", "")
    if is_list:
        if key not in current:
            current[key] = []
        if isinstance(value, list):
            current[key].extend(value)
        else:
            current[key].append(value)
    else:
        current[key] = value

def build_contract(asset, mappings):
    """Build Atlan contract for a single asset (excluding SLA)"""
    contract = {}
    for m in mappings:
        # Skip SLA mappings here
        if m["ODCS_Path"].startswith("slaProperties[]."):
            continue

        src_path = m["ODCS_Path"]
        dst_path = m["Atlan_Path"]
        new_val = m.get("New_Value")
        default_val =m.get("Default_Value")
        level = m.get("Level")
        json_index = m.get("Index")
        json_index = int(json_index) if json_index is not None else None

        if level == "column" and "quality" in src_path:
            for table in asset.get("schema", []):
                for col in table.get("properties", []):
                    col_name = col.get("name")
                    if "quality" in col:
                        for q in col["quality"]:
                            # inject column name into quality
                            q_copy = dict(q)  # avoid mutating original
                            q_copy["column"] = col_name
                            final_val = handle_new_value(q_copy, new_val, default_val)
                            set_value(contract, dst_path, final_val)
            continue

        # Table-level quality
        if level == "table" and "quality" in src_path:
            for table in asset.get("schema", []):
                for q in table.get("quality", []):
                    final_val = handle_new_value(q, new_val, default_val)
                    set_value(contract, dst_path, final_val)
            continue

        results = get_value(asset, src_path)
        if not results:
            continue

        if json_index is not None:
            if json_index < len(results):
                val, odcs_idx = results[json_index]
            else:
                continue
            final_val = handle_new_value(val, new_val, default_val)
            set_value(contract, dst_path, final_val, json_index)
        else:
            for val, odcs_idx in results:
                final_val = handle_new_value(val, new_val, default_val)
                set_value(contract, dst_path, final_val, odcs_idx)
    return contract

def handle_new_value(val, new_val, default_val):
    """Handle the New_Value transformation if it's a dictionary"""
    if isinstance(new_val, dict):
        # If new_val is a dictionary, check if the value exists in the dict
        if val in new_val:
            return new_val[val]
        elif default_val is not None:
            return default_val
        else:
            return None
    elif new_val is not None:
        # If new_val is a direct value (not a dictionary)
        return new_val
    return val  # Default: return the original value if no mapping is found

def process_sla(odcs, mappings, contracts_by_asset):
    """Process SLA rules and append to corresponding assets"""
    sla_rules = odcs.get("slaProperties", [])
    default_element = odcs.get("slaDefaultElement")

    sla_mappings = [m for m in mappings if m["ODCS_Path"].startswith("slaProperties[].")]

    if not sla_mappings:
        return

    for rule in sla_rules:
        # Determine asset
        if rule.get("element"):
            asset_name = rule["element"].split(".")[0]
        else:
            if not default_element:
                continue
            asset_name = default_element.split(".")[0]

        if asset_name not in contracts_by_asset:
            continue

        # Ensure SLA list exists
        if "sla" not in contracts_by_asset[asset_name] or not isinstance(contracts_by_asset[asset_name]["sla"], list):
            contracts_by_asset[asset_name]["sla"] = []

        # Build SLA object
        sla_obj = {}
        for m in sla_mappings:
            src_key = m["ODCS_Path"].replace("slaProperties[].", "")
            dst_path = m["Atlan_Path"].replace("sla.", "")

            if src_key in rule:
                val = m.get("New_Value") if m.get("New_Value") is not None else rule[src_key]
                parts = dst_path.split(".")
                cur = sla_obj
                for p in parts[:-1]:
                    if p not in cur or not isinstance(cur[p], dict):
                        cur[p] = {}
                    cur = cur[p]
                cur[parts[-1]] = val

        contracts_by_asset[asset_name]["sla"].append(sla_obj)

def run(odcs_file, mapping_file, output_dir="data_contracts"):
    os.makedirs(output_dir, exist_ok=True)

    with open(odcs_file, "r", encoding="utf-8") as f:
        odcs_content = yaml.safe_load(f)

    with open(mapping_file, "r", encoding="utf-8") as f:
        mapping_json = json.load(f)
        mappings = mapping_json["mappings"]

    # Ensure we have a list of assets
    assets = odcs_content if isinstance(odcs_content, list) else [odcs_content]

    contracts_by_asset = {}

    for asset in assets:
        schema_list = asset.get("schema", [])
        for table in schema_list:
            table_name = table.get("name", "unknown")
            asset_root = {"schema": [table], **{k: v for k, v in asset.items() if k != "schema"}}
            contract = build_contract(asset_root, mappings)
            contracts_by_asset[table_name] = contract

    # Process SLA after building contracts
    for asset in assets:
        process_sla(asset, mappings, contracts_by_asset)

    # Write YAML files
    for asset_name, contract in contracts_by_asset.items():
        output_path = os.path.join(output_dir, f"{asset_name}.yml")
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(contract, f, sort_keys=False)
        print(f"Generated: {output_path}")

def extract_and_append_config(input_yaml_path, output_config_path='config.yaml'):
    # Load the input YAML
    with open(input_yaml_path, 'r') as f:
        data = yaml.safe_load(f)

    # Extract fields
    physical_name = data.get('physicalName', '')
    connection_name = data.get('connection_name', '')
    data_source = data.get('schema', [{}])[0].get('name', '')

    if not physical_name or not connection_name:
        print(f"Missing 'physicalName' or 'connection_name' in {input_yaml_path}")
        return

    # Parse physicalName
    # Example: default/bigquery/1751545061/sturdy-tuner-464808-c6/sales_demo/email_events
    parts = physical_name.split('/')
    if len(parts) < 5:
        print(f"Unexpected format for physicalName: {physical_name}")
        return

    qualified_name = '/'.join(parts[:3])  # default/bigquery/1751545061
    database = parts[3]                   # sturdy-tuner-464808-c6
    schema = parts[4]                     # sales_demo

    data_source_val = f"data_source {data_source}"

    new_entry = {
       data_source_val : {
            'type': 'bigquery',
            'connection': {
                'name': connection_name,
                'qualified_name': qualified_name
            },
            'database': database,
            'schema': schema
        }
    }

    # Load existing config.yaml if it exists
    if os.path.exists(output_config_path):
        with open(output_config_path, 'r') as f:
            try:
                existing_config = yaml.safe_load(f) or {}
            except yaml.YAMLError:
                existing_config = {}
    else:
        existing_config = {}

    # Append or update entry
    existing_config.update(new_entry)

    # Write back to config.yaml
    with open(output_config_path, 'w') as f:
        yaml.dump(existing_config, f, default_flow_style=False)

    print(f"Updated config written to {output_config_path}")

if __name__ == "__main__":
    '''if len(sys.argv) != 3:
        print("Usage: python conversion.py odcs_template.yaml mapping.json")
        sys.exit(1)'''
    odcs = "odcs_new.yml"
    mapping = "mapping_new.json"
    #run(sys.argv[1], sys.argv[2])
    extract_and_append_config(odcs)
    run(odcs, mapping)

 
