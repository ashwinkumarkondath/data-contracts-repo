import yaml
import json
import os
import argparse
import sys

def str_representer(dumper, data):
    if isinstance(data, str):
        data = data.rstrip()
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

yaml.add_representer(str, str_representer)

def handle_case_fun(case_fun, val):
    if case_fun is not None:
        case_fun = case_fun.lower()
    if case_fun == 'lower':
        return val.lower()
    elif case_fun == 'upper':
        return val.upper()
    return val

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
        if m["ODCS_Path"].startswith("slaProperties[]."):
            continue

        src_path = m["ODCS_Path"]
        dst_path = m["Atlan_Path"]
        new_val = m.get("New_Value")
        case_fun = m.get("Case_Func")
        default_val = m.get("Default_Value")
        level = m.get("Level")
        json_index = m.get("Index")
        json_index = int(json_index) if json_index is not None else None

        if level == "column" and "quality" in src_path:
            for table in asset.get("schema", []):
                for col in table.get("properties", []):
                    col_name = col.get("name")
                    if "quality" in col:
                        for q in col["quality"]:
                            q_copy = dict(q)
                            q_copy["column"] = col_name
                            final_val = handle_new_value(q_copy, new_val, default_val)
                            final_val1 = handle_case_fun(case_fun, final_val)
                            set_value(contract, dst_path, final_val1)
            continue

        if level == "table" and "quality" in src_path:
            for table in asset.get("schema", []):
                for q in table.get("quality", []):
                    final_val = handle_new_value(q, new_val, default_val)
                    final_val1 = handle_case_fun(case_fun, final_val)
                    set_value(contract, dst_path, final_val1)
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
            final_val1 = handle_case_fun(case_fun, final_val)
            set_value(contract, dst_path, final_val1, json_index)
        else:
            for val, odcs_idx in results:
                final_val = handle_new_value(val, new_val, default_val)
                final_val1 = handle_case_fun(case_fun, final_val)
                set_value(contract, dst_path, final_val1, odcs_idx)
    return contract

def handle_new_value(val, new_val, default_val):
    if isinstance(new_val, dict):
        if val in new_val:
            return new_val[val]
        elif default_val is not None:
            return default_val
        else:
            return None
    elif new_val is not None:
        return new_val
    return val

def process_sla(odcs, mappings, contracts_by_asset):
    sla_rules = odcs.get("slaProperties", [])
    default_element = odcs.get("slaDefaultElement")
    sla_mappings = [m for m in mappings if m["ODCS_Path"].startswith("slaProperties[].")]
    if not sla_mappings:
        return
    for rule in sla_rules:
        asset_name = rule.get("element", default_element).split(".")[0] if (rule.get("element") or default_element) else None
        if not asset_name or asset_name not in contracts_by_asset:
            continue
        if "sla" not in contracts_by_asset[asset_name] or not isinstance(contracts_by_asset[asset_name]["sla"], list):
            contracts_by_asset[asset_name]["sla"] = []
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

    assets = odcs_content if isinstance(odcs_content, list) else [odcs_content]
    contracts_by_asset = {}

    for asset in assets:
        schema_list = asset.get("schema", [])
        for table in schema_list:
            table_name = table.get("name", "unknown")
            q_name = table.get("physicalName", "unknown")
            conn_name = table.get("connection_name", "unknown")
            asset_root = {"schema": [table], **{k: v for k, v in asset.items() if k != "schema"}}
            contract = build_contract(asset_root, mappings)
            contracts_by_asset[table_name] = contract
            extract_and_append_config(odcs, table_name, q_name, conn_name)

    for asset in assets:
        process_sla(asset, mappings, contracts_by_asset)

    for asset_name, contract in contracts_by_asset.items():
        output_path = os.path.join(output_dir, f"{asset_name}.yaml")
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(contract, f, sort_keys=False) 
        print(f"Generated: {output_path}")

def extract_and_append_config(input_yaml_path, table_name, q_name, conn_name, output_config_path='config.yaml'):

    if not q_name or not conn_name:
        print(f"Missing 'physicalName' or 'connection_name' in {input_yaml_path}")
        return

    parts = q_name.split('/')
    if len(parts) < 5:
        print(f"Unexpected format for physicalName: {q_name}")
        return

    qualified_name = '/'.join(parts[:3])
    database = parts[3]
    schema = parts[4]
    type = parts[1]
    data_source_val = f"data_source {table_name.lower()}"

    new_entry = {
       data_source_val : {
            'type': type,
            'connection': {
                'name': conn_name,
                'qualified_name': qualified_name
            },
            'database': database,
            'schema': schema
        }
    }

    if os.path.exists(output_config_path):
        with open(output_config_path, 'r') as f:
            try:
                existing_config = yaml.safe_load(f) or {}
            except yaml.YAMLError:
                existing_config = {}
    else:
        existing_config = {}

    existing_config.update(new_entry)

    with open(output_config_path, 'w') as f:
        yaml.dump(existing_config, f, default_flow_style=False)

    print(f"Updated config written to {output_config_path} for asset {table_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a YAML file against a JSON Schema.")
    parser.add_argument("yaml_file", type=str, help="Path to the YAML file to validate (e.g., datacontract.yaml)")
    args = parser.parse_args()
    odcs = args.yaml_file
    mapping = "mapping/mappings.json"
    run(odcs, mapping)
 
