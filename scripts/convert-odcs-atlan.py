import yaml
import json
import os
import sys

# ---------------- YAML helpers ----------------

def str_representer(dumper, data):
    if isinstance(data, str):
        data = data.rstrip()
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

yaml.add_representer(str, str_representer)

# ---------------- Path helpers ----------------

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
            current.setdefault(key, [])
            pos = idx if idx is not None else len(current[key])
            while len(current[key]) <= pos:
                current[key].append({})
            current = current[key][pos]
        else:
            current.setdefault(key, {})
            current = current[key]

    last = parts[-1]
    is_list = last.endswith("[]")
    key = last.replace("[]", "")

    if is_list:
        current.setdefault(key, [])
        if isinstance(value, list):
            current[key].extend(value)
        else:
            current[key].append(value)
    else:
        current[key] = value

# ---------------- Value handlers ----------------

def handle_new_value(val, new_val, default_val):
    if isinstance(new_val, dict):
        if val in new_val:
            return new_val[val]
        return default_val
    if new_val is not None:
        return new_val
    return val

def handle_case_fun(case_fun, val):
    if case_fun:
        case_fun = case_fun.lower()
    if case_fun == "lower":
        return val.lower()
    if case_fun == "upper":
        return val.upper()
    return val

# ---------------- Contract builder ----------------

def build_contract(asset, mappings):
    """Build Atlan contract for a single asset (EXCLUDING SLA)"""
    contract = {}

    for m in mappings:

        # SLA is handled separately
        if m["ODCS_Path"].startswith("slaProperties"):
            continue

        src_path = m["ODCS_Path"]
        dst_path = m["Atlan_Path"]
        new_val = m.get("New_Value")
        case_fun = m.get("Case_Func")
        default_val = m.get("Default_Value")
        level = m.get("Level")
        json_index = m.get("Index")
        tag = m.get("Tag")

        json_index = int(json_index) if json_index is not None else None

        # Tag handling
        if tag == "tag_name" and "tags" in src_path:
            tags = asset.get("tags", [])
            asset["tags"] = [{"name": t} for t in tags]
            continue

        # Table-level quality
        if level == "table" and "quality" in src_path:
            for table in asset.get("schema", []):
                for q in table.get("quality", []):
                    final_val = handle_new_value(q, new_val, default_val)
                    final_value = handle_case_fun(case_fun, final_val)
                    contract.setdefault("custom_metadata", {}) \
                            .setdefault("Metadata", {})["quality"] = final_value
            continue


        results = get_value(asset, src_path)
        if not results:
            continue

        if json_index is not None:
            if json_index < len(results):
                val, odcs_idx = results[json_index]
                final_val = handle_new_value(val, new_val, default_val)
                final_value = handle_case_fun(case_fun, final_val)
                set_value(contract, dst_path, final_value, json_index)
        else:
            for val, odcs_idx in results:
                final_val = handle_new_value(val, new_val, default_val)
                final_value = handle_case_fun(case_fun, final_val)
                set_value(contract, dst_path, final_value, odcs_idx)

    return contract

# ---------------- SLA processor ----------------

def process_sla(odcs, mappings, contracts_by_asset):
    sla_rules = odcs.get("slaProperties", [])
    if not sla_rules:
        return

    sla_mapping = next(
        (m for m in mappings if m.get("ODCS_Path") == "slaProperties[]"),
        None
    )
    if not sla_mapping:
        return

    dst_path = sla_mapping["Atlan_Path"]

    # Group SLA by asset
    sla_by_asset = {}
    for rule in sla_rules:
        asset_name = rule.get("element")
        prop = rule.get("property")
        val = rule.get("value")

        if not asset_name or not prop:
            continue

        sla_by_asset.setdefault(asset_name, {})
        sla_by_asset[asset_name][prop] = val

    # Write SLA into correct asset
    for asset_name, sla_dict in sla_by_asset.items():
        if asset_name not in contracts_by_asset:
            continue
        set_value(contracts_by_asset[asset_name], dst_path, sla_dict)

# ---------------- Config extractor ----------------

def extract_and_append_config(input_yaml_path, table_name, q_name, conn_name, output_config_path='config.yaml'):
    if not q_name or not conn_name:
        return

    parts = q_name.split('/')
    if len(parts) < 5:
        return

    qualified_name = '/'.join(parts[:3])
    database = parts[3]
    schema = parts[4]

    new_entry = {
        f"data_source {table_name.lower()}": {
            'type': 'bigquery',
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
            existing = yaml.safe_load(f) or {}
    else:
        existing = {}

    existing.update(new_entry)

    with open(output_config_path, 'w') as f:
        yaml.dump(existing, f)

# ---------------- Runner ----------------

def run(odcs_file, mapping_file, output_dir="data_contracts_output"):
    os.makedirs(output_dir, exist_ok=True)

    with open(odcs_file, "r", encoding="utf-8") as f:
        odcs_content = yaml.safe_load(f)

    with open(mapping_file, "r", encoding="utf-8") as f:
        mappings = json.load(f)["mappings"]

    assets = odcs_content if isinstance(odcs_content, list) else [odcs_content]
    contracts_by_asset = {}
    for asset in assets:
        for table in asset.get("schema", []):
            table_name = table.get("name", "unknown")
            q_name = table.get("physicalName")
            conn_name = table.get("connection_name")

            asset_root = {
                "schema": [table],
                **{k: v for k, v in asset.items() if k != "schema"}
            }

            data_prod = asset_root.get("dataProduct")

            contract = build_contract(asset_root, mappings)
            contracts_by_asset[table_name] = contract

            extract_and_append_config(odcs_file, table_name, q_name, conn_name)

    for asset in assets:
        process_sla(asset, mappings, contracts_by_asset)

    for asset_name, contract in contracts_by_asset.items():
        if data_prod:
            f_path = os.path.join(output_dir, data_prod)
            path = os.path.join(f_path, f"{asset_name}.yml")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(contract, f, sort_keys=False)
            print(f"Generated: {path}")
        else:
            print(f"Data Product is missing from the odcs - {asset_name}")

# ---------------- Main ----------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate a YAML file against a JSON Schema.")
    parser.add_argument("yaml_file", type=str, help="Path to the YAML file to validate (e.g., datacontract.yaml)")
    args = parser.parse_args()
    odcs = args.yaml_file
    mapping = "mapping/mappings.json"
    run(odcs, mapping)
