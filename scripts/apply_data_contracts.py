#!/usr/bin/env python3

# apply_data_contracts.py

# This script expects each contract YAML to include at minimum both:
#  - name
#  - asset_qualified_name

#Usage:
#  python apply_data_contracts.py --dir ./contracts --report ./report.csv [--dry-run] [--certify]

#Env:
#  ATLAN_API_KEY (or whichever env AtlanClient expects)
#  ATLAN_BASE_URL

#Notes:
# - This version enforces Option 1: each YAML must contain name and asset_qualified_name.
# - It keeps robust creation/update paths and converts dict specs to YAML before calling the SDK.
# - Certification is performed as a separate step AFTER create/update when --certify is provided.

import os
import sys
import glob
import csv
import argparse
import logging
import yaml
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import DataContract, Asset
# DataContractSpec class location can vary across SDKs
try:
    from pyatlan.model.contract import DataContractSpec
except Exception:
    DataContractSpec = None

# instantiate Atlan client (relies on env vars or .env if you use dotenv)
client = AtlanClient()

# simple logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("apply_contracts")

# ---------------------
# Utility functions
# ---------------------

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_spec_from_official_template(yaml_obj):
    """
    Build a minimal contract spec dict from the official Atlan template shape.
    Requires `name` and `asset_qualified_name` to be present.
    Adds required `kind` and `dataset` fields to satisfy Atlan's bulk API.
    """
    name = yaml_obj.get("name")
    asset_qn = (
        yaml_obj.get("asset_qualified_name")
        or yaml_obj.get("assetQualifiedName")
        or yaml_obj.get("qualified_name")
        or yaml_obj.get("qualifiedName")
    )
    dataset = yaml_obj.get("dataset")

    if not name or not asset_qn:
        raise ValueError("YAML must include both 'name' and 'asset_qualified_name'")

    spec = {
        "kind": "DataContract",
        "dataset": dataset if dataset else name,
        "name": name,
        "description": yaml_obj.get("description", ""),
        "assets": [asset_qn],
        "assetQualifiedName": asset_qn,
    }

    # Optional sections
    if "custom_metadata" in yaml_obj:
        spec["extra_properties"] = yaml_obj["custom_metadata"]

    if "expectations" in yaml_obj:
        spec["expectations"] = yaml_obj["expectations"]

    if "sla" in yaml_obj:
        spec["sla"] = yaml_obj["sla"]

    # owners handled separately by principals_map, so not included here
    return spec


def contract_exists(client, name, asset_qn):
    try:
        from pyatlan.model.search import IndexSearchRequest, BoolQuery, TermQuery
        where = BoolQuery(must=[
            TermQuery(field="name.keyword", value=name),
            TermQuery(field="assetQualifiedName.keyword", value=asset_qn)
        ])
        req = IndexSearchRequest(where=where, dsl={"size": 3})
        res = client.asset.search(req, asset_type=DataContract)
        for r in res:
            return r
    except Exception as e:
        LOG.debug("contract_exists search failed: %s", e)
    return None


def _spec_to_yaml(contract_spec):
    # already a YAML string
    if isinstance(contract_spec, str):
        return contract_spec
    # if it's a dict, dump to YAML
    if isinstance(contract_spec, dict):
        return yaml.safe_dump(contract_spec, sort_keys=False)
    # Try SDK helpers
    if hasattr(contract_spec, "to_yaml"):
        try:
            return contract_spec.to_yaml()
        except Exception:
            pass
    if hasattr(contract_spec, "to_dict"):
        try:
            return yaml.safe_dump(contract_spec.to_dict(), sort_keys=False)
        except Exception:
            pass
    try:
        return yaml.safe_dump(contract_spec.__dict__, sort_keys=False)
    except Exception as e:
        LOG.exception("Could not convert contract_spec to YAML: %s", e)
        raise


# ---------------------
# Versioning + Certification helpers
# ---------------------

def _add_apply_metadata_to_spec_yaml(spec_yaml: str, runner_name="github-actions"):
    """
    Inject tiny metadata so Atlan treats the spec as changed and creates a new version.
    """
    try:
        obj = yaml.safe_load(spec_yaml) or {}
    except Exception:
        if isinstance(spec_yaml, dict):
            obj = spec_yaml
        else:
            raise
    obj["applied_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    obj["applied_by"] = runner_name
    return yaml.safe_dump(obj, sort_keys=False)


def certify_contract(client, contract_guid: str, status="VERIFIED", message="Verified via CI"):
    """
    Mark a contract as certified/verified. Performs a separate update call.
    """
    try:
        contract = client.asset.get_by_guid(contract_guid, asset_type=DataContract)
    except Exception:
        try:
            contract = client.asset.get_by_guid(contract_guid)
        except Exception as e:
            LOG.exception("Failed to fetch contract by guid %s: %s", contract_guid, e)
            return False

    # Try the common shapes
    try:
        contract.certification = {"status": status, "message": message}
        client.asset.save(contract)
        return True
    except Exception:
        pass
    try:
        setattr(contract, "certificateStatus", status)
        setattr(contract, "certificateMessage", message)
        client.asset.save(contract)
        return True
    except Exception as e:
        LOG.exception("Failed to set certification on contract %s: %s", contract_guid, e)
        return False


def get_contract_version(client, contract_guid: str):
    """
    Fetch contract asset and try common fields that indicate version.
    Returns a string or None.
    """
    try:
        contract = client.asset.get_by_guid(contract_guid, asset_type=DataContract)
    except Exception:
        try:
            contract = client.asset.get_by_guid(contract_guid)
        except Exception as e:
            LOG.debug("get_contract_version: could not fetch contract %s: %s", contract_guid, e)
            return None
    # try multiple attribute names
    for attr in ["version", "dataContractVersion", "latestVersion", "contractVersion", "data_contract_version"]:
        v = getattr(contract, attr, None)
        if v:
            return v
    # fallback: inspect dict-like attributes
    try:
        d = contract.__dict__
        for k in d:
            if "version" in k.lower():
                return d[k]
    except Exception:
        pass
    return None


# ---------------------
# Create / Update (wrapper that injects metadata + optionally certify)
# ---------------------

def create_or_update_and_certify(client, contract_spec, asset_qualified_name, dry_run=False, force_certify=False, runner_name="github-actions"):
    # Build YAML string first
    if isinstance(contract_spec, str):
        base_yaml = contract_spec
    else:
        base_yaml = _spec_to_yaml(contract_spec)

    # inject applied_at to force version change
    spec_yaml_with_meta = _add_apply_metadata_to_spec_yaml(base_yaml, runner_name=runner_name)

    # Call the existing create/update path using the YAML string
    result = create_or_update_contract(client, spec_yaml_with_meta, asset_qualified_name, dry_run=dry_run)

    # If created/updated and certification requested, do a separate certify call
    if result.get("status") in ("CREATED", "UPDATED") and result.get("guid") and force_certify and not dry_run:
        ok = certify_contract(client, result["guid"], status="VERIFIED", message=f"Verified by {runner_name} on {datetime.datetime.utcnow().isoformat()}Z")
        if ok:
            result["message"] = (result.get("message", "") + " ; Certified VERIFIED")
        else:
            result["message"] = (result.get("message", "") + " ; Certification FAILED")

    # Attempt to fetch and include version info
    if result.get("guid"):
        ver = get_contract_version(client, result.get("guid"))
        if ver:
            result["version"] = ver
    return result


# ---------------------
# Create / Update (existing functions)
# ---------------------

def create_or_update_contract(client, contract_spec, asset_qualified_name, dry_run=False):
    if isinstance(contract_spec, dict):
        name = contract_spec.get("name")
    else:
        # for YAML strings, attempt to parse to extract name for idempotency
        if isinstance(contract_spec, str):
            try:
                parsed = yaml.safe_load(contract_spec)
                name = parsed.get("name")
            except Exception:
                name = None
        else:
            name = getattr(contract_spec, "name", None)

    existing = contract_exists(client, name, asset_qualified_name)
    if existing:
        if dry_run:
            return {"status": "DRYRUN-UPDATE", "message": f"Would update existing contract {existing.guid}"}
        try:
            desc = None
            if isinstance(contract_spec, dict):
                desc = contract_spec.get("description")
            else:
                # try to parse YAML and extract description
                try:
                    parsed = yaml.safe_load(contract_spec) if isinstance(contract_spec, str) else None
                    desc = parsed.get("description") if parsed else None
                except Exception:
                    desc = None
            if desc:
                existing.description = desc
            extra = None
            if isinstance(contract_spec, dict):
                extra = contract_spec.get("extra_properties")
            else:
                try:
                    parsed = yaml.safe_load(contract_spec) if isinstance(contract_spec, str) else None
                    extra = parsed.get("extra_properties") if parsed else None
                except Exception:
                    extra = None
            if extra:
                try:
                    existing.additional_attributes = extra
                except Exception:
                    pass
            resp = client.asset.save(existing)
            return {"status": "UPDATED", "guid": getattr(resp, "guid", None), "message": "Updated existing contract"}
        except Exception as e:
            return {"status": "FAILED", "message": f"Failed updating contract: {e}"}
    else:
        if dry_run:
            return {"status": "DRYRUN-CREATE", "message": f"Would create contract for asset {asset_qualified_name}"}

        # CREATE path (ensure YAML string passed to SDK)
        try:
            try:
                spec_yaml = contract_spec if isinstance(contract_spec, str) else _spec_to_yaml(contract_spec)
                contract_obj = DataContract.creator(
                    asset_qualified_name=asset_qualified_name,
                    contract_spec=spec_yaml
                )
                resp = client.asset.save(contract_obj)
                return {"status": "CREATED", "guid": getattr(resp, "guid", None), "message": "Created contract via DataContract.creator"}
            except Exception as e_creator:
                LOG.debug("DataContract.creator attempt failed: %s", e_creator)

            # Fallback: try client.data_contract.create if available
            try:
                if hasattr(client, "data_contract") and hasattr(client.data_contract, "create"):
                    if isinstance(contract_spec, dict):
                        resp = client.data_contract.create(contract_spec)
                    else:
                        resp = client.data_contract.create(_spec_to_yaml(contract_spec))
                    return {"status": "CREATED", "guid": getattr(resp, "guid", None), "message": "Created contract via client.data_contract.create"}
            except Exception as e_dc_create:
                LOG.debug("client.data_contract.create attempt failed: %s", e_dc_create)

            raise RuntimeError("No supported create path succeeded for DataContract")

        except Exception as e:
            LOG.exception("Top-level create failed")
            return {"status": "FAILED", "message": f"Top-level create failed: {e}"}


# ---------------------
# Worker
# ---------------------

def process_file(path, client, dry_run=False, certify=False):
    LOG.info("Processing %s", path)
    try:
        y = load_yaml(path)

        # Enforce Option 1: require name and asset_qualified_name
        name = y.get("name")
        asset_qn = y.get("asset_qualified_name") or y.get("assetQualifiedName") or y.get("qualified_name") or y.get("qualifiedName")
        if not name or not asset_qn:
            return {"file": path, "status": "FAILED", "message": "YAML must include both 'name' and 'asset_qualified_name'. Please update the file."}

        # Build spec from the official template shape
        spec = build_spec_from_official_template(y)

        # Use wrapper that injects applied_at and optionally certifies after create/update
        result = create_or_update_and_certify(client, spec, asset_qn, dry_run=dry_run, force_certify=certify)
        return {"file": path, "status": result.get("status"), "message": result.get("message"), "guid": result.get("guid"), "version": result.get("version")}
    except Exception as e:
        LOG.exception("Unhandled error processing %s", path)
        return {"file": path, "status": "FAILED", "message": str(e)}


# ---------------------
# Main
# ---------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", "-d", required=True, help="folder with YAML files")
    p.add_argument("--workers", type=int, default=4, help="concurrent workers")
    p.add_argument("--report", default="report.csv")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--certify", action="store_true", help="Set certification.status=VERIFIED after create/update")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.dir, "*.yaml")) + glob.glob(os.path.join(args.dir, "*.yml")))
    if not files:
        LOG.error("No YAML files found in %s", args.dir)
        sys.exit(2)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_file, f, client, args.dry_run, args.certify): f for f in files}
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            LOG.info("File %s -> %s: %s", res["file"], res["status"], res["message"])

    # write report
    with open(args.report, "w", newline="", encoding="utf-8") as csvf:
        w = csv.DictWriter(csvf, fieldnames=["file", "status", "message", "guid", "version"])
        w.writeheader()
        for r in results:
            w.writerow({"file": r["file"], "status": r["status"], "message": r["message"], "guid": r.get("guid"), "version": r.get("version")})

    LOG.info("Completed. Report saved to %s", args.report)


if __name__ == "__main__":
    main()
