#!/usr/bin/env python3

# apply_data_contracts.py

# This script expects each contract YAML to include at minimum both:
#  - name
#  - asset_qualified_name

#Usage:
#  python apply_data_contracts.py --dir ./contracts --report ./report.csv [--dry-run] [--certify] [--embed-certify]

#Env:
#  ATLAN_API_KEY (or whichever env AtlanClient expects)
#  ATLAN_BASE_URL
#  (Optional) ATLAN_ADMIN_API_KEY - if present, used only for certification step

#Notes:
# - This version enforces Option 1: each YAML must contain name and asset_qualified_name.
# - It converts dict specs to YAML before calling the SDK.
# - Certification may be done either as a separate step (--certify) or embedded in the
#   initial payload (--embed-certify). Use only one method at a time.

import os
import sys
import glob
import csv
import argparse
import logging
import yaml
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import DataContract, Asset,Table
# DataContractSpec class location can vary across SDKs
try:
    from pyatlan.model.contract import DataContractSpec
except Exception:
    DataContractSpec = None
from pyatlan.errors import ApiError
from pyatlan.model.enums import DataContractStatus

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

def build_spec_from_official_template(yaml_obj, embed_cert=False, template_version="0.0.2"):
    """
    Build a minimal contract spec dict from the official Atlan template shape.
    Requires `name` and `asset_qualified_name` to be present.
    Adds required `kind` and `dataset` fields to satisfy Atlan's bulk API.

    If embed_cert is True, the function will include a certification block in the spec
    so the contract is created already VERIFIED (if the caller has permission).
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
        "template_version": template_version,
        "dataset": dataset if dataset else name,
        "name": name,
        # set top-level status to 'verified' when embedding certification, else default 'draft'
        "status": ("verified" if embed_cert else yaml_obj.get("status", "draft")),
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

    # If embed_cert, include certification block in the spec payload
    if embed_cert:
        spec["certification"] = {"status": "VERIFIED", "message": "Verified by CI via embed-certify"}

    return spec


def contract_exists(client, name, asset_qn):
    try:
        # Some SDK versions don't expose BoolQuery/TermQuery; fall back to simple search if needed
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
        LOG.debug("contract_exists search with DSL failed: %s", e)
        # fallback: fetch a few DataContract via generic search and filter locally
        try:
            res = client.asset.search({})
            for r in res:
                rn = getattr(r, "name", None) or getattr(r, "displayText", None)
                aqn = None
                try:
                    aqn = getattr(r, "assetQualifiedName", None) or (getattr(r, "assets", [None])[0])
                except Exception:
                    aqn = None
                if rn and aqn and str(rn).strip().lower() == str(name).strip().lower() and str(aqn) == str(asset_qn):
                    return r
        except Exception:
            pass
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
# Version extraction helpers
# ---------------------

def get_contract_version(contract_obj):
    """
    Safely extract the version number of a DataContract asset object.
    Tries common attribute names and attribute dicts.
    """
    if not contract_obj:
        return None

    for attr in ["version", "data_contract_version", "dataContractVersion", "contractVersion", "latestVersion"]:
        if hasattr(contract_obj, attr):
            v = getattr(contract_obj, attr)
            if v not in (None, "", {}):
                return v

    # Attribute dict fallback
    attrs = getattr(contract_obj, "attributes", {})
    if isinstance(attrs, dict):
        for key in ["version", "dataContractVersion", "data_contract_version"]:
            if key in attrs and attrs[key] not in (None, ""):
                return attrs[key]

    # Last resort: inspect __dict__ for keys containing 'version'
    try:
        d = getattr(contract_obj, "__dict__", {}) or {}
        for k, v in d.items():
            if "version" in k.lower() and v:
                return v
    except Exception:
        pass

    return None

def _add_apply_metadata_to_spec_yaml(spec_yaml: str, runner_name="github-actions"):
    """
    Inject tiny metadata so Atlan treats the spec as changed and creates a new version.
    Uses timezone-aware UTC datetimes.
    """
    try:
        obj = yaml.safe_load(spec_yaml) or {}
    except Exception:
        if isinstance(spec_yaml, dict):
            obj = spec_yaml
        else:
            raise
    obj["applied_at"] = datetime.now(timezone.utc).isoformat()
    obj["applied_by"] = runner_name
    return yaml.safe_dump(obj, sort_keys=False)


def certify_with_client(c_client, guid, status="VERIFIED", message="Verified via CI"):
    """
    Simple wrapper using provided AtlanClient to attempt certification update.
    Returns True on success.
    """
    if not guid:
        LOG.error("certify_with_client called with empty GUID")
        return False
    try:
        contract = c_client.asset.get_by_guid(guid, asset_type=DataContract)
    except Exception:
        try:
            contract = c_client.asset.get_by_guid(guid)
        except Exception as e:
            LOG.exception("Failed to fetch contract guid=%s for certification: %s", guid, e)
            return False
    try:
        contract.certification = {"status": status, "message": message}
        c_client.asset.save(contract)
        LOG.info("Certification applied via client.asset.save using certification attr for guid=%s", guid)
        return True
    except Exception as e:
        LOG.debug("Setting contract.certification failed: %s", e)
    try:
        setattr(contract, "certificateStatus", status)
        setattr(contract, "certificateMessage", message)
        c_client.asset.save(contract)
        LOG.info("Certification applied via certificateStatus/certificateMessage for guid=%s", guid)
        return True
    except Exception as e:
        LOG.exception("All certification attempts failed for guid=%s: %s", guid, e)
        return False

def get_contract_version_by_guid(client, guid):
    """
    Helper to fetch contract by GUID and extract version using get_contract_version()
    """
    try:
        c = client.asset.get_by_guid(guid, asset_type=DataContract)
    except Exception:
        try:
            c = client.asset.get_by_guid(guid)
        except Exception as e:
            LOG.debug("get_contract_version_by_guid failed to fetch guid=%s: %s", guid, e)
            return None
    return get_contract_version(c)

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
    if result.get("status") in ("CREATED", "UPDATED") and not dry_run and force_certify:
        # try to resolve guid if missing
        guid = result.get("guid")
        if not guid:
            try:
                parsed = yaml.safe_load(base_yaml) if isinstance(base_yaml, str) else {}
                name = parsed.get("name")
            except Exception:
                name = None
            guid = _resolve_guid_quick(name, asset_qualified_name)

        if guid:
            admin_key = os.environ.get("ATLAN_ADMIN_API_KEY")
            if admin_key:
                admin_client = AtlanClient(api_key=admin_key, base_url=os.environ.get("ATLAN_BASE_URL"))
                ok = certify_with_client(admin_client, guid, status="VERIFIED", message=f"Verified by {runner_name} on {datetime.now(timezone.utc).isoformat()}")
            else:
                ok = certify_with_client(client, guid, status="VERIFIED", message=f"Verified by {runner_name} on {datetime.now(timezone.utc).isoformat()}")
            if ok:
                result["message"] = (result.get("message", "") + " ; Certified VERIFIED")
            else:
                result["message"] = (result.get("message", "") + " ; Certification FAILED")
        else:
            LOG.error("Could not determine contract GUID for certification (name=%s asset=%s). Certification skipped.", name, asset_qualified_name)

    # Attempt to fetch and include version info
    if result.get("guid"):
        ver = get_contract_version_by_guid(client, result.get("guid"))
        if ver:
            result["version"] = ver
    return result

# ---------------------
# Create / Update (existing functions)
# ---------------------
from pyatlan.errors import ApiError
from pyatlan.model.assets import Table, DataContract
try:
    from pyatlan.model.contract import DataContractSpec
    from pyatlan.model.enums import DataContractStatus
except Exception:
    DataContractSpec = None
    DataContractStatus = None


def create_or_update_contract(
    client,
    contract_spec,
    asset_qualified_name: str,
    dry_run: bool = False,
    **kwargs,  # swallow extra flags like embed_certify
):
    """
    Idempotent create-or-update:
    - If the asset has no contract yet -> create using DataContract.creator
    - If it already has a contract      -> update using DataContract.updater
    """

    # Derive the contract name from spec (YAML or dict)
    spec_name = None
    if isinstance(contract_spec, dict):
        spec_name = contract_spec.get("name")
    elif isinstance(contract_spec, str):
        try:
            parsed = yaml.safe_load(contract_spec) or {}
            spec_name = parsed.get("name")
        except Exception:
            spec_name = None

    # Load the table WITH relationships so we can see data_contract_latest
    try:
        table = client.asset.get_by_qualified_name(
            asset_type=Table,
            qualified_name=asset_qualified_name,
            ignore_relationships=False,
        )
    except ApiError as e:
        return {
            "status": "FAILED",
            "message": f"Failed to load asset {asset_qualified_name}: {e}",
        }

    latest_contract = getattr(table, "data_contract_latest", None)

    # Always convert whatever spec we built into YAML string
    spec_yaml = _spec_to_yaml(contract_spec)

    # ---------- CREATE path: no contract yet ----------
    if latest_contract is None:
        if dry_run:
            return {
                "status": "DRYRUN-CREATE",
                "message": f"Would create contract for asset {asset_qualified_name}",
            }

        try:
            final_yaml = spec_yaml
            if DataContractSpec is not None:
                try:
                    dcs = DataContractSpec.from_yaml(spec_yaml)
                    if kwargs.get("embed_certify") and DataContractStatus is not None:
                        dcs.status = DataContractStatus.VERIFIED
                    final_yaml = dcs.to_yaml()
                except Exception:
                    final_yaml = spec_yaml

            contract = DataContract.creator(
                asset_qualified_name=asset_qualified_name,
                contract_spec=final_yaml,
            )
            resp = client.asset.save(contract)
            return {
                "status": "CREATED",
                "guid": getattr(resp, "guid", None),
                "message": "Created contract via DataContract.creator",
            }
        except Exception as e:
            LOG.exception("Top-level create failed")
            return {
                "status": "FAILED",
                "message": f"Top-level create failed: {e}",
            }

    # ---------- UPDATE path: contract already exists ----------
    if dry_run:
        return {
            "status": "DRYRUN-UPDATE",
            "message": (
                f"Would update existing contract for asset {asset_qualified_name}"
            ),
        }

    try:
        # 1) Fetch the FULL contract by GUID to get a real qualified_name
        latest_guid = getattr(latest_contract, "guid", None) or getattr(
            latest_contract, "id", None
        )
        if not latest_guid:
            return {
                "status": "FAILED",
                "message": "Existing contract found but has no guid – cannot update.",
            }

        try:
            full_contract = client.asset.get_by_guid(
                latest_guid, asset_type=DataContract
            )
        except Exception:
            full_contract = client.asset.get_by_guid(latest_guid)

        contract_qn = (
            getattr(full_contract, "qualified_name", None)
            or getattr(full_contract, "qualifiedName", None)
        )
        if not contract_qn:
            return {
                "status": "FAILED",
                "message": "Existing contract has no qualified_name – cannot update.",
            }

        # 2) Build updated spec YAML (optionally via DataContractSpec)
        final_yaml = spec_yaml
        if DataContractSpec is not None:
            try:
                dcs = DataContractSpec.from_yaml(spec_yaml)
                if kwargs.get("embed_certify") and DataContractStatus is not None:
                    dcs.status = DataContractStatus.VERIFIED
                final_yaml = dcs.to_yaml()
            except Exception:
                final_yaml = spec_yaml

        # 3) Choose a name for updater: spec_name > full_contract.name > fallback
        update_name = (
            spec_name
            or getattr(full_contract, "name", None)
            or getattr(full_contract, "displayText", None)
            or "Data Contract"
        )

        updater = DataContract.updater(
            qualified_name=contract_qn,
            name=update_name,
        )
        updater.data_contract_spec = final_yaml

        resp = client.asset.save(updater)
        return {
            "status": "UPDATED",
            "guid": getattr(resp, "guid", None),
            "message": "Updated existing contract via DataContract.updater",
        }

    except Exception as e:
        LOG.exception("Failed to update contract")
        return {
            "status": "FAILED",
            "message": f"Failed updating existing contract: {e}",
        }

# ---------------------
# Quick GUID resolution helper (short-window search)
# ---------------------

def _resolve_guid_quick(name, asset_qn):
    # Prefer searching via asset relationships (fast when available)
    try:
        try:
            asset = client.asset.get_by_qualified_name(asset_qn, asset_type=Asset, ignore_relationships=False)
        except TypeError:
            asset = client.asset.get_by_qualified_name(asset_qn, asset_type=Asset, ignore_relationships=False)
        except Exception as e:
            LOG.debug("asset fetch for quick resolution failed: %s", e)
            asset = None
        if asset:
            latest = getattr(asset, "data_contract_latest", None) or getattr(asset, "dataContractLatest", None)
            if latest:
                return getattr(latest, "guid", None) or getattr(latest, "id", None)
    except Exception as e:
        LOG.debug("quick asset-based resolution failed: %s", e)

    # Fallback: short search by assetQualifiedName
    try:
        from pyatlan.model.search import IndexSearchRequest, BoolQuery, TermQuery
        where = BoolQuery(must=[TermQuery(field="assetQualifiedName.keyword", value=asset_qn)])
        req = IndexSearchRequest(where=where, dsl={"size": 5})
        res = client.asset.search(req, asset_type=DataContract)
        for r in res:
            g = getattr(r, "guid", None) or getattr(r, "id", None)
            if g:
                return g
    except Exception as e:
        LOG.debug("quick index search failed: %s", e)
    return None

# ---------------------
# Worker
# ---------------------

def process_file(path, client, dry_run=False, certify=False, embed_cert=False):
    LOG.info("Processing %s", path)
    try:
        y = load_yaml(path)

        # Enforce Option 1: require name and asset_qualified_name
        name = y.get("name")
        asset_qn = y.get("asset_qualified_name") or y.get("assetQualifiedName") or y.get("qualified_name") or y.get("qualifiedName")
        if not name or not asset_qn:
            return {"file": path, "status": "FAILED", "message": "YAML must include both 'name' and 'asset_qualified_name'. Please update the file."}

        # Build spec from the official template shape
        spec = build_spec_from_official_template(y, embed_cert=embed_cert)

        # Use wrapper that injects applied_at and optionally certifies after create/update
        # If embed_cert is True, the certification is included in the payload, so we do not request separate certify
        result = create_or_update_and_certify(client, spec, asset_qn, dry_run=dry_run, force_certify=certify and not embed_cert)
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
    p.add_argument("--certify", action="store_true", help="Set certification.status=VERIFIED after create/update (separate step)")
    p.add_argument("--embed-certify", action="store_true", help="Embed certification in the initial create payload (useful if runner has cert privileges)")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.dir, "*.yaml")) + glob.glob(os.path.join(args.dir, "*.yml")))
    if not files:
        LOG.error("No YAML files found in %s", args.dir)
        sys.exit(2)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_file, f, client, args.dry_run, args.certify, args.embed_certify): f for f in files}
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
