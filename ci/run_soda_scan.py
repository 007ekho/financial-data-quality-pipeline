# """
# ci/run_soda_scan.py
# Runs a Soda Core 4.x contract verification.
# Usage: python3 ci/run_soda_scan.py <data_source_yaml> <contract_path>
# Exits 0 if all checks pass, 1 if any fail.
# """
# import sys
# from soda_core.contracts import verify_contract_locally

# data_source_yaml = sys.argv[1]
# contract_path    = sys.argv[2]

# result = verify_contract_locally(
#     data_source_file_path=data_source_yaml,
#     contract_file_path=contract_path,
#     publish=False,
# )

# if not result.is_ok:
#     print(f"❌ Contract failed: {contract_path}")
#     sys.exit(1)

# print(f"✅ Contract passed: {contract_path}")
# sys.exit(0)





"""
ci/run_soda_scan.py
Runs a Soda Core 4.x contract verification with verbose output.
Usage: python3 ci/run_soda_scan.py <data_source_yaml> <contract_path>
"""
import sys
from soda_core.contracts import verify_contract_locally

data_source_yaml = sys.argv[1]
contract_path    = sys.argv[2]

print(f"Running Soda scan...")
print(f"Data source: {data_source_yaml}")
print(f"Contract: {contract_path}")
print("-" * 60)

result = verify_contract_locally(
    data_source_file_path=data_source_yaml,
    contract_file_path=contract_path,
    publish=False,
)

# Print all check results
for cr in (result.contract_verification_results or []):
    for check in (cr.check_results or []):
        outcome = check.outcome.name.lower() if hasattr(check.outcome, "name") else str(check.outcome).lower()
        check_id = getattr(check.check, "identity", "unknown")
        print(f"[{outcome.upper()}] {check_id}")
        if hasattr(check, "diagnostic_lines"):
            for line in check.diagnostic_lines:
                print(f"    {line}")

print("-" * 60)

if not result.is_ok:
    print(f"❌ Contract FAILED: {contract_path}")
    sys.exit(1)

print(f"✅ Contract PASSED: {contract_path}")
sys.exit(0)