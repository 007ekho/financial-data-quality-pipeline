# """
# ci/run_soda_scan.py
# Runs a Soda Core 4.x contract verification.
# Usage: python3 ci/run_soda_scan.py <data_source_yaml> <contract_path>
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
 
# print(result.get_logs() if hasattr(result, 'get_logs') else str(result))
 
# if not result.is_ok():
#     sys.exit(1)
 
# sys.exit(0)


"""
ci/run_soda_scan.py
Runs a Soda Core 4.x contract verification.
Usage: python3 ci/run_soda_scan.py <data_source_yaml> <contract_path>
Exits 0 if all checks pass, 1 if any fail.
"""
import sys
from soda_core.contracts import verify_contract_locally

data_source_yaml = sys.argv[1]
contract_path    = sys.argv[2]

result = verify_contract_locally(
    data_source_file_path=data_source_yaml,
    contract_file_path=contract_path,
    publish=False,
)

if not result.is_ok:
    print(f"❌ Contract failed: {contract_path}")
    sys.exit(1)

print(f"✅ Contract passed: {contract_path}")
sys.exit(0)