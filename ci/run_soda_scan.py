"""
ci/run_soda_scan.py
Runs a Soda Core 4.x contract scan.
Usage: python3 ci/run_soda_scan.py <data_source> <config_path> <contract_path> <scan_name>
"""
import sys
from soda.scan import Scan

data_source   = sys.argv[1]
config_path   = sys.argv[2]
contract_path = sys.argv[3]
scan_name     = sys.argv[4]

scan = Scan()
scan.set_data_source_name(data_source)
scan.add_configuration_yaml_file(config_path)
scan.add_sodacl_yaml_file(contract_path)
scan.set_scan_definition_name(scan_name)

exit_code = scan.execute()
print(scan.get_logs_text())
sys.exit(exit_code)