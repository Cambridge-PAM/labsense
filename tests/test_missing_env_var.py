from Labsense_SQL import ChemInventory_sqlserver as cis
import os


def test_missing_env_var_raises():
    # Ensure the env var is not set
    os.environ.pop("CHEMINVENTORY_CONNECTION_STRING", None)
    try:
        cis.main(dry_run=False)
        raised = False
    except Exception as e:
        raised = isinstance(e, RuntimeError)
    assert raised, "Expected RuntimeError when CHEMINVENTORY_CONNECTION_STRING is missing and dry_run=False"
