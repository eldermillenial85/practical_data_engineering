
import sys
sys.path.append("/work")

from practical_data_engineering.utils import hash_id

def test_hash_id():
    assert hash_id("test_string") == "fbb2c0178edee895eb0a988974462c2e", "Hashed outpust does not match expected value."