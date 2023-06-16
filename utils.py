
import pandas as pd
from datetime import datetime
import hashlib

def hash_id(id_str):
    return hashlib.shake_256(str(id_str).encode("utf-8")).hexdigest(length=16)

def get_lookup_fn(df, from_col, to_col):
    assert from_col in df.columns
    assert to_col in df.columns
    
    lookup_dict = pd.Series(df[to_col].values, index=df[from_col]).to_dict()
    
    return lambda from_val: lookup_dict[from_val]

def parse_date_formats(date, formats):
    i = 0
    while i < len(formats):
        try:
            return datetime.strptime(date, formats[i])
        except ValueError:
            i += 1
    
    raise ValueError(f"Could not parse date: {date}")