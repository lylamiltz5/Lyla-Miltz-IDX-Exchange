import pandas as pd
from pathlib import Path

# Observed counts from last run:
# - Rows before concatenation (sum of source files): 566455
# - Rows after concatenation (combined file): 566455
# - Rows before Residential filter (combined file): 566455
# - Rows after Residential filter (residential file): 289595
#
# Script: concatenate all raw/CRMLSSold*.csv files into one CSV,
# then filter to PropertyType == 'Residential' and save result.
# This script prints counts before/after concatenation and before/after filter.

raw_dir = Path("raw")
pattern = "CRMLSSold*.csv"
combined_path = Path("concatenated_sold.csv")
residential_path = Path("concatenated_sold_residential.csv")

chunksize = 100_000
property_col = 'PropertyType'

files = sorted(raw_dir.glob(pattern))
if not files:
    print(f"No files found matching {pattern} in {raw_dir}")
    raise SystemExit(1)

# Count rows in source files (before concatenation)
total_before_concat = 0
for f in files:
    for chunk in pd.read_csv(f, chunksize=chunksize, low_memory=False):
        total_before_concat += len(chunk)

# Concatenate files into combined_path
combined_first_write = True
combined_rows = 0
for f in files:
    for chunk in pd.read_csv(f, chunksize=chunksize, low_memory=False):
        if combined_first_write:
            chunk.to_csv(combined_path, index=False, mode='w')
            combined_first_write = False
        else:
            chunk.to_csv(combined_path, index=False, header=False, mode='a')
        combined_rows += len(chunk)

# At this point: total_before_concat == combined_rows (unless something changed during write)

# Filter combined to Residential
res_first_write = True
res_rows = 0
for chunk in pd.read_csv(combined_path, chunksize=chunksize, low_memory=False):
    if property_col not in chunk.columns:
        continue
    before_filter_chunk_rows = len(chunk)
    mask = chunk[property_col].astype(str).str.strip().eq('Residential')
    filtered = chunk[mask]
    if filtered.empty:
        continue
    if res_first_write:
        filtered.to_csv(residential_path, index=False, mode='w')
        res_first_write = False
    else:
        filtered.to_csv(residential_path, index=False, header=False, mode='a')
    res_rows += len(filtered)

# Print counts
print(f"Rows before concatenation (sum of source files): {total_before_concat}")
print(f"Rows after concatenation (combined file): {combined_rows}")
print(f"Rows before Residential filter (combined file): {combined_rows}")
print(f"Rows after Residential filter (residential file): {res_rows}")
print(f"Combined saved to: {combined_path}")
print(f"Residential saved to: {residential_path}")
