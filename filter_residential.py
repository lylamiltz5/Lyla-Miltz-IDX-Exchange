import pandas as pd
from pathlib import Path

# Last observed counts (from latest run):
# - Total rows in source: 779713
# - Residential rows written: 492896

input_path = Path("concatenated_listings.csv")
output_path = Path("concatenated_listings_residential.csv")

if not input_path.exists():
    print(f"Input file not found: {input_path}")
    raise SystemExit(1)

col = 'PropertyType'
chunksize = 100_000
first_write = True
written = 0
for chunk in pd.read_csv(input_path, chunksize=chunksize, low_memory=False):
    if col not in chunk.columns:
        continue
    # accumulate totals
    total_rows = 0
    residential_rows = 0
    break

# Re-read to compute accurately in chunks (keeps memory low)
total_rows = 0
residential_rows = 0
written = 0
first_write = True
for chunk in pd.read_csv(input_path, chunksize=chunksize, low_memory=False):
    if col not in chunk.columns:
        continue
    total_rows += len(chunk)
    mask = chunk[col].astype(str).str.strip().eq('Residential')
    filtered = chunk[mask]
    if filtered.empty:
        continue
    residential_rows += len(filtered)
    if first_write:
        filtered.to_csv(output_path, index=False, mode='w')
        first_write = False
    else:
        filtered.to_csv(output_path, index=False, header=False, mode='a')
    written += len(filtered)

print(f"Total rows in source: {total_rows}")
print(f"Residential rows written: {residential_rows}")
print(f"Wrote {written} residential rows to {output_path}")
