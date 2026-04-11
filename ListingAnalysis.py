import pandas as pd
import glob
import os

file_pattern = os.path.join("raw", "CRMLSListing*.csv")
listing_files = glob.glob(file_pattern)

print(f"{len(listing_files)} listing files.")

df_listings = []

for file in listing_files:
    df = pd.read_csv(file, low_memory=False)
    df_listings.append(df)
   
concated_listings = pd.concat(df_listings, ignore_index=True)

print("Concatenated DataFrame shape:", concated_listings.shape)

output_file = 'concatenated_listings.csv'
concated_listings.to_csv(output_file, index=False)
print(f"Concatenated listings saved to {output_file}")

concated_listings.info()
concated_listings.head()
concated_listings.describe()
concated_listings.columns
concated_listings.shape