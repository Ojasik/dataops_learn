# 1. Read CSV
df = spark.read.csv("/mnt/raw/data.csv", header=True, inferSchema=True)

# 2. Delete empty rows
df_clean = df.dropna()

# 3. Minimal validation
if df_clean.count() == 0:
    raise ValueError("There is no data after deleting empty rows")

expected_columns = ["col1", "col2", "col3"]
if df_clean.columns != expected_columns:
    raise ValueError("The columns don't match")

# 4. Save to curated layer
df_clean.write.format("delta").mode("overwrite").save("/mnt/curated/data")

print("Run is successful!")

#testttt
