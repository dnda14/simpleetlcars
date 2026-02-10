import duckdb

con = duckdb.connect()

df = con.execute("""
    SELECT
        ingest_date,
        COUNT(*) AS rows,
        AVG(msrp) AS avg_price
    FROM read_parquet('data/analytics/**/*.parquet')
    GROUP BY ingest_date
    ORDER BY ingest_date DESC
""").df()

print(df)
