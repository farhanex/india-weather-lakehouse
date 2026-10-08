STORAGE = "strgweatherde"
SERVER = "sql-weather-de.database.windows.net"
DB = "weatherdb"
gold = f"abfss://gold@{STORAGE}.dfs.core.windows.net/"
pwd = dbutils.secrets.get("kv-weather", "farhan786")

for name in ["dim_city", "fact_daily_weather"]:
    (spark.read.format("delta").load(gold + name)
      .write.format("sqlserver")
      .option("host", SERVER)
      .option("port", "1433")
      .option("database", DB)
      .option("dbtable", f"dbo.{name}")
      .option("user", "farhan786")
      .option("password", pwd)
      .mode("overwrite").save())
print("loaded")