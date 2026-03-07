import pyspark.sql.functions as f
from pyspark import pipelines as dp
from databricks.sdk.runtime import dbutils

client_id = dbutils.secrets.get(scope="streampulse", key="SP_CLIENT_ID")
tenant_id = dbutils.secrets.get(scope="streampulse", key="SP_TENANT_ID")
client_secret = dbutils.secrets.get(scope="streampulse", key="SP_CLIENT_SECRET")

# Set configs at the Spark session level instead of passing via .options()
spark.conf.set("fs.azure.account.auth.type.streampulse.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.streampulse.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.streampulse.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.streampulse.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.streampulse.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

@dp.table(name="streampulse.bronze.content_catalog_dim")
def content_catalog_dim():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "abfss://dimensions@streampulse.dfs.core.windows.net/checkpoints/schema")
        .load("abfss://dimensions@streampulse.dfs.core.windows.net/content-catalog/")
    )

@dp.table(name="streampulse.bronze.user_profile_dim")
def user_profile_dim():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "abfss://dimensions@streampulse.dfs.core.windows.net/checkpoints/schema")
        .load("abfss://dimensions@streampulse.dfs.core.windows.net/user-profiles/")
    )

