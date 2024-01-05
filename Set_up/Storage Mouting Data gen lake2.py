# Databricks notebook source
# MAGIC   %md
# MAGIC 1. First create a storage acccount
# MAGIC 2. Create a bucket inside it
# MAGIC 3. Go to azure active directory. click on app registration and copyt the client id, tenant id and then create secret and copy those values.
# MAGIC 4. Go to azure storage account then the container click on Iam and give blob contributer role the app registration service account.
# MAGIC 5. Go to azure key vault create policy based azure key vault give required read and write permission by editing the policy. and then add the secrets.
# MAGIC 5. Once thhis is done open the databricks home page add #/secrets/createScope in the URL and create a secret scope. by giving the URI and resource id of the keyvault.
# MAGIC 6. This will allow secretscope manager to access the keyvault

# COMMAND ----------

def mount_datalake(storage_account, container):
    client_id = access_key=dbutils.secrets.get(scope="formula1project2",key="storageclientid1")
    tenant_id = access_key=dbutils.secrets.get(scope="formula1project2",key="storagetenantid")
    client_secret =access_key=dbutils.secrets.get(scope="formula1project2",key="storagesecret")
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    if any(mount.mountPoint == f"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container}")
    dbutils.fs.mount(
    source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container}", extra_configs = configs)
    


# COMMAND ----------

dbutils.secrets.help() 

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1project2')

# COMMAND ----------

display(mount_datalake('finaldatabricks','processed'))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 
# MAGIC dbfs:/mnt/finaldatabricks/processed/

# COMMAND ----------


