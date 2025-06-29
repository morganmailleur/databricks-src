# Databricks notebook source
# MAGIC %md
# MAGIC # Architecture Médaillon : Ingestion ➝ KPI Métier
# MAGIC
# MAGIC Démonstration complète Bronze → Silver → Gold avec deux tables. Objectif : montrer la chaîne de valeur Data Engineer, du sourcing au KPI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 – Schémas

# COMMAND ----------

spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.bronze')
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.silver')
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.gold')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 – Ingestion Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC lire les données depuis samples.bakehouse et les stocker sans transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL sales_franchises

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.bronze.sales_franchises AS SELECT * FROM samples.bakehouse.sales_franchises;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyspark (Lazy) clients

# COMMAND ----------

# PySpark (lazy) — aucune action tant qu’on n’affiche pas
df_customers = spark.read.table("samples.bakehouse.sales_customers")

# COMMAND ----------

# Action pour montrer le lazy → exécution réelle
display(df_customers)

# COMMAND ----------

# Écriture des données des clients dans la table bronze
df_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.sales_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 – Nettoyage Silver

# COMMAND ----------

# MAGIC %md
# MAGIC Utiliser nos données bronze pour faire une couche silver qui n'a que les colonnes dont on a besoin et qui suit les normes de notre entreprise (customerID doit être nommé id_client).

# COMMAND ----------

# Silver clients
df_customers_silver = (
    spark.read.table("workspace.bronze.sales_customers")
        .select("customerID", "first_name", "last_name", "country","continent")
        .withColumnRenamed("customerID", "id_client")
)
df_customers_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.silver.clients")

# Silver franchises
df_franchises_silver = (
    spark.read.table("workspace.bronze.sales_franchises")
          .select("franchiseID", "country", "size")
)
df_franchises_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.silver.franchises")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 – Transformation Gold

# COMMAND ----------

# MAGIC %md
# MAGIC **Besoin** : Une table gold qui permet d'avoir des informations sur les clients par franchise et continent

# COMMAND ----------

from pyspark.sql.functions import countDistinct, round, col

df_clients    = spark.table("workspace.silver.clients")
df_franchises = spark.table("workspace.silver.franchises")

df_join = df_clients.join(df_franchises, on="country", how="inner")

df_gold = (
    df_join.groupBy("continent", "country")
           .agg(
               countDistinct("id_client").alias("nb_clients"),
               countDistinct("franchiseID").alias("nb_franchises")
           )
           .withColumn(
               "clients_par_franchise",
               round(col("nb_clients") / col("nb_franchises"), 2)
           )
           .orderBy(col("clients_par_franchise").desc())
)

df_gold.write.format("delta").mode("overwrite") \
       .saveAsTable("workspace.gold.kpi_clients_per_franchise")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 – Visualisation

# COMMAND ----------

# MAGIC %md
# MAGIC **Besoin** : Visualiser le nombre de clients par continent en ayant une information aussi sur le nombre de franchises

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Lecture de la table gold depuis Databricks
df = spark.table("workspace.gold.kpi_clients_per_franchise").toPandas()

# Graphique vertical avec hue = nb_franchises
plt.figure(figsize=(8, 5))
barplot = sns.barplot(
    data=df,
    x="country",
    y="clients_par_franchise",
    hue="nb_franchises",
    palette="viridis"
)

# Afficher les valeurs au-dessus des barres
for p in barplot.patches:
    height = p.get_height()
    if height > 0:
        barplot.annotate(f"{height:.1f}",
                         (p.get_x() + p.get_width() / 2., height),
                         ha='center', va='bottom', fontsize=9)

plt.title("Clients par franchise par pays")
plt.xlabel("Pays")
plt.ylabel("Clients par franchise")
plt.legend(title="Nombre de franchises")
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fin
# MAGIC
# MAGIC Pipeline complet Bronze → Silver → Gold + KPI et visualisation.
# MAGIC
# MAGIC ![GIF](https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExYWgzaGk2N3VsaDJnYW5tbjJnZnoxcWJwMmcyamNyNHQzbHQ1d2NzdCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/xUA7aQOxkz00lvCAOQ/giphy.gif)