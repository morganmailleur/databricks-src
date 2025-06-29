# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks — Interoperability & Delta MERGE Demo
# MAGIC
# MAGIC Certification DE Associate — **2 cas réels** de la certification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objectifs :
# MAGIC
# MAGIC - Illustrer **l’interopérabilité** issu d’une question de la certification
# MAGIC - Montrer un **cas concret d’interopérabilité**
# MAGIC - Démontrer le **MERGE (UPSERT) Delta en SQL**, avec question de la certification
# MAGIC - Utiliser des **données client fictives** pour un cas concret en **Spark**

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Création de données pour nos cas d'usage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table `geo_lookup`

# COMMAND ----------

# Création des données fictives
geo_data = [
    ("Paris", "FR", "EU"),
    ("Lyon", "FR", "EU"),
    ("Casablanca", "MA", "AF"),
    ("Dakar", "SN", "AF"),
    ("Lagos", "NG", "AF"),
    ("Tokyo", "JP", "AS"),
]
# Création de la DataFrame et écriture
geo_df = spark.createDataFrame(geo_data, ["city", "country", "continent"])
geo_df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.geo_lookup")

# COMMAND ----------

# Données de ventes simulées
sales_data = [
    (101, "Casablanca", "AF", 1500),
    (102, "Dakar", "AF", 1200),
    (103, "Paris", "EU", 2000),
    (104, "Tokyo", "AS", 1800),
    (105, "Lagos", "AF", 1600),
    (106, "Lyon", "EU", 1400),
    (107, "Dakar", "AF", 1100),
]
# Création de la DataFrame et écriture
sales_df = spark.createDataFrame(sales_data, ["sale_id", "city", "continent", "amount"])
sales_df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables `demo_merge_clients` et `demo_merge_clients_update`

# COMMAND ----------

# Données clients existants
clients = [
    (1, "Alice", "alice@mail.com", "Paris"),
    (2, "Bob", "bob@gmail.com", "Lyon"),
    (3, "Charlie", "charlie@protonmail.com", "Marseille-bébé"),
]

# Données mises à jour
updates = [
    # Ligne déjà présente, aucune modification
    (2, "Bob", "bob@gmail.com", "Lyon"),
    # Correction de la ville
    (3, "Charlie", "charlie@protonmail.com", "Marseille"),
    # Nouvelle ligne
    (4, "Diane", "diane@mail.com", "Nice"),
]

clients_df = spark.createDataFrame(clients, ["client_id", "name", "email", "city"])
updates_df = spark.createDataFrame(updates, ["client_id", "name", "email", "city"])

# Écriture des tables cible (silver) et source (bronze)
clients_df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.demo_merge_clients")
updates_df.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.demo_merge_clients_update")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Interopérabilité : cas *certification*

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❓ Question d’examen — Interopérabilité Python / SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Un data ingénieur junior souhaite exécuter les deux cellules suivantes l’une après l’autre. Les tables `geo_lookup` et `sales` sont valides.
# MAGIC <br/><br/>
# MAGIC ```python
# MAGIC # Cellule Python
# MAGIC city_af = [x[0] for x in (
# MAGIC     spark.table("workspace.silver.geo_lookup")
# MAGIC          .filter("continent = 'AF'")
# MAGIC          .select("city")
# MAGIC          .collect()
# MAGIC )]
# MAGIC ```
# MAGIC ```sql
# MAGIC -- Cellule SQL
# MAGIC CREATE TEMP VIEW sales_af AS
# MAGIC SELECT *
# MAGIC FROM   workspace.silver.sales
# MAGIC WHERE  city IN city_af
# MAGIC   AND  continent = "AF";
# MAGIC ```
# MAGIC
# MAGIC Quel est le résultat ?
# MAGIC
# MAGIC - **A** Les deux cellules s’exécutent. La vue `sales_af` contient toutes les lignes de `sales` où `continent = 'AF'` et `city` est dans la liste `city_af`.
# MAGIC - **B** La cellule Python réussit (elle renvoie une liste Python), mais la cellule SQL échoue : `city_af` n’est pas accessible dans `%sql`.
# MAGIC - **C** Les deux cellules réussissent : la cellule Python crée un DataFrame PySpark utilisable directement dans `%sql`.
# MAGIC - **D** La cellule Python crée la liste ; la cellule SQL s’exécute mais la vue `sales_af` est vide (aucun résultat).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exécution du code

# COMMAND ----------

# Cellule Python
city_af = [x[0] for x in (
    spark.table("workspace.silver.geo_lookup")
         .filter("continent = 'AF'")
         .select("city")
         .collect()
)]

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cellule SQL
# MAGIC CREATE OR REPLACE TEMP VIEW sales_af AS
# MAGIC SELECT *
# MAGIC FROM   workspace.silver.sales
# MAGIC WHERE  city IN city_af
# MAGIC   AND  continent = "AF";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemple fonctionnel

# COMMAND ----------

# Cellule Python fonctionnelle
city_af_df = (
    spark.table("workspace.silver.geo_lookup")
         .filter("continent = 'AF'")
         .select("city")
)
# Exposer le DataFrame dans une vue temporaire
city_af_df.createOrReplaceTempView("city_af")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sales_af AS
# MAGIC SELECT *
# MAGIC FROM   workspace.silver.sales
# MAGIC WHERE  city IN (SELECT city FROM city_af)
# MAGIC   AND  continent = 'AF';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_af;

# COMMAND ----------

# MAGIC %md
# MAGIC ### lien databricks Interoperability 
# MAGIC [doc-officiel](https://docs.databricks.com/aws/en/lakehouse-architecture/interoperability-and-usability/)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Delta MERGE — SQL puis PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❓ Question d’examen — Delta MERGE (sans `WHEN MATCHED`)

# COMMAND ----------

# MAGIC %md
# MAGIC Nous disposons des deux tables suivantes :
# MAGIC
# MAGIC - `workspace.silver.demo_merge_clients` (cible)
# MAGIC - `workspace.bronze.demo_merge_clients_update` (source)
# MAGIC
# MAGIC Le code suivant est proposé :
# MAGIC <br/><br/>
# MAGIC ```sql
# MAGIC MERGE INTO workspace.silver.demo_merge_clients AS cible
# MAGIC USING   workspace.bronze.demo_merge_clients_update AS maj
# MAGIC ON      cible.client_id = maj.client_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC ```
# MAGIC
# MAGIC Que se passe-t-il pour les lignes où `cible.client_id = maj.client_id` ?
# MAGIC
# MAGIC - **A** Les lignes existantes sont mises à jour dans la table cible.
# MAGIC - **B** Les lignes existantes sont dupliquées ; l’instruction `INSERT *` ajoute un doublon.
# MAGIC - **C** Les lignes existantes sont ignorées : aucune modification n’est effectuée.
# MAGIC - **D** La commande échoue, car la clause `WHEN MATCHED` est obligatoire dans un `MERGE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exécution du `MERGE`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.demo_merge_clients

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.bronze.demo_merge_clients_update

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.silver.demo_merge_clients AS cible
# MAGIC USING   workspace.bronze.demo_merge_clients_update AS maj
# MAGIC ON      cible.client_id = maj.client_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.demo_merge_clients ORDER BY client_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemple classique (MERGE en PySpark)

# COMMAND ----------

# Réinitialisation des tables pour un exemple propre
clients_df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.demo_merge_clients")
updates_df.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze.demo_merge_clients_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.demo_merge_clients

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.bronze.demo_merge_clients_update

# COMMAND ----------

from delta.tables import DeltaTable

clients_tbl = DeltaTable.forName(spark, "workspace.silver.demo_merge_clients")
updates_df = spark.table("workspace.bronze.demo_merge_clients_update")

merge_stats = clients_tbl.alias("cible").merge(
    updates_df.alias("maj"),
    "cible.client_id = maj.client_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

display(merge_stats)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.demo_merge_clients ORDER BY client_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### lien databricks merge into
# MAGIC [doc-officiel](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fin !

# COMMAND ----------

# MAGIC %md
# MAGIC ![gif](https://media1.tenor.com/m/Lmhw-ZBSI4cAAAAC/kronk-mission-accomplished.gif)