# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG if not EXISTS fmcg;
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA if not EXISTS fmcg.gold;
# MAGIC CREATE SCHEMA if not EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA if not EXISTS fmcg.bronze;

# COMMAND ----------

