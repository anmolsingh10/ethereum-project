# from pyspark.sql.functions import when, lit, col

# class CollectionRepository:

#     def __init__(self, spark):
#         self.spark = spark
#         self.table_name = "collection_reference"

#     # -------------------------
#     # Create table
#     # -------------------------
#     def create_collections(self, collections_dict):

#         rows = [
#             (name, address, "NOT_FETCHED")
#             for name, address in collections_dict.items()
#         ]

#         df = self.spark.createDataFrame(
#             rows,
#             ["collection_name", "contract_address", "status"]
#         )

#         df.write \
#             .mode("overwrite") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(self.table_name)

#     # -------------------------
#     # Update status
#     # -------------------------
#     def update_status(self, collection_name, new_status):

#         df = self.spark.table(self.table_name)

#         updated_df = df.withColumn(
#             "status",
#             when(col("collection_name") == collection_name, lit(new_status))
#             .otherwise(col("status"))
#         )

#         updated_df.write \
#             .mode("overwrite") \
#             .option("overwriteSchema", "true") \
#             .saveAsTable(self.table_name)

#     # -------------------------
#     # Get Contract Address
#     # -------------------------
#     def get_collection_id(self, collection_name):

#         df = self.spark.table(self.table_name)

#         result = (
#             df.filter(col("collection_name") == collection_name)
#               .select("contract_address")
#               .first()
#         )

#         return result["contract_address"] if result else None

#     # -------------------------
#     # Get Status
#     # -------------------------
#     def get_status(self, collection_name):

#         df = self.spark.table(self.table_name)

#         result = (
#             df.filter(col("collection_name") == collection_name)
#               .select("status")
#               .first()
#         )

#         return result["status"] if result else None
from pyspark.sql.functions import when, lit, col, max as spark_max


class CollectionRepository:

    def __init__(self, spark):
        self.spark = spark
        self.table_name = "collection_reference"

    # -------------------------
    # Create table
    # -------------------------
    def create_collections(self, collections_dict):

        rows = [
            (i + 1, name, address, "NOT_FETCHED")
            for i, (name, address) in enumerate(collections_dict.items())
        ]

        df = self.spark.createDataFrame(
            rows,
            ["id", "collection_name", "contract_address", "status"]
        )

        df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(self.table_name)

    # -------------------------
    # Get reference id
    # -------------------------
    def __get_collection_ref_id(self, collection_name):

        df = self.spark.table(self.table_name)

        result = (
            df.filter(col("collection_name") == collection_name)
              .select("id")
              .first()
        )

        return result["id"] if result else None

    # -------------------------
    # Insert new collection
    # -------------------------
    def __store_collection_to_table(self, collection_name):

        df = self.spark.table(self.table_name)

        new_id = df.select(spark_max("id")).first()[0] + 1

        new_row = self.spark.createDataFrame(
            [(new_id, collection_name, None, "NOT_FETCHED")],
            ["id", "collection_name", "contract_address", "status"]
        )

        new_row.write.mode("append").saveAsTable(self.table_name)

        return new_id

    # -------------------------
    # Get or create collection id
    # -------------------------
    def get_collection_ref_id(self, collection_name):

        ref_id = self.__get_collection_ref_id(collection_name)

        if ref_id:
            return ref_id

        return self.__store_collection_to_table(collection_name)

    # -------------------------
    # Get Contract Address
    # -------------------------
    def get_contract_address(self, collection_name):

        df = self.spark.table(self.table_name)

        result = (
            df.filter(col("collection_name") == collection_name)
              .select("contract_address")
              .first()
        )

        return result["contract_address"] if result else None

    # -------------------------
    # Update status
    # -------------------------
    def update_status(self, collection_name, new_status):

        df = self.spark.table(self.table_name)

        updated_df = df.withColumn(
            "status",
            when(col("collection_name") == collection_name, lit(new_status))
            .otherwise(col("status"))
        )

        updated_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(self.table_name)

    # -------------------------
    # Get Status
    # -------------------------
    def get_status(self, collection_name):

        df = self.spark.table(self.table_name)

        result = (
            df.filter(col("collection_name") == collection_name)
              .select("status")
              .first()
        )

        return result["status"] if result else None