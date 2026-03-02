from pyspark.sql.functions import when, lit, col

class CollectionRepository:

    def __init__(self, spark):
        self.spark = spark
        self.table_name = "collection_reference"

    # -------------------------
    # Create table
    # -------------------------
    def create_collections(self, collections_dict):

        rows = [
            (name, address, "NOT_FETCHED")
            for name, address in collections_dict.items()
        ]

        df = self.spark.createDataFrame(
            rows,
            ["collection_name", "contract_address", "status"]
        )

        df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(self.table_name)

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
    # Get Contract Address
    # -------------------------
    def get_collection_id(self, collection_name):

        df = self.spark.table(self.table_name)

        result = (
            df.filter(col("collection_name") == collection_name)
              .select("contract_address")
              .first()
        )

        return result["contract_address"] if result else None

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