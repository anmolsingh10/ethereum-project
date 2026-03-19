from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round,
    when,
    row_number
)
from pyspark.sql.window import Window


class NFTCollectionManager:

    def __init__(self, spark, alchemy_key):

        from repository.collection_repository import CollectionRepository
        from services.nft_base_service import NFTBaseService
        from services.nft_metadata_service import NFTMetadataService

        self.spark = spark
        self.repo = CollectionRepository(spark)
        self.base_service = NFTBaseService(spark, alchemy_key)
        self.metadata_service = NFTMetadataService(spark, alchemy_key)

    # -------------------------
    # Fetch Base NFTs
    # -------------------------
    def fetch_base(self, collection_name):

        status = self.repo.get_status(collection_name)

        if status == "FETCHED":
            print(f"{collection_name}: Base already fetched.")
            return

        self.repo.update_status(collection_name, "FETCHING")

        collection_ref_id = self.repo.get_collection_ref_id(collection_name)
        contract_address = self.repo.get_contract_address(collection_name)

        self.base_service.fetch_nft_base(
            collection_ref_id,
            contract_address,
            collection_name
        )

        self.repo.update_status(collection_name, "FETCHED")

    # -------------------------
    # Fetch Metadata
    # -------------------------
    async def fetch_metadata(self, collection_name):

        status = self.repo.get_status(collection_name)

        if status == "METADATA_FETCHED":
            print(f"{collection_name}: Metadata already fetched.")
            return

        self.repo.update_status(collection_name, "METADATA_FETCHING")

        collection_ref_id = self.repo.get_collection_ref_id(collection_name)

        # ✅ pass collection_ref_id so only that collection is fetched
        await self.metadata_service.fetch_all_metadata_async(collection_ref_id)

        self.repo.update_status(collection_name, "METADATA_FETCHED")

    # -------------------------
    # Calculate NFT Uniqueness
    # -------------------------
    def calculate_uniqueness(self, collection_name):

        collection_ref_id = self.repo.get_collection_ref_id(collection_name)

        df = self.spark.table("nft_metadata_table") \
            .filter(col("collection_id") == collection_ref_id)

        trait_freq = df.groupBy(
            "collection_id",
            "trait_type",
            "trait_value"
        ).agg(
            count("*").alias("trait_count")
        )

        trait_total = trait_freq.groupBy(
            "collection_id",
            "trait_type"
        ).agg(
            spark_sum("trait_count").alias("total_trait_count")
        )

        rarity_calc = trait_freq.join(
            trait_total,
            ["collection_id", "trait_type"]
        )

        rarity_calc = rarity_calc.withColumn(
            "rarity_score",
            when(col("trait_count") != 0,
                 round(col("total_trait_count") / col("trait_count"), 0)
            ).otherwise(0).cast("int")
        )

        rarity_df = df.join(
            rarity_calc,
            ["collection_id", "trait_type", "trait_value"]
        )

        nft_rarity = rarity_df.groupBy(
            "collection_id",
            "token_id"
        ).agg(
            spark_sum("rarity_score").cast("int").alias("rarity_score")
        )

        window_spec = Window.partitionBy("collection_id") \
            .orderBy(col("rarity_score").asc())

        nft_rarity = nft_rarity.withColumn(
            "rank",
            row_number().over(window_spec)
        )

        nft_rarity.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("nft_rarity_table")

        print(f"Uniqueness calculated for: {collection_name}")
        return nft_rarity