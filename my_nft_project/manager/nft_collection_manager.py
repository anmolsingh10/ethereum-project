from pyspark.sql.functions import col, count, sum as spark_sum


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
            print("Base already fetched.")
            return

        self.repo.update_status(collection_name, "FETCHING")

        # collection_id = contract_address in your repo
        collection_id = self.repo.get_collection_id(collection_name)
        contract_address = collection_id

        self.base_service.fetch_nft_base(
            collection_id,
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
            print("Metadata already fetched.")
            return

        self.repo.update_status(collection_name, "METADATA_FETCHING")

        await self.metadata_service.fetch_all_metadata_async()

        self.repo.update_status(collection_name, "METADATA_FETCHED")

    # -------------------------
    # Calculate NFT Uniqueness
    # -------------------------
    def calculate_uniqueness(self):

        df = self.spark.table("nft_metadata_table")

        # Step 1: Trait frequency
        trait_freq = df.groupBy(
            "collection_id",
            "trait_type",
            "trait_value"
        ).agg(
            count("*").alias("trait_count")
        )

        # Step 2: Total NFTs per collection
        total_nfts = df.select(
            "collection_id",
            "token_id"
        ).distinct().groupBy(
            "collection_id"
        ).agg(
            count("*").alias("total_nfts")
        )

        # Step 3: Join metadata with frequencies
        rarity_df = df.join(
            trait_freq,
            ["collection_id", "trait_type", "trait_value"]
        ).join(
            total_nfts,
            ["collection_id"]
        )

        # Step 4: Calculate trait frequency
        rarity_df = rarity_df.withColumn(
            "trait_frequency",
            col("trait_count") / col("total_nfts")
        )

        # Step 5: Calculate rarity score
        rarity_df = rarity_df.withColumn(
            "rarity_score",
            1 / col("trait_frequency")
        )

        # Step 6: Aggregate rarity score per NFT
        nft_rarity = rarity_df.groupBy(
            "collection_id",
            "token_id"
        ).agg(
            spark_sum("rarity_score").alias("rarity_score")
        )

        # Step 7: Save result
        nft_rarity.write.mode("overwrite").saveAsTable("nft_rarity_table")

        return nft_rarity