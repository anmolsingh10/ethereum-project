from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    row_number,
    when
)
from pyspark.sql.window import Window
import asyncio
import nest_asyncio
import traceback


class NFTCollectionManager:

    def __init__(self, spark, alchemy_key):

        from repository.collection_repository import CollectionRepository
        from services.nft_base_service import NFTBaseService
        from services.nft_metadata_service import NFTMetadataService

        self.spark = spark
        self.repo             = CollectionRepository(spark)
        self.base_service     = NFTBaseService(spark, alchemy_key)
        self.metadata_service = NFTMetadataService(spark, alchemy_key)

    # -------------------------
    # Run Full Pipeline (no async)
    # -------------------------
    def run(self, contract_address):  # ✅ no async
        nest_asyncio.apply()
        asyncio.run(self._run_async(contract_address))

    # -------------------------
    # Internal Async Pipeline
    # -------------------------
    async def _run_async(self, contract_address):
        try:
            collection_name = f"collection_{contract_address[:8].lower()}"

            self.repo.create_collections({
                collection_name: contract_address
            })

            print(f"\n{'='*40}")
            print(f"Processing: {contract_address}")
            print(f"{'='*40}")

            print(f"\nStep 1: Fetching Base NFTs...")
            self.fetch_base(collection_name)

            print(f"\nStep 2: Fetching Metadata...")
            await self.fetch_metadata(collection_name)

            print(f"\nStep 3: Calculating Rarity...")
            self.calculate_uniqueness(collection_name)

            print(f"\n✅ Done: {contract_address}")

        except Exception:
            print(f"\n❌ Failed for: {contract_address}")
            traceback.print_exc()
            raise

    # -------------------------
    # Fetch Base NFTs
    # -------------------------
    def fetch_base(self, collection_name):
        try:
            status = self.repo.get_status(collection_name)

            if status == "FETCHED":
                print("Base already fetched.")
                return

            self.repo.update_status(collection_name, "FETCHING")

            collection_ref_id = self.repo.get_collection_ref_id(collection_name)
            contract_address  = self.repo.get_contract_address(collection_name)

            self.base_service.fetch_nft_base(
                collection_ref_id,
                contract_address,
                collection_name
            )

            self.repo.update_status(collection_name, "FETCHED")

        except Exception:
            print(f"❌ Error in fetch_base: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Fetch Metadata
    # -------------------------
    async def fetch_metadata(self, collection_name):
        try:
            status = self.repo.get_status(collection_name)

            if status == "METADATA_FETCHED":
                print("Metadata already fetched.")
                return

            self.repo.update_status(collection_name, "METADATA_FETCHING")

            collection_ref_id = self.repo.get_collection_ref_id(collection_name)

            await self.metadata_service.fetch_all_metadata_async(collection_ref_id)

            self.repo.update_status(collection_name, "METADATA_FETCHED")

        except Exception:
            print(f"❌ Error in fetch_metadata: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Calculate NFT Uniqueness
    # -------------------------
    def calculate_uniqueness(self, collection_name):
        try:
            collection_ref_id = str(self.repo.get_collection_ref_id(collection_name))

            # ── Load metadata ───────────────────────
            metadata_df = self.spark.table("nft_metadata_table") \
                .filter(col("collection_id") == collection_ref_id) \
                .filter(col("token_id").isNotNull())

            # ── Load base tokens ────────────────────
            base_tokens = self.spark.table("nft_base_table") \
                .filter(col("collection_id") == collection_ref_id) \
                .filter(col("token_id").isNotNull()) \
                .select("collection_id", "token_id") \
                .distinct()

            # ── Only valid traits ───────────────────
            trait_df = metadata_df \
                .filter(col("trait_type").isNotNull()) \
                .filter(col("trait_value").isNotNull())

            # ── Step 1: trait frequency ─────────────
            trait_freq = trait_df.groupBy(
                "collection_id",
                "trait_type",
                "trait_value"
            ).agg(count("*").alias("trait_count"))

            # ── Step 2: total per trait type ────────
            trait_total = trait_freq.groupBy(
                "collection_id",
                "trait_type"
            ).agg(spark_sum("trait_count").alias("total_trait_count"))

            # ── Step 3: join frequencies ────────────
            rarity_calc = trait_freq.join(
                trait_total,
                ["collection_id", "trait_type"]
            )

            # ── Step 4: rarity score ────────────────
            rarity_calc = rarity_calc.withColumn(
                "rarity_score",
                when(
                    col("total_trait_count") == col("trait_count"),
                    0
                ).otherwise(
                    (col("trait_count") /
                    (col("total_trait_count") - col("trait_count")) * 100)
                    .cast("int")
                )
            )

            # ── Step 5: map back to NFTs ────────────
            rarity_df = trait_df.join(
                rarity_calc,
                ["collection_id", "trait_type", "trait_value"]
            )

            # ── Step 6: aggregate per NFT ───────────
            nft_rarity = rarity_df.groupBy(
                "collection_id",
                "token_id"
            ).agg(
                spark_sum("rarity_score").cast("int").alias("rarity_score")
            )

            # ── Step 7: left join with base tokens ──
            nft_rarity = base_tokens.join(
                nft_rarity,
                ["collection_id", "token_id"],
                "left"
            ).fillna(0, subset=["rarity_score"])

            # ── Step 8: rank ────────────────────────
            window_spec = Window.partitionBy("collection_id") \
                .orderBy(col("rarity_score").asc(), col("token_id").asc())

            nft_rarity = nft_rarity.withColumn(
                "rank",
                row_number().over(window_spec)
            )

            # ── Step 9: create table if not exists ──
            nft_rarity.write \
                .mode("ignore") \
                .option("mergeSchema", "true") \
                .partitionBy("collection_id") \
                .saveAsTable("nft_rarity_table")

            # ── Step 10: MERGE — no duplicates ───────
            nft_rarity.createOrReplaceTempView("new_rarity")

            self.spark.sql("""
                MERGE INTO nft_rarity_table AS target
                USING new_rarity AS source
                ON  target.collection_id = source.collection_id
                AND target.token_id      = source.token_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.rarity_score = source.rarity_score,
                        target.rank         = source.rank
                WHEN NOT MATCHED THEN
                    INSERT (collection_id, token_id, rarity_score, rank)
                    VALUES (source.collection_id, source.token_id, source.rarity_score, source.rank)
            """)

            print(f"✅ Uniqueness calculated and saved for: {collection_name}")

        except Exception:
            print(f"❌ Error in calculate_uniqueness: {collection_name}")
            traceback.print_exc()
            raise