# from manager.nft_collection_manager import NFTCollectionManager
# from repository.collection_repository import CollectionRepository
# import traceback

# ALCHEMY_KEY = dbutils.secrets.get(
#     scope="nft-scope",
#     key="ALCHEMY_KEY"
# )

# manager = NFTCollectionManager(spark, ALCHEMY_KEY)
# repo    = CollectionRepository(spark)

# collections = {
#     "collection_1": "0x23581767a106ae21c074b2276D25e5C3e136a68b",
#     "collection_2": "0xBd3531dA5CF5857e7CfAA92426877b022e612cf8"
# }

# # ── Reset tables ───────────────────────────────
# spark.sql("DROP TABLE IF EXISTS collection_reference")
# spark.sql("DROP TABLE IF EXISTS nft_base_table")
# spark.sql("DROP TABLE IF EXISTS nft_metadata_table")
# spark.sql("DROP TABLE IF EXISTS nft_rarity_table")

# repo.create_collections(collections)

# # ── Process each collection ────────────────────
# for collection_name in collections.keys():
#     try:
#         print(f"\n{'='*40}")
#         print(f"Processing: {collection_name}")
#         print(f"{'='*40}")

#         # Step 1
#         print(f"\nStep 1: Fetching Base NFTs...")
#         manager.fetch_base(collection_name)

#         # Step 2
#         print(f"\nStep 2: Fetching Metadata...")
#         await manager.fetch_metadata(collection_name)

#         # Step 3
#         print(f"\nStep 3: Calculating Rarity...")
#         manager.calculate_uniqueness(collection_name)

#         print(f"\nDone: {collection_name}")

#     except Exception as e:
#         print(f"\nFailed for: {collection_name}")
#         traceback.print_exc()
#         continue  # continues to next collection even if one fails

# print("\n All collections processed!")


from manager.nft_collection_manager import NFTCollectionManager

ALCHEMY_KEY = dbutils.secrets.get(scope="nft-scope", key="ALCHEMY_KEY")
manager     = NFTCollectionManager(spark, ALCHEMY_KEY)