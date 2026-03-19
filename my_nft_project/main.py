from manager.nft_collection_manager import NFTCollectionManager
from repository.collection_repository import CollectionRepository

ALCHEMY_KEY = dbutils.secrets.get(
    scope="nft-scope",
    key="ALCHEMY_KEY"
)

manager = NFTCollectionManager(spark, ALCHEMY_KEY)
repo = CollectionRepository(spark)

# Add both collections here
collections = {
    "collection_1": "0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB",  
    "collection_2": "0xBd3531dA5CF5857e7CfAA92426877b022e612cf8"
}

# Drop tables if needed
spark.sql("DROP TABLE IF EXISTS collection_reference")
spark.sql("DROP TABLE IF EXISTS nft_base_table")
spark.sql("DROP TABLE IF EXISTS nft_metadata_table")
spark.sql("DROP TABLE IF EXISTS nft_rarity_table")

# Create collections table with both collections
repo.create_collections(collections)

# Loop through each collection
for collection_name in collections.keys():

    print(f"\n{'='*40}")
    print(f"Processing: {collection_name}")
    print(f"{'='*40}")

    # Step 1: Fetch base NFTs
    manager.fetch_base(collection_name)

    # Step 2: Fetch metadata
    await manager.fetch_metadata(collection_name)

    # Step 3: Calculate rarity
    manager.calculate_uniqueness(collection_name)

    print(f" Done: {collection_name}")

print("\nAll collections processed successfully!")