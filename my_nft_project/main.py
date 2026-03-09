from manager.nft_collection_manager import NFTCollectionManager
from repository.collection_repository import CollectionRepository

ALCHEMY_KEY = dbutils.secrets.get(
    scope="nft-scope",
    key="ALCHEMY_KEY"
)

manager = NFTCollectionManager(spark, ALCHEMY_KEY)

repo = CollectionRepository(spark)

collections = {
    "collection_1": "0xBd3531dA5CF5857e7CfAA92426877b022e612cf8"
}

repo.create_collections(collections)

# Step 1: Fetch base NFTs
manager.fetch_base("collection_1")

# Step 2: Fetch metadata
await manager.fetch_metadata("collection_1")

# Step 3: Run uniqueness algorithm (no printing)
manager.calculate_uniqueness()
