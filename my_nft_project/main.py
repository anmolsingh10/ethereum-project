import asyncio
from manager.nft_collection_manager import NFTCollectionManager
from repository.collection_repository import CollectionRepository

# Spark already exists in Serverless
# DO NOT recreate unless needed

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

manager.fetch_base("collection_1")

# safer than await
await manager.fetch_metadata("collection_1")
