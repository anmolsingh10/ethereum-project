import asyncio
from pyspark.sql import SparkSession
from manager.nft_collection_manager import NFTCollectionManager
from repository.collection_repository import CollectionRepository

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

ALCHEMY_KEY = "f9cmHEGLyyy_opmzDfQYA"  
# Initialize manager
manager = NFTCollectionManager(spark, ALCHEMY_KEY)

# Create collection once
repo = CollectionRepository(spark)
collections = {
    "collection_1": "0xBd3531dA5CF5857e7CfAA92426877b022e612cf8"
}
repo.create_collections(collections)

# Fetch Base
manager.fetch_base("collection_1")

# Fetch Metadata
await manager.fetch_metadata("collection_1")
