from manager.nft_collection_manager import NFTCollectionManager

def get_manager(spark, dbutils):
    alchemy_key = dbutils.secrets.get(scope="nft-scope", key="ALCHEMY_KEY")
    return NFTCollectionManager(spark, alchemy_key)