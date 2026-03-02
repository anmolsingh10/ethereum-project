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

        contract_address = self.repo.get_collection_id(collection_name)

        self.base_service.fetch_nft_base(contract_address, collection_name)

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