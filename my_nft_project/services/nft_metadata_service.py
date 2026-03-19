import asyncio
import aiohttp
import json
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType  # ✅ add this


class NFTMetadataService:

    def __init__(self, spark, alchemy_key):
        self.spark = spark
        self.alchemy_key = alchemy_key
        self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
        self.metadata_table = "nft_metadata_table"

    # ✅ Define schema explicitly
    def _get_schema(self):
        return StructType([
            StructField("collection_id",    StringType(), True),
            StructField("collection_name",  StringType(), True),
            StructField("contract_address", StringType(), True),
            StructField("token_id",         StringType(), True),
            StructField("name",             StringType(), True),
            StructField("description",      StringType(), True),
            StructField("image_url",        StringType(), True),
            StructField("attributes_json",  StringType(), True),
            StructField("metadata_json",    StringType(), True),
            StructField("trait_type",       StringType(), True),
            StructField("trait_value",      StringType(), True),
        ])

    async def _fetch_metadata(
        self,
        session,
        collection_id,
        collection_name,
        contract_address,
        token_id,
        semaphore
    ):
        url = f"{self.base_url}/getNFTMetadata"
        params = {
            "contractAddress": contract_address,
            "tokenId": token_id
        }

        async with semaphore:
            async with session.get(url, params=params) as response:

                if response.status != 200:
                    return None

                data = await response.json()
                metadata = data.get("raw", {}).get("metadata", {})
                attributes = metadata.get("attributes", [])

                base_data = {
                    "collection_id":    str(collection_id)      if collection_id    else None,
                    "collection_name":  str(collection_name)    if collection_name  else None,
                    "contract_address": str(contract_address)   if contract_address else None,
                    "token_id":         str(token_id)           if token_id         else None,
                    "name":             str(metadata.get("name"))           if metadata.get("name")         else None,
                    "description":      str(metadata.get("description"))    if metadata.get("description")  else None,
                    "image_url":        str(metadata.get("image"))          if metadata.get("image")        else None,
                    "attributes_json":  json.dumps(attributes),
                    "metadata_json":    json.dumps(metadata)
                }

                if not attributes:
                    return [{
                        **base_data,
                        "trait_type":  None,
                        "trait_value": None
                    }]

                rows = []
                for attr in attributes:
                    rows.append({
                        **base_data,
                        "trait_type":  str(attr.get("trait_type")) if attr.get("trait_type") else None,
                        "trait_value": str(attr.get("value"))      if attr.get("value")      else None
                    })

                return rows

    async def fetch_all_metadata_async(self, collection_id):

        nft_df = self.spark.table("nft_base_table") \
            .filter(col("collection_id") == collection_id)

        nft_rows = nft_df.collect()

        semaphore = asyncio.Semaphore(20)

        async with aiohttp.ClientSession() as session:

            tasks = [
                self._fetch_metadata(
                    session,
                    row.collection_id,
                    row.collection_name,
                    row.contract_address,
                    row.token_id,
                    semaphore
                )
                for row in nft_rows
            ]

            results = await asyncio.gather(*tasks)

        metadata_rows = [
            item
            for sublist in results if sublist
            for item in sublist
        ]

        # ✅ pass explicit schema — no more type inference errors
        df = self.spark.createDataFrame(metadata_rows, schema=self._get_schema())

        df.write \
            .partitionBy("collection_id") \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.metadata_table)

        print(f"Metadata fetched for collection_id: {collection_id}")
        return df