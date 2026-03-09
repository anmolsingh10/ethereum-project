# import asyncio
# import aiohttp
# import json

# class NFTMetadataService:

#     def __init__(self, spark, alchemy_key):
#         self.spark = spark
#         self.alchemy_key = alchemy_key
#         self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
#         self.metadata_table = "nft_metadata_table"

#     async def _fetch_metadata(self, session, contract_address, token_id, semaphore):

#         url = f"{self.base_url}/getNFTMetadata"
#         params = {
#             "contractAddress": contract_address,
#             "tokenId": token_id
#         }

#         async with semaphore:
#             async with session.get(url, params=params) as response:

#                 if response.status != 200:
#                     return None

#                 data = await response.json()
#                 metadata = data.get("raw", {}).get("metadata", {})
#                 attributes = metadata.get("attributes", [])

#                 base_data = {
#                     "contract_address": contract_address,
#                     "token_id": token_id,
#                     "name": metadata.get("name"),
#                     "description": metadata.get("description"),
#                     "image_url": metadata.get("image"),
#                     "attributes_json": json.dumps(attributes),
#                     "metadata_json": json.dumps(metadata)
#                 }

#                 if not attributes:
#                     return [{
#                         **base_data,
#                         "trait_type": None,
#                         "trait_value": None
#                     }]

#                 rows = []
#                 for attr in attributes:
#                     rows.append({
#                         **base_data,
#                         "trait_type": attr.get("trait_type"),
#                         "trait_value": attr.get("value")
#                     })

#                 return rows

#     async def fetch_all_metadata_async(self):

#         nft_df = self.spark.table("nft_base_table")
#         nft_rows = nft_df.collect()

#         semaphore = asyncio.Semaphore(20)

#         async with aiohttp.ClientSession() as session:

#             tasks = [
#                 self._fetch_metadata(
#                     session,
#                     row.contract_address,
#                     row.token_id,
#                     semaphore
#                 )
#                 for row in nft_rows
#             ]

#             results = await asyncio.gather(*tasks)

#         metadata_rows = [
#             item
#             for sublist in results if sublist
#             for item in sublist
#         ]

#         df = self.spark.createDataFrame(metadata_rows)

#         df.write \
#             .mode("overwrite") \
#             .option("mergeSchema", "true") \
#             .saveAsTable(self.metadata_table)

#         print("Metadata fetched successfully.")
#         return df

import asyncio
import aiohttp
import json


class NFTMetadataService:

    def __init__(self, spark, alchemy_key):
        self.spark = spark
        self.alchemy_key = alchemy_key
        self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
        self.metadata_table = "nft_metadata_table"

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
                    "collection_id": collection_id,   # ✅ added
                    "collection_name": collection_name,
                    "contract_address": contract_address,
                    "token_id": token_id,
                    "name": metadata.get("name"),
                    "description": metadata.get("description"),
                    "image_url": metadata.get("image"),
                    "attributes_json": json.dumps(attributes),
                    "metadata_json": json.dumps(metadata)
                }

                # If no attributes exist
                if not attributes:
                    return [{
                        **base_data,
                        "trait_type": None,
                        "trait_value": None
                    }]

                rows = []
                for attr in attributes:
                    rows.append({
                        **base_data,
                        "trait_type": attr.get("trait_type"),
                        "trait_value": attr.get("value")
                    })

                return rows

    async def fetch_all_metadata_async(self):

        nft_df = self.spark.table("nft_base_table")
        nft_rows = nft_df.collect()

        semaphore = asyncio.Semaphore(20)

        async with aiohttp.ClientSession() as session:

            tasks = [
                self._fetch_metadata(
                    session,
                    row.collection_id,      # ✅ added
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

        df = self.spark.createDataFrame(metadata_rows)

        df.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.metadata_table)

        print("Metadata fetched successfully.")
        return df