import requests
import time
from pyspark.sql.types import StructType, StructField, StringType


class NFTBaseService:

    def __init__(self, spark, alchemy_key):
        self.spark = spark
        self.alchemy_key = alchemy_key
        self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
        self.base_table = "nft_base_table"

    def fetch_nft_base(self, collection_id: str, contract_address: str, collection_name: str):

        url = f"{self.base_url}/getNFTsForContract"
        rows = []
        page_key = None

        while True:
            params = {
                "contractAddress": contract_address,
                "withMetadata": "true"
            }

            if page_key:
                params["pageKey"] = page_key

            response = requests.get(url, params=params)

            if response.status_code != 200:
                print("Error fetching NFTs:", response.text)
                break

            data = response.json()
            nfts = data.get("nfts", [])

            if not nfts:
                break

            for nft in nfts:

                metadata = nft.get("metadata") or {}
                raw_metadata = nft.get("raw", {}).get("metadata", {})

                name = (
                    metadata.get("name")
                    or raw_metadata.get("name")
                    or nft.get("title")
                    or ""
                )

                description = (
                    metadata.get("description")
                    or raw_metadata.get("description")
                    or nft.get("description")
                    or ""
                )

                image_url = (
                    metadata.get("image")
                    or raw_metadata.get("image")
                    or nft.get("image", {}).get("cachedUrl")
                    or ""
                )

                rows.append({
                    "collection_id": collection_id,
                    "collection_name": collection_name,
                    "contract_address": contract_address,
                    "token_id": str(nft.get("tokenId") or ""),
                    "name": name,
                    "description": description,
                    "image_url": image_url
                })

            page_key = data.get("pageKey")
            if not page_key:
                break

            time.sleep(0.2)

        schema = StructType([
            StructField("collection_id", StringType(), True),
            StructField("collection_name", StringType(), True),
            StructField("contract_address", StringType(), True),
            StructField("token_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("image_url", StringType(), True)
        ])

        df = self.spark.createDataFrame(rows, schema=schema)

        df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.base_table)

        print(f"Base NFTs fetched for collection: {collection_name}")
        return df