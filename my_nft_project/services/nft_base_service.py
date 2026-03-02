import requests
import time
from pyspark.sql.types import StructType, StructField, StringType

class NFTBaseService:

    def __init__(self, spark, alchemy_key):
        self.spark = spark
        self.alchemy_key = alchemy_key
        self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
        self.base_table = "nft_base_table"

    # -----------------------------------
    # Fetch NFT Base (With Metadata)
    # -----------------------------------
    def fetch_nft_base(self, contract_address: str, collection_name: str):

        url = f"{self.base_url}/getNFTsForContract"
        rows = []
        page_key = None

        while True:
            params = {
                "contractAddress": contract_address,
                "withMetadata": "true"   # 🔥 Fetch name, description, image
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
                metadata = nft.get("metadata") or {}  # Ensure dict even if None

                rows.append({
                    "collection_name": collection_name,
                    "contract_address": contract_address,
                    "token_id": str(nft.get("tokenId") or ""),  # Convert to string
                    "name": metadata.get("name") or "",         # Default empty string
                    "description": metadata.get("description") or "",
                    "image_url": metadata.get("image") or ""
                })

            page_key = data.get("pageKey")
            if not page_key:
                break

            time.sleep(0.2)

        # -------------------------
        # Explicit schema
        # -------------------------
        schema = StructType([
            StructField("collection_name", StringType(), True),
            StructField("contract_address", StringType(), True),
            StructField("token_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("image_url", StringType(), True)
        ])

        df = self.spark.createDataFrame(rows, schema=schema)

        df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(self.base_table)

        print("Base NFTs with metadata fetched successfully.")
        return df