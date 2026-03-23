import asyncio
import aiohttp
import json
import time
import traceback
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType


class NFTMetadataService:

    def __init__(self, spark, alchemy_key):
        self.spark = spark
        self.alchemy_key = alchemy_key
        self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
        self.metadata_table = "nft_metadata_table"

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

    async def _fetch_metadata(self, session, row, semaphore):
        try:
            token_id = str(row.token_id) if row.token_id else None
            if token_id is None or row.contract_address is None:
                return None

            url    = f"{self.base_url}/getNFTMetadata"
            params = {
                "contractAddress": str(row.contract_address),
                "tokenId":         token_id
            }

            async with semaphore:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        return None

                    data       = await response.json()
                    metadata   = data.get("raw", {}).get("metadata", {})
                    attributes = metadata.get("attributes", [])

                    base_data = {
                        "collection_id":    str(row.collection_id)   if row.collection_id   else None,
                        "collection_name":  str(row.collection_name) if row.collection_name else None,
                        "contract_address": str(row.contract_address),
                        "token_id":         token_id,
                        "name":             str(metadata.get("name"))        if metadata.get("name")        else None,
                        "description":      str(metadata.get("description")) if metadata.get("description") else None,
                        "image_url":        str(metadata.get("image"))       if metadata.get("image")       else None,
                        "attributes_json":  json.dumps(attributes),
                        "metadata_json":    json.dumps(metadata)
                    }

                    if not attributes:
                        return [{**base_data, "trait_type": None, "trait_value": None}]

                    return [
                        {
                            **base_data,
                            "trait_type":  str(attr.get("trait_type")) if attr.get("trait_type") else None,
                            "trait_value": str(attr.get("value"))      if attr.get("value")      else None
                        }
                        for attr in attributes
                    ]

        except Exception:
            traceback.print_exc()
            return None

    async def fetch_all_metadata_async(self, collection_id):
        try:
            all_rows = self.spark.table("nft_base_table") \
                .filter(col("collection_id") == str(collection_id)) \
                .filter(col("token_id").isNotNull()) \
                .collect()

            total_nfts = len(all_rows)

            if total_nfts == 0:
                print("⚠️ No NFTs found for this collection")
                return

            batch_size      = 100
            total_processed = 0

            for i in range(0, total_nfts, batch_size):
                batch_rows = all_rows[i: i + batch_size]

                semaphore = asyncio.Semaphore(10)

                async with aiohttp.ClientSession() as session:
                    tasks = [
                        self._fetch_metadata(session, row, semaphore)
                        for row in batch_rows
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                metadata_rows = [
                    item
                    for sublist in results
                    if sublist and not isinstance(sublist, Exception)
                    for item in sublist
                ]

                if metadata_rows:
                    # ── Retry createDataFrame ─────────
                    df = None
                    for attempt in range(1, 4):
                        try:
                            df = self.spark.createDataFrame(
                                metadata_rows,
                                schema=self._get_schema()
                            )
                            break
                        except Exception:
                            if attempt < 3:
                                time.sleep(30)
                            else:
                                raise

                    if df is None:
                        continue

                    # ✅ simple append — fast
                    df.write \
                        .mode("append") \
                        .option("mergeSchema", "true") \
                        .partitionBy("collection_id") \
                        .saveAsTable(self.metadata_table)

                    total_processed += len(metadata_rows)

            print(f"✅ Metadata fetched for collection_id: {collection_id} | Total: {total_processed}")

        except Exception as e:
            print(f"❌ Error in fetch_all_metadata_async for collection_id: {collection_id}")
            traceback.print_exc()
            raise