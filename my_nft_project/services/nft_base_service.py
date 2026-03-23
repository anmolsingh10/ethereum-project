import requests
import time
import traceback
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col


class NFTBaseService:

    def __init__(self, spark, alchemy_key):
        self.spark = spark
        self.alchemy_key = alchemy_key
        self.base_url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{alchemy_key}"
        self.base_table = "nft_base_table"

    # -------------------------
    # Parse token_id safely
    # -------------------------
    def _parse_token_id(self, raw_token):
        try:
            if raw_token:
                raw_token = str(raw_token)
                if raw_token.startswith("0x"):
                    return str(int(raw_token, 16))
                return str(int(raw_token))
        except:
            return None
        return None

    # -------------------------
    # Schema
    # -------------------------
    def _get_schema(self):
        return StructType([
            StructField("collection_id",    StringType(), True),
            StructField("collection_name",  StringType(), True),
            StructField("contract_address", StringType(), True),
            StructField("token_id",         StringType(), True),
            StructField("name",             StringType(), True),
            StructField("description",      StringType(), True),
            StructField("image_url",        StringType(), True)
        ])

    # -------------------------
    # Save batch with retry
    # -------------------------
    def _save_batch(self, batch, schema):
        # ── Retry createDataFrame ─────────────────
        df = None
        for attempt in range(1, 4):
            try:
                df = self.spark.createDataFrame(batch, schema=schema)
                break
            except Exception:
                if attempt < 3:
                    print(f"⚠️ Attempt {attempt} failed, retrying in 30s...")
                    time.sleep(30)
                else:
                    raise

        if df is None:
            return

        # ✅ simple append — fast
        df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("collection_id") \
            .saveAsTable(self.base_table)

    # -------------------------
    # Fetch NFTs with Pagination
    # -------------------------
    def fetch_nft_base(self, collection_id: str, contract_address: str, collection_name: str):
        try:
            url         = f"{self.base_url}/getNFTsForContract"
            rows        = []
            page_key    = None
            batch_size  = 500
            schema      = self._get_schema()
            total_saved = 0

            while True:
                try:
                    params = {
                        "contractAddress": contract_address,
                        "withMetadata":    "true",
                        "limit":           100
                    }

                    if page_key:
                        params["pageKey"] = page_key

                    response = requests.get(url, params=params)

                    if response.status_code != 200:
                        break

                    data     = response.json()
                    nfts     = data.get("nfts", [])
                    page_key = data.get("pageKey")

                    if not nfts:
                        break

                    for nft in nfts:
                        try:
                            token_id = self._parse_token_id(nft.get("tokenId"))
                            if token_id is None:
                                continue

                            metadata     = nft.get("metadata") or {}
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
                                "collection_id":    str(collection_id),
                                "collection_name":  collection_name,
                                "contract_address": contract_address,
                                "token_id":         token_id,
                                "name":             name,
                                "description":      description,
                                "image_url":        image_url
                            })

                        except Exception:
                            continue

                    # ✅ save batch when reached batch_size
                    if len(rows) >= batch_size:
                        self._save_batch(rows, schema)
                        total_saved += len(rows)
                        rows = []

                    if not page_key:
                        break

                    time.sleep(0.2)

                except Exception:
                    traceback.print_exc()
                    break

            # ✅ save remaining rows
            if rows:
                self._save_batch(rows, schema)
                total_saved += len(rows)

            print(f"✅ Base NFTs fetched for: {collection_name} | Total: {total_saved}")

        except Exception as e:
            print(f"❌ Error in fetch_nft_base for: {collection_name}")
            traceback.print_exc()
            raise