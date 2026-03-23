from pyspark.sql.functions import when, lit, col, max as spark_max
import traceback


class CollectionRepository:

    def __init__(self, spark):
        self.spark = spark
        self.table_name = "collection_reference"

    # -------------------------
    # Create or Merge collections
    # -------------------------
    def create_collections(self, collections_dict):
        try:
            # ── Create table if not exists ──────────
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id               INT,
                    collection_name  STRING,
                    contract_address STRING,
                    status           STRING
                )
            """)

            for name, address in collections_dict.items():

                # ── Check if contract already exists ─
                existing = self.spark.sql(f"""
                    SELECT id FROM {self.table_name}
                    WHERE contract_address = '{address}'
                """).first()

                if existing:
                    # print(f"⏭️  Already exists: {address}")
                    continue

                # ── Get next available id ────────────
                max_id_row = self.spark.sql(f"""
                    SELECT COALESCE(MAX(id), 0) AS max_id
                    FROM {self.table_name}
                """).first()

                new_id = max_id_row["max_id"] + 1

                # ── Insert new collection ────────────
                self.spark.sql(f"""
                    INSERT INTO {self.table_name}
                    VALUES ({new_id}, '{name}', '{address}', 'NOT_FETCHED')
                """)

                print(f"✅ Registered: {address} with id: {new_id}")

        except Exception:
            print(f"❌ Error in create_collections")
            traceback.print_exc()
            raise

    # -------------------------
    # Get reference id (private)
    # -------------------------
    def __get_collection_ref_id(self, collection_name):
        try:
            df = self.spark.table(self.table_name)
            result = (
                df.filter(col("collection_name") == collection_name)
                  .select("id")
                  .first()
            )
            return result["id"] if result else None

        except Exception:
            print(f"❌ Error in __get_collection_ref_id: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Insert new collection (private)
    # -------------------------
    def __store_collection_to_table(self, collection_name):
        try:
            max_id_row = self.spark.sql(f"""
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM {self.table_name}
            """).first()

            new_id = max_id_row["max_id"] + 1

            self.spark.sql(f"""
                INSERT INTO {self.table_name}
                VALUES ({new_id}, '{collection_name}', NULL, 'NOT_FETCHED')
            """)

            print(f"✅ Stored new collection: {collection_name} with id: {new_id}")
            return new_id

        except Exception:
            print(f"❌ Error in __store_collection_to_table: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Get or create collection id
    # -------------------------
    def get_collection_ref_id(self, collection_name):
        try:
            ref_id = self.__get_collection_ref_id(collection_name)
            if ref_id:
                return ref_id
            return self.__store_collection_to_table(collection_name)

        except Exception:
            print(f"❌ Error in get_collection_ref_id: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Get Contract Address
    # -------------------------
    def get_contract_address(self, collection_name):
        try:
            df = self.spark.table(self.table_name)
            result = (
                df.filter(col("collection_name") == collection_name)
                  .select("contract_address")
                  .first()
            )
            return result["contract_address"] if result else None

        except Exception:
            print(f"❌ Error in get_contract_address: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Update Status
    # -------------------------
    def update_status(self, collection_name, new_status):
        try:
            self.spark.sql(f"""
                MERGE INTO {self.table_name} AS target
                USING (SELECT '{collection_name}' AS collection_name,
                              '{new_status}'      AS status) AS source
                ON target.collection_name = source.collection_name
                WHEN MATCHED THEN
                    UPDATE SET target.status = source.status
            """)

        except Exception:
            print(f"❌ Error in update_status: {collection_name}")
            traceback.print_exc()
            raise

    # -------------------------
    # Get Status
    # -------------------------
    def get_status(self, collection_name):
        try:
            df = self.spark.table(self.table_name)
            result = (
                df.filter(col("collection_name") == collection_name)
                  .select("status")
                  .first()
            )
            return result["status"] if result else None

        except Exception:
            print(f"❌ Error in get_status: {collection_name}")
            traceback.print_exc()
            raise