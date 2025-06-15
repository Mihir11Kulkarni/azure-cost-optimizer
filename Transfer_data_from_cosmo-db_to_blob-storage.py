import json
import os
import time
from datetime import datetime, timedelta
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TieredDataTransfer:
    def __init__(self):
        """Initialize connections to Cosmos DB and Blob Storage"""
        # Cosmos DB configuration
        self.cosmos_client = CosmosClient(
            url=os.getenv('COSMOS_ENDPOINT'),
            credential=os.getenv('COSMOS_KEY')
        )
        self.database = self.cosmos_client.get_database_client(os.getenv('COSMOS_DATABASE', 'billing-database'))
        self.container = self.database.get_container_client('billing-records')
        
        # Blob Storage configuration
        self.blob_client = BlobServiceClient.from_connection_string(
            os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        )
        self.hot_container = 'billing-hot'
        self.cold_container = 'billing-cold'
        
        # Migration statistics
        self.stats = {
            'tier1_to_tier2': {'success': 0, 'failed': 0, 'errors': []},
            'tier2_to_tier3': {'success': 0, 'failed': 0, 'errors': []},
            'total_size_migrated': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Create containers if they don't exist
        self._create_containers()
    
    def _create_containers(self):
        """Create blob containers if they don't exist"""
        containers = [self.hot_container, self.cold_container]
        
        for container_name in containers:
            try:
                container_client = self.blob_client.get_container_client(container_name)
                container_client.create_container(public_access=None)
                logger.info(f"Created container: {container_name}")
            except Exception as e:
                if "ContainerAlreadyExists" not in str(e):
                    logger.error(f"Error creating container {container_name}: {e}")
                else:
                    logger.info(f"Container {container_name} already exists")
    
    def migrate_tier1_to_tier2(self, batch_size=100):
        """
        Migrate data from Tier 1 (Cosmos DB) to Tier 2 (Hot Blob Storage)
        Records older than 1 month but newer than 3 months
        """
        logger.info("Starting Tier 1 → Tier 2 migration (1-month old data)")
        
        # Calculate date ranges
        one_month_ago = datetime.utcnow() - timedelta(days=30)
        three_months_ago = datetime.utcnow() - timedelta(days=90)
        
        # Query for records between 1-3 months old that haven't been migrated
        query = """
        SELECT * FROM c 
        WHERE c.createdAt < @one_month_ago 
        AND c.createdAt >= @three_months_ago
        AND (c.storage_tier IS NULL OR c.storage_tier = 'cosmos')
        ORDER BY c.createdAt ASC
        """
        
        parameters = [
            {"name": "@one_month_ago", "value": one_month_ago.isoformat()},
            {"name": "@three_months_ago", "value": three_months_ago.isoformat()}
        ]
        
        try:
            # Execute query in batches
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=batch_size
            ))
            
            logger.info(f"Found {len(items)} records to migrate from Tier 1 to Tier 2")
            
            for item in items:
                try:
                    # Generate blob path
                    blob_path = self._generate_blob_path(item, 'hot')
                    
                    # Store in hot blob storage
                    blob_size = self._store_in_blob(item, self.hot_container, blob_path)
                    
                    # Update record in Cosmos DB with migration info
                    item['storage_tier'] = 'hot_blob'
                    item['blob_path'] = blob_path
                    item['migrated_to_hot_at'] = datetime.utcnow().isoformat()
                    item['blob_size'] = blob_size
                    
                    # Replace item in Cosmos DB
                    self.container.replace_item(item['id'], item)
                    
                    # Update statistics
                    self.stats['tier1_to_tier2']['success'] += 1
                    self.stats['total_size_migrated'] += blob_size
                    
                    logger.debug(f"Migrated record {item['id']} to hot blob storage ({blob_size} bytes)")
                    
                except Exception as e:
                    error_msg = f"Record {item.get('id', 'unknown')}: {str(e)}"
                    logger.error(f"Failed to migrate record to Tier 2: {error_msg}")
                    self.stats['tier1_to_tier2']['failed'] += 1
                    self.stats['tier1_to_tier2']['errors'].append(error_msg)
            
            logger.info(f"Tier 1 → Tier 2 migration completed. Success: {self.stats['tier1_to_tier2']['success']}, Failed: {self.stats['tier1_to_tier2']['failed']}")
            
        except Exception as e:
            logger.error(f"Tier 1 → Tier 2 migration failed: {e}")
            raise
    
    def migrate_tier2_to_tier3(self, batch_size=100):
        """
        Migrate data from Tier 2 (Hot Blob) to Tier 3 (Cold Blob Storage)
        Records older than 3 months
        """
        logger.info("Starting Tier 2 → Tier 3 migration (3-month old data)")
        
        three_months_ago = datetime.utcnow() - timedelta(days=90)
        
        # Query for records older than 3 months currently in hot blob storage
        query = """
        SELECT * FROM c 
        WHERE c.createdAt < @three_months_ago 
        AND c.storage_tier = 'hot_blob'
        ORDER BY c.createdAt ASC
        """
        
        parameters = [
            {"name": "@three_months_ago", "value": three_months_ago.isoformat()}
        ]
        
        try:
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=batch_size
            ))
            
            logger.info(f"Found {len(items)} records to migrate from Tier 2 to Tier 3")
            
            for item in items:
                try:
                    # Retrieve full record from hot blob storage
                    full_record = self._retrieve_from_blob(self.hot_container, item['blob_path'])
                    
                    if full_record:
                        # Generate new blob path for cold storage
                        cold_blob_path = self._generate_blob_path(full_record, 'cold')
                        
                        # Store in cold blob storage
                        blob_size = self._store_in_blob(full_record, self.cold_container, cold_blob_path)
                        
                        # Update record in Cosmos DB
                        item['storage_tier'] = 'cold_blob'
                        item['blob_path'] = cold_blob_path
                        item['migrated_to_cold_at'] = datetime.utcnow().isoformat()
                        item['blob_size'] = blob_size
                        
                        self.container.replace_item(item['id'], item)
                        
                        # Delete from hot blob storage
                        self._delete_from_blob(self.hot_container, item['blob_path'])
                        
                        # Update statistics
                        self.stats['tier2_to_tier3']['success'] += 1
                        self.stats['total_size_migrated'] += blob_size
                        
                        logger.debug(f"Migrated record {item['id']} to cold blob storage")
                    else:
                        raise Exception("Could not retrieve record from hot blob storage")
                    
                except Exception as e:
                    error_msg = f"Record {item.get('id', 'unknown')}: {str(e)}"
                    logger.error(f"Failed to migrate record to Tier 3: {error_msg}")
                    self.stats['tier2_to_tier3']['failed'] += 1
                    self.stats['tier2_to_tier3']['errors'].append(error_msg)
            
            logger.info(f"Tier 2 → Tier 3 migration completed. Success: {self.stats['tier2_to_tier3']['success']}, Failed: {self.stats['tier2_to_tier3']['failed']}")
            
        except Exception as e:
            logger.error(f"Tier 2 → Tier 3 migration failed: {e}")
            raise
    
    def _generate_blob_path(self, record, tier):
        """Generate hierarchical blob path for organized storage"""
        try:
            # Parse creation date
            if isinstance(record['createdAt'], str):
                created_at = datetime.fromisoformat(record['createdAt'].replace('Z', '+00:00'))
            else:
                created_at = record['createdAt']
            
            year = created_at.year
            month = f"{created_at.month:02d}"
            day = f"{created_at.day:02d}"
            customer_id = record.get('customerId', 'unknown')
            
            # Sanitize customer ID for file path
            safe_customer_id = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in customer_id)
            
            return f"{tier}/{year}/{month}/{day}/{safe_customer_id}/{record['id']}.json"
            
        except Exception as e:
            logger.error(f"Error generating blob path for record {record.get('id', 'unknown')}: {e}")
            # Fallback path
            return f"{tier}/error/{record['id']}.json"
    
    def _store_in_blob(self, record, container_name, blob_path):
        """Store record in blob storage with metadata"""
        try:
            blob_client = self.blob_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            
            # Convert record to JSON with proper date handling
            record_json = json.dumps(record, indent=2, default=self._json_serializer)
            record_bytes = record_json.encode('utf-8')
            blob_size = len(record_bytes)
            
            # Upload with metadata and proper content type
            blob_client.upload_blob(
                record_bytes,
                overwrite=True,
                content_settings={
                    'content_type': 'application/json',
                    'content_encoding': 'utf-8'
                },
                metadata={
                    'record_id': record['id'],
                    'customer_id': record.get('customerId', ''),
                    'created_at': str(record.get('createdAt', '')),
                    'migrated_at': datetime.utcnow().isoformat(),
                    'original_size': str(blob_size),
                    'storage_tier': container_name.replace('billing-', '')
                }
            )
            
            return blob_size
            
        except Exception as e:
            logger.error(f"Error storing blob {blob_path}: {e}")
            raise
    
    def _retrieve_from_blob(self, container_name, blob_path):
        """Retrieve record from blob storage"""
        try:
            blob_client = self.blob_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            
            blob_data = blob_client.download_blob().readall()
            record = json.loads(blob_data.decode('utf-8'))
            return record
            
        except Exception as e:
            logger.error(f"Error retrieving blob {blob_path}: {e}")
            return None
    
    def _delete_from_blob(self, container_name, blob_path):
        """Delete record from blob storage"""
        try:
            blob_client = self.blob_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            blob_client.delete_blob(delete_snapshots="include")
            logger.debug(f"Deleted blob: {blob_path}")
            
        except Exception as e:
            logger.error(f"Error deleting blob {blob_path}: {e}")
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for datetime objects"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def run_full_migration(self):
        """Run complete migration process for both tiers"""
        self.stats['start_time'] = datetime.utcnow()
        
        logger.info("Starting full tiered migration process")
        
        try:
            # Migrate 1-month old data from Cosmos to Hot Blob
            self.migrate_tier1_to_tier2()
            
            # Migrate 3-month old data from Hot Blob to Cold Blob
            self.migrate_tier2_to_tier3()
            
        except Exception as e:
            logger.error(f"Migration process failed: {e}")
            raise
        finally:
            self.stats['end_time'] = datetime.utcnow()
            self._print_migration_summary()
    
    def _print_migration_summary(self):
        """Print detailed migration summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        total_success = self.stats['tier1_to_tier2']['success'] + self.stats['tier2_to_tier3']['success']
        total_failed = self.stats['tier1_to_tier2']['failed'] + self.stats['tier2_to_tier3']['failed']
        size_mb = self.stats['total_size_migrated'] / (1024 * 1024)
        
        print("\n" + "="*60)
        print("MIGRATION SUMMARY")
        print("="*60)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Total Records Processed: {total_success + total_failed}")
        print(f"Successfully Migrated: {total_success}")
        print(f"Failed Migrations: {total_failed}")
        print(f"Total Data Migrated: {size_mb:.2f} MB")
        print()
        print("Tier 1 → Tier 2 (Cosmos → Hot Blob):")
        print(f"  Success: {self.stats['tier1_to_tier2']['success']}")
        print(f"  Failed: {self.stats['tier1_to_tier2']['failed']}")
        print()
        print("Tier 2 → Tier 3 (Hot Blob → Cold Blob):")
        print(f"  Success: {self.stats['tier2_to_tier3']['success']}")
        print(f"  Failed: {self.stats['tier2_to_tier3']['failed']}")
        
        if total_failed > 0:
            print("\nFirst 5 Errors:")
            all_errors = self.stats['tier1_to_tier2']['errors'] + self.stats['tier2_to_tier3']['errors']
            for i, error in enumerate(all_errors[:5]):
                print(f"  {i+1}. {error}")
        
        print("="*60)

def main():
    """Main execution function"""
    print("🚀 Starting Azure Tiered Storage Migration")
    print("This will migrate billing records across storage tiers based on age")
    print()
    
    # Validate environment variables
    required_vars = ['COSMOS_ENDPOINT', 'COSMOS_KEY', 'AZURE_STORAGE_CONNECTION_STRING']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these environment variables before running the migration.")
        return
    
    try:
        # Initialize and run migration
        transfer = TieredDataTransfer()
        transfer.run_full_migration()
        
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        logger.error(f"Migration failed with error: {e}")

if __name__ == "__main__":
    main()
