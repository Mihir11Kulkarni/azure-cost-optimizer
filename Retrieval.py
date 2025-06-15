import json
import os
import time
from datetime import datetime
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TieredRetrieval:
    def __init__(self):
        """Initialize connections to all storage tiers"""
        # Cosmos DB configuration (Tier 1)
        self.cosmos_client = CosmosClient(
            url=os.getenv('COSMOS_ENDPOINT'),
            credential=os.getenv('COSMOS_KEY')
        )
        self.database = self.cosmos_client.get_database_client(os.getenv('COSMOS_DATABASE', 'billing-database'))
        self.container = self.database.get_container_client('billing-records')
        
        # Blob Storage configuration (Tier 2 & 3)
        self.blob_client = BlobServiceClient.from_connection_string(
            os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        )
        self.hot_container = 'billing-hot'
        self.cold_container = 'billing-cold'
        
        # Performance tracking
        self.performance_stats = {
            'tier1_hits': 0,
            'tier2_hits': 0,
            'tier3_hits': 0,
            'cache_misses': 0,
            'average_response_times': {
                'tier1': [],
                'tier2': [],
                'tier3': []
            }
        }
    
    def get_billing_record(self, record_id):
        """
        Smart retrieval that searches all tiers automatically
        Returns: (record_data, source_tier, response_time_ms)
        """
        start_time = time.time()
        
        logger.info(f"🔍 Searching for record {record_id} across all storage tiers")
        
        # Tier 1: Search Cosmos DB first (fastest - 1-10ms)
        logger.debug(f"Searching Tier 1 (Cosmos DB) for record {record_id}")
        record, tier1_time = self._search_cosmos_db(record_id)
        
        if record and self._is_record_in_tier1(record):
            # Record found in Tier 1 and hasn't been migrated
            response_time = (time.time() - start_time) * 1000
            self.performance_stats['tier1_hits'] += 1
            self.performance_stats['average_response_times']['tier1'].append(response_time)
            
            logger.info(f"✅ Found record {record_id} in Tier 1 (Cosmos DB) - {response_time:.2f}ms")
            return record, 'tier1-cosmos', response_time
        
        # Tier 2: Search Hot Blob Storage (medium speed - 100-500ms)
        if record and record.get('storage_tier') == 'hot_blob':
            logger.debug(f"Searching Tier 2 (Hot Blob) for record {record_id}")
            blob_record, tier2_time = self._search_hot_blob(record.get('blob_path'))
            
            if blob_record:
                response_time = (time.time() - start_time) * 1000
                self.performance_stats['tier2_hits'] += 1
                self.performance_stats['average_response_times']['tier2'].append(response_time)
                
                logger.info(f"✅ Found record {record_id} in Tier 2 (Hot Blob) - {response_time:.2f}ms")
                return blob_record, 'tier2-hot', response_time
        
        # Tier 3: Search Cold Blob Storage (slower - 1-3 seconds)
        if record and record.get('storage_tier') == 'cold_blob':
            logger.debug(f"Searching Tier 3 (Cold Blob) for record {record_id}")
            blob_record, tier3_time = self._search_cold_blob(record.get('blob_path'))
            
            if blob_record:
                response_time = (time.time() - start_time) * 1000
                self.performance_stats['tier3_hits'] += 1
                self.performance_stats['average_response_times']['tier3'].append(response_time)
                
                logger.info(f"✅ Found record {record_id} in Tier 3 (Cold Blob) - {response_time:.2f}ms")
                return blob_record, 'tier3-cold', response_time
        
        # Not found in any tier
        response_time = (time.time() - start_time) * 1000
        self.performance_stats['cache_misses'] += 1
        
        logger.warning(f"❌ Record {record_id} not found in any storage tier - {response_time:.2f}ms")
        return None, 'not-found', response_time
    
    def _search_cosmos_db(self, record_id):
        """Search for record in Cosmos DB (Tier 1)"""
        search_start = time.time()
        
        try:
            logger.debug(f"Querying Cosmos DB for record {record_id}")
            item = self.container.read_item(item=record_id, partition_key=record_id)
            search_time = (time.time() - search_start) * 1000
            
            logger.debug(f"Cosmos DB query completed in {search_time:.2f}ms")
            return item, search_time
            
        except exceptions.CosmosResourceNotFoundError:
            search_time = (time.time() - search_start) * 1000
            logger.debug(f"Record {record_id} not found in Cosmos DB ({search_time:.2f}ms)")
            return None, search_time
            
        except Exception as e:
            search_time = (time.time() - search_start) * 1000
            logger.error(f"Error searching Cosmos DB for {record_id}: {e} ({search_time:.2f}ms)")
            return None, search_time
    
    def _search_hot_blob(self, blob_path):
        """Search for record in Hot Blob Storage (Tier 2)"""
        if not blob_path:
            return None, 0
            
        search_start = time.time()
        
        try:
            logger.debug(f"Retrieving from Hot Blob Storage: {blob_path}")
            blob_client = self.blob_client.get_blob_client(
                container=self.hot_container,
                blob=blob_path
            )
            
            blob_data = blob_client.download_blob().readall()
            record = json.loads(blob_data.decode('utf-8'))
            search_time = (time.time() - search_start) * 1000
            
            logger.debug(f"Hot blob retrieval completed in {search_time:.2f}ms")
            return record, search_time
            
        except Exception as e:
            search_time = (time.time() - search_start) * 1000
            logger.error(f"Error retrieving from hot blob {blob_path}: {e} ({search_time:.2f}ms)")
            return None, search_time
    
    def _search_cold_blob(self, blob_path):
        """Search for record in Cold Blob Storage (Tier 3)"""
        if not blob_path:
            return None, 0
            
        search_start = time.time()
        
        try:
            logger.debug(f"Retrieving from Cold Blob Storage: {blob_path}")
            blob_client = self.blob_client.get_blob_client(
                container=self.cold_container,
                blob=blob_path
            )
            
            blob_data = blob_client.download_blob().readall()
            record = json.loads(blob_data.decode('utf-8'))
            search_time = (time.time() - search_start) * 1000
            
            logger.debug(f"Cold blob retrieval completed in {search_time:.2f}ms")
            return record, search_time
            
        except Exception as e:
            search_time = (time.time() - search_start) * 1000
            logger.error(f"Error retrieving from cold blob {blob_path}: {e} ({search_time:.2f}ms)")
            return None, search_time
    
    def _is_record_in_tier1(self, record):
        """Check if record is still in Tier 1 (not migrated)"""
        storage_tier = record.get('storage_tier')
        return storage_tier is None or storage_tier == 'cosmos'
    
    def get_multiple_records(self, record_ids):
        """
        Retrieve multiple records efficiently
        Returns: list of (record_id, record_data, source_tier, response_time)
        """
        logger.info(f"🔍 Retrieving {len(record_ids)} records from tiered storage")
        results = []
        
        for record_id in record_ids:
            record, source, response_time = self.get_billing_record(record_id)
            results.append((record_id, record, source, response_time))
        
        return results
    
    def get_records_by_customer(self, customer_id, limit=100):
        """
        Get records for a specific customer across all tiers
        Returns: list of records sorted by creation date (newest first)
        """
        logger.info(f"🔍 Searching for records by customer {customer_id}")
        
        all_records = []
        
        try:
            # Query Cosmos DB for all records (including migrated ones for metadata)
            query = "SELECT * FROM c WHERE c.customerId = @customer_id ORDER BY c.createdAt DESC"
            parameters = [{"name": "@customer_id", "value": customer_id}]
            
            items = list(self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=limit
            ))
            
            logger.info(f"Found {len(items)} records for customer {customer_id}")
            
            # For each record, get the full data from appropriate tier
            for item in items:
                if self._is_record_in_tier1(item):
                    # Record is in Tier 1 (Cosmos DB)
                    all_records.append({
                        'record': item,
                        'source': 'tier1-cosmos',
                        'record_id': item['id']
                    })
                elif item.get('storage_tier') == 'hot_blob':
                    # Record is in Tier 2 (Hot Blob)
                    blob_record, _ = self._search_hot_blob(item.get('blob_path'))
                    if blob_record:
                        all_records.append({
                            'record': blob_record,
                            'source': 'tier2-hot',
                            'record_id': item['id']
                        })
                elif item.get('storage_tier') == 'cold_blob':
                    # Record is in Tier 3 (Cold Blob)
                    blob_record, _ = self._search_cold_blob(item.get('blob_path'))
                    if blob_record:
                        all_records.append({
                            'record': blob_record,
                            'source': 'tier3-cold',
                            'record_id': item['id']
                        })
            
            return all_records
            
        except Exception as e:
            logger.error(f"Error retrieving records for customer {customer_id}: {e}")
            return []
    
    def get_storage_statistics(self):
        """
        Get statistics about data distribution across storage tiers
        """
        logger.info("📊 Gathering storage tier statistics")
        
        stats = {
            'tier1_cosmos': 0,
            'tier2_hot_blob': 0,
            'tier3_cold_blob': 0,
            'total_records': 0,
            'performance': self._get_performance_stats()
        }
        
        try:
            # Query record counts by storage tier
            query = """
            SELECT 
                c.storage_tier,
                COUNT(1) as record_count
            FROM c 
            GROUP BY c.storage_tier
            """
            
            items = list(self.container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            for item in items:
                tier = item.get('storage_tier', 'cosmos')
                count = item.get('record_count', 0)
                
                if tier in [None, 'cosmos']:
                    stats['tier1_cosmos'] = count
                elif tier == 'hot_blob':
                    stats['tier2_hot_blob'] = count
                elif tier == 'cold_blob':
                    stats['tier3_cold_blob'] = count
                
                stats['total_records'] += count
            
            logger.info(f"Storage statistics gathered: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting storage statistics: {e}")
            return stats
    
    def _get_performance_stats(self):
        """Calculate performance statistics"""
        stats = {
            'total_requests': sum([
                self.performance_stats['tier1_hits'],
                self.performance_stats['tier2_hits'],
                self.performance_stats['tier3_hits'],
                self.performance_stats['cache_misses']
            ]),
            'hit_distribution': {
                'tier1_percentage': 0,
                'tier2_percentage': 0,
                'tier3_percentage': 0,
                'miss_percentage': 0
            },
            'average_response_times': {}
        }
        
        total = stats['total_requests']
        if total > 0:
            stats['hit_distribution']['tier1_percentage'] = (self.performance_stats['tier1_hits'] / total) * 100
            stats['hit_distribution']['tier2_percentage'] = (self.performance_stats['tier2_hits'] / total) * 100
            stats['hit_distribution']['tier3_percentage'] = (self.performance_stats['tier3_hits'] / total) * 100
            stats['hit_distribution']['miss_percentage'] = (self.performance_stats['cache_misses'] / total) * 100
        
        # Calculate average response times
        for tier, times in self.performance_stats['average_response_times'].items():
            if times:
                stats['average_response_times'][tier] = sum(times) / len(times)
            else:
                stats['average_response_times'][tier] = 0
        
        return stats

# Convenience functions for easy usage
def get_billing_record(record_id):
    """
    Simple function to get a billing record from any storage tier
    Usage: record = get_billing_record("record-id-123")
    """
    retrieval = TieredRetrieval()
    record, source, response_time = retrieval.get_billing_record(record_id)
    return record

def get_billing_records(record_ids):
    """
    Get multiple billing records at once
    Usage: records = get_billing_records(["id1", "id2", "id3"])
    """
    retrieval = TieredRetrieval()
    return retrieval.get_multiple_records(record_ids)

def get_customer_records(customer_id, limit=100):
    """
    Get all records for a specific customer
    Usage: records = get_customer_records("customer-123")
    """
    retrieval = TieredRetrieval()
    return retrieval.get_records_by_customer(customer_id, limit)

def get_storage_stats():
    """
    Get current storage tier statistics
    Usage: stats = get_storage_stats()
    """
    retrieval = TieredRetrieval()
    return retrieval.get_storage_statistics()

# Example usage and testing
if __name__ == "__main__":
    print("🔍 Testing Tiered Storage Retrieval")
    print("=" * 50)
    
    # Validate environment variables
    required_vars = ['COSMOS_ENDPOINT', 'COSMOS_KEY', 'AZURE_STORAGE_CONNECTION_STRING']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these environment variables before testing retrieval.")
        exit(1)
    
    # Test single record retrieval
    test_record_id = input("Enter a record ID to test (or press Enter to skip): ").strip()
    
    if test_record_id:
        print(f"\n🔍 Testing retrieval for record: {test_record_id}")
        record, source, response_time = TieredRetrieval().get_billing_record(test_record_id)
        
        if record:
            print(f"✅ Found record in {source} (took {response_time:.2f}ms)")
            print(f"   Customer: {record.get('customerId')}")
            print(f"   Amount: {record.get('amount')} {record.get('currency')}")
            print(f"   Created: {record.get('createdAt')}")
        else:
            print(f"❌ Record {test_record_id} not found in any tier")
    
    # Get storage statistics
    print(f"\n📊 Current Storage Distribution:")
    stats = get_storage_stats()
    print(f"   Tier 1 (Cosmos DB): {stats['tier1_cosmos']:,} records")
    print(f"   Tier 2 (Hot Blob): {stats['tier2_hot_blob']:,} records")
    print(f"   Tier 3 (Cold Blob): {stats['tier3_cold_blob']:,} records")
    print(f"   Total: {stats['total_records']:,} records")
    
    # Performance statistics
    perf = stats['performance']
    if perf['total_requests'] > 0:
        print(f"\n⚡ Performance Statistics:")
        print(f"   Total Requests: {perf['total_requests']}")
        print(f"   Tier 1 Hits: {perf['hit_distribution']['tier1_percentage']:.1f}%")
        print(f"   Tier 2 Hits: {perf['hit_distribution']['tier2_percentage']:.1f}%")
        print(f"   Tier 3 Hits: {perf['hit_distribution']['tier3_percentage']:.1f}%")
        print(f"   Cache Misses: {perf['hit_distribution']['miss_percentage']:.1f}%")
        
        print(f"\n🕐 Average Response Times:")
        for tier, avg_time in perf['average_response_times'].items():
            print(f"   {tier.replace('tier', 'Tier ')}: {avg_time:.2f}ms")
    
    print("\n✅ Retrieval system test completed!")
