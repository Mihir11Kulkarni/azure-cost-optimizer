# Tiered Storage Architecture

## Overview

This solution implements a **3-tier storage architecture** that automatically manages billing records based on their age and access patterns. The system provides transparent data retrieval while optimizing costs through intelligent data placement.

## Storage Tiers

### Tier 1: Cosmos DB (0-1 month)
- **Purpose**: Recent data requiring fast access
- **Performance**: 1-5ms response time
- **Cost**: ~$50/month for recent data
- **Use Case**: Current billing period data
- **Technology**: Azure Cosmos DB with provisioned throughput

### Tier 2: Hot Blob Storage (1-3 months)
- **Purpose**: Frequently accessed historical data
- **Performance**: 100-500ms response time
- **Cost**: ~$30/month for 1-3 month data
- **Use Case**: Previous billing periods
- **Technology**: Azure Blob Storage Hot tier

### Tier 3: Cold Blob Storage (3+ months)
- **Purpose**: Infrequently accessed archived data
- **Performance**: 1-2 seconds response time
- **Cost**: ~$15/month for old data
- **Use Case**: Historical records for compliance
- **Technology**: Azure Blob Storage Cool tier

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CLIENT APPLICATIONS                       │
│              (No changes required)                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                 RETRIEVAL API                               │
│         (Transparent multi-tier search)                    │
└─────────────────────┬───────────────────────────────────────┘
                      │
      ┌───────────────┼───────────────┐
      │               │               │
      ▼               ▼               ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│  TIER 1  │    │  TIER 2  │    │  TIER 3  │
│ Cosmos   │    │   Hot    │    │  Cold    │
│    DB    │    │  Blob    │    │  Blob    │
│          │    │          │    │          │
│ 0-1 mo   │    │ 1-3 mo   │    │ 3+ mo    │
│ <10ms    │    │ <500ms   │    │ <2000ms  │
│ $50/mo   │    │ $30/mo   │    │ $15/mo   │
└──────────┘    └──────────┘    └──────────┘
      ▲               ▲               ▲
      │               │               │
┌─────┴───────────────┴───────────────┴─────┐
│           MIGRATION SCHEDULER              │
│    (Automated data movement)               │
│                                           │
│ • Daily: 1-month → Hot Blob               │
│ • Weekly: 3-month → Cold Blob             │
│ • Lifecycle: Hot → Cold (90 days)         │
└───────────────────────────────────────────┘
```

## Data Lifecycle

### Stage 1: Data Creation (Day 0)
```
New Record → Cosmos DB (Tier 1)
- Immediate availability
- High performance for recent queries
- Full CRUD operations supported
```

### Stage 2: First Migration (Day 30)
```
Cosmos DB → Hot Blob Storage (Tier 2)
- Automated daily check
- JSON serialization
- Metadata preserved in Cosmos DB for tracking
```

### Stage 3: Second Migration (Day 90)
```
Hot Blob → Cold Blob Storage (Tier 3)
- Automated weekly check
- Lower cost storage
- Slightly slower retrieval
```

### Stage 4: Long-term Storage (Day 90+)
```
Cold Blob Storage (Permanent)
- Compliance requirements met
- Cost-optimized storage
- On-demand retrieval available
```

## Automatic Retrieval Logic

```python
def get_record(record_id):
    # Step 1: Try Tier 1 first (fastest)
    record = search_cosmos_db(record_id)
    if record and not record.migrated:
        return record, "tier1-cosmos", ~10ms
    
    # Step 2: Try Tier 2 (medium speed)
    if record.storage_location == "hot_blob":
        blob_record = search_hot_blob(record.blob_path)
        if blob_record:
            return blob_record, "tier2-hot", ~500ms
    
    # Step 3: Try Tier 3 (slower but acceptable)
    if record.storage_location == "cold_blob":
        blob_record = search_cold_blob(record.blob_path)
        if blob_record:
            return blob_record, "tier3-cold", ~2000ms
    
    return None, "not-found", 0ms
```

## Migration Strategies

### Strategy 1: Batch Migration
- **When**: Daily at 2 AM (low traffic)
- **Scope**: All records meeting age criteria
- **Batch Size**: 1000 records per batch
- **Error Handling**: Individual record failure doesn't stop batch

### Strategy 2: Real-time Migration
- **When**: On-demand during low usage
- **Scope**: Specific records or date ranges
- **Use Case**: Manual optimization or testing

### Strategy 3: Lifecycle Policies
- **When**: Automated by Azure Blob Storage
- **Scope**: Hot → Cold transitions
- **Configuration**: JSON policy file
- **Monitoring**: Azure Monitor alerts

## Cost Analysis

### Current State (Single Tier)
```
2M records × 300KB × $0.25/GB/month = $150/month
All data in expensive Cosmos DB storage
```

### Optimized State (Three Tier)
```
Tier 1: 100K records × $0.25/GB = $7.50/month  (recent)
Tier 2: 400K records × $0.15/GB = $18/month    (1-3 months)
Tier 3: 1.5M records × $0.08/GB = $36/month    (3+ months)
Total: $61.50/month (59% savings)
```

## Performance Characteristics

| Tier | Storage Type | Avg Response | 95th Percentile | Availability |
|------|-------------|--------------|-----------------|--------------|
| 1 | Cosmos DB | 8ms | 25ms | 99.99% |
| 2 | Hot Blob | 200ms | 800ms | 99.9% |
| 3 | Cold Blob | 1200ms | 3000ms | 99.9% |

## Disaster Recovery

### Backup Strategy
- **Tier 1**: Cosmos DB automatic backup (35 days)
- **Tier 2**: Blob Storage geo-redundant storage
- **Tier 3**: Blob Storage geo-redundant storage + versioning

### Recovery Scenarios
1. **Cosmos DB failure**: Retrieve from blob storage tiers
2. **Blob Storage failure**: Geo-redundant automatic failover
3. **Complete region failure**: Cross-region replication available

## Monitoring and Alerting

### Key Metrics
- **Response Time**: By tier and overall
- **Success Rate**: Retrieval success percentage
- **Cost**: Monthly spend by tier
- **Migration Status**: Success/failure rates

### Alerts
- Response time > 5 seconds
- Success rate < 99%
- Migration failures > 5%
- Cost increase > 20%

## Scalability

### Horizontal Scaling
- **Cosmos DB**: Auto-scaling based on RU consumption
- **Blob Storage**: Virtually unlimited storage capacity
- **Processing**: Azure Functions scale automatically

### Vertical Scaling
- **Cosmos DB**: Increase provisioned throughput as needed
- **Blob Storage**: Access tier optimization based on patterns
- **Compute**: Premium function plans for guaranteed performance
