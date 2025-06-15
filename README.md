According to the problem, cost optimization can be done by using Blob Storage service of Azure, which provides service to store huge amount of unstructured data at a feasible cost.

In Blob Storage we can create lifecycle management policy for the data of different duration to cut the cost incurred by the organization.

As of now, if there 2 million records of 300 KB each then organization is paying for 600 GB of data which will cost around $150/per month. Data can be divided in different storage according to the lifecycle policy.

## Lifecycle Management Policy

First of all, talking about the whole data, if all data is 3 months old, organization can follow lifecycle policy if new data is also there then following steps:

**Up to 1 month**: Data up to 1 month can be stored in Cosmos DB, as person needs fast access for that to see the cost every month.

**1 month – 3 month**: Then data older than 1 month but not 3 months will be stored in hot tier, so that it can be accessed easily and little frequently.

**3+ months**: Data older than 3 months will be stored in cold tier, as it will be accessed but not frequently.

We are not using archive tier since data will be retrieved even though rarely, and retrieval cost is higher in archive tier.

## Tiered Storage Architecture

```
┌─────────────────┐
│   Client API    │ ← No changes to existing API
│   (Unchanged)   │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Retrieval Logic │ ← Searches all tiers automatically
└─────────────────┘
         │
    ┌────┴────┬────────────┐
    ▼         ▼            ▼
┌─────────┐ ┌──────────┐ ┌──────────┐
│ Tier 1  │ │  Tier 2  │ │  Tier 3  │
│Cosmos DB│ │Hot Blob  │ │Cold Blob │
│0-1 month│ │1-3 months│ │3+ months │
│~$50/mo  │ │ ~$30/mo  │ │ ~$15/mo  │
└─────────┘ └──────────┘ └──────────┘
```

## Cost Optimization Results

| Timeframe | Current Storage | Proposed Storage | Monthly Cost |
|-----------|----------------|------------------|--------------|
| 0-1 month | Cosmos DB | Cosmos DB | $50 |
| 1-3 months | Cosmos DB | Hot Blob | $30 |
| 3+ months | Cosmos DB | Cold Blob | $15 |
| **Total** | **$150** | **$95** | **37% savings** |

## Files Description

- `Transfer_data_from_cosmo-db_to_blob-storage.py` - Automated migration script
- `Retrieval.py` - Smart retrieval from all storage tiers
- `Blob_Storage_Policy.json` - Lifecycle policy for automatic hot→cold transition
- `setup.py` - One-click setup script

## Quick Setup

1. Clone this repository
2. Run `python setup.py` to configure Azure resources
3. Schedule the transfer script to run monthly
4. Your API will automatically retrieve from all tiers

## Usage

```python
# Your existing API code doesn't change
from Retrieval import get_billing_record

# This automatically searches Cosmos DB → Hot Blob → Cold Blob
record = get_billing_record("record-id-123")
```

**Note**: Optimization strategies are my own - ChatGPT used purely for automation code and system design.

