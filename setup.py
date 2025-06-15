import os
import sys
import json
import time
import subprocess
from datetime import datetime

class AzureTieredStorageSetup:
    def __init__(self):
        self.resource_group = f"billing-tiered-storage-{int(time.time())}"
        self.location = "eastus"
        self.cosmos_account = f"billing-cosmos-{int(time.time())}"
        self.storage_account = f"billing{str(int(time.time()))[-8:]}"  # Keep it short
        self.cosmos_database = "billing-database"
        self.cosmos_container = "billing-records"
        
        self.created_resources = []
    
    def check_prerequisites(self):
        """Check if Azure CLI is installed and user is logged in"""
        print("🔍 Checking prerequisites...")
        
        try:
            # Check Azure CLI installation
            result = subprocess.run(['az', '--version'], capture_output=True, text=True)
            if result.returncode != 0:
                print("❌ Azure CLI is not installed. Please install it first.")
                print("   Visit: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli")
                return False
            
            # Check if logged in
            result = subprocess.run(['az', 'account', 'show'], capture_output=True, text=True)
            if result.returncode != 0:
                print("❌ Not logged into Azure. Please run 'az login' first.")
                return False
            
            account_info = json.loads(result.stdout)
            print(f"✅ Logged into Azure as: {account_info.get('user', {}).get('name', 'Unknown')}")
            print(f"✅ Subscription: {account_info.get('name', 'Unknown')}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error checking prerequisites: {e}")
            return False
    
    def create_resource_group(self):
        """Create Azure Resource Group"""
        print(f"📦 Creating resource group: {self.resource_group}")
        
        cmd = [
            'az', 'group', 'create',
            '--name', self.resource_group,
            '--location', self.location
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Resource group created successfully")
            self.created_resources.append(f"Resource Group: {self.resource_group}")
            return True
        else:
            print(f"❌ Failed to create resource group: {result.stderr}")
            return False
    
    def create_cosmos_db(self):
        """Create Cosmos DB account, database, and container"""
        print(f"🌌 Creating Cosmos DB account: {self.cosmos_account}")
        
        # Create Cosmos DB account
        cmd = [
            'az', 'cosmosdb', 'create',
            '--resource-group', self.resource_group,
            '--name', self.cosmos_account,
            '--default-consistency-level', 'Session',
            '--locations', f'regionName={self.location}',
            '--enable-automatic-failover'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create Cosmos DB account: {result.stderr}")
            return False
        
        print("✅ Cosmos DB account created")
        self.created_resources.append(f"Cosmos DB Account: {self.cosmos_account}")
        
        # Create database
        print(f"📚 Creating database: {self.cosmos_database}")
        cmd = [
            'az', 'cosmosdb', 'sql', 'database', 'create',
            '--resource-group', self.resource_group,
            '--account-name', self.cosmos_account,
            '--name', self.cosmos_database
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create database: {result.stderr}")
            return False
        
        print("✅ Database created")
        
        # Create container
        print(f"📋 Creating container: {self.cosmos_container}")
        cmd = [
            'az', 'cosmosdb', 'sql', 'container', 'create',
            '--resource-group', self.resource_group,
            '--account-name', self.cosmos_account,
            '--database-name', self.cosmos_database,
            '--name', self.cosmos_container,
            '--partition-key-path', '/id',
            '--throughput', '400'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create container: {result.stderr}")
            return False
        
        print("✅ Container created")
        return True
    
    def create_storage_account(self):
        """Create Storage Account with Hot and Cool tiers"""
        print(f"💾 Creating storage account: {self.storage_account}")
        
        cmd = [
            'az', 'storage', 'account', 'create',
            '--resource-group', self.resource_group,
            '--name', self.storage_account,
            '--location', self.location,
            '--sku', 'Standard_LRS',
            '--access-tier', 'Hot',
            '--kind', 'StorageV2'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create storage account: {result.stderr}")
            return False
        
        print("✅ Storage account created")
        self.created_resources.append(f"Storage Account: {self.storage_account}")
        
        # Create blob containers
        containers = ['billing-hot', 'billing-cold']
        
        for container in containers:
            print(f"📦 Creating blob container: {container}")
            cmd = [
                'az', 'storage', 'container', 'create',
                '--name', container,
                '--account-name', self.storage_account,
                '--public-access', 'off'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"❌ Failed to create container {container}: {result.stderr}")
                return False
            
            print(f"✅ Container {container} created")
        
        return True
    
    def setup_lifecycle_policy(self):
        """Apply lifecycle management policy to storage account"""
        print("📋 Setting up blob lifecycle management policy...")
        
        # Read the policy file
        try:
            with open('Blob_Storage_Policy.json', 'r') as f:
                policy = json.load(f)
        except FileNotFoundError:
            print("⚠️  Blob_Storage_Policy.json not found, creating default policy...")
            policy = {
                "rules": [
                    {
                        "name": "MoveHotToColdAfter90Days",
                        "enabled": True,
                        "type": "Lifecycle",
                        "definition": {
                            "filters": {
                                "blobTypes": ["blockBlob"],
                                "prefixMatch": ["billing-hot/"]
                            },
                            "actions": {
                                "baseBlob": {
                                    "tierToCool": {
                                        "daysAfterModificationGreaterThan": 90
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        
        # Write policy to temporary file
        policy_file = 'temp_policy.json'
        with open(policy_file, 'w') as f:
            json.dump(policy, f, indent=2)
        
        try:
            cmd = [
                'az', 'storage', 'account', 'management-policy', 'create',
                '--account-name', self.storage_account,
                '--resource-group', self.resource_group,
                '--policy', f'@{policy_file}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Lifecycle policy applied successfully")
            else:
                print(f"⚠️  Warning: Failed to apply lifecycle policy: {result.stderr}")
        finally:
            # Clean up temporary file
            if os.path.exists(policy_file):
                os.remove(policy_file)
        
        return True
    
    def get_connection_strings(self):
        """Retrieve connection strings for created resources"""
        print("🔗 Retrieving connection strings...")
        
        connection_info = {}
        
        # Get Cosmos DB connection details
        try:
            cmd = [
                'az', 'cosmosdb', 'show',
                '--resource-group', self.resource_group,
                '--name', self.cosmos_account,
                '--query', 'documentEndpoint',
                '--output', 'tsv'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                connection_info['cosmos_endpoint'] = result.stdout.strip()
            
            cmd = [
                'az', 'cosmosdb', 'keys', 'list',
                '--resource-group', self.resource_group,
                '--name', self.cosmos_account,
                '--query', 'primaryMasterKey',
                '--output', 'tsv'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                connection_info['cosmos_key'] = result.stdout.strip()
        
        except Exception as e:
            print(f"⚠️  Warning: Could not retrieve Cosmos DB connection info: {e}")
        
        # Get Storage Account"""
One-click setup script for Azure Tiered Storage
Creates all necessary Azure resources and configurations
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime

class AzureTieredStorageSetup:
    def __init__(self):
        self.resource_group = f"billing-tiered-storage-{int(time.time())}"
        self.location = "eastus"
        self.cosmos_account = f"billing-cosmos-{int(time.time())}"
        self.storage_account = f"billing{str(int(time.time()))[-8:]}"  # Keep it short
        self.cosmos_database = "billing-database"
        self.cosmos_container = "billing-records"
        
        self.created_resources = []
    
    def check_prerequisites(self):
        """Check if Azure CLI is installed and user is logged in"""
        print("🔍 Checking prerequisites...")
        
        try:
            # Check Azure CLI installation
            result = subprocess.run(['az', '--version'], capture_output=True, text=True)
            if result.returncode != 0:
                print("❌ Azure CLI is not installed. Please install it first.")
                print("   Visit: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli")
                return False
            
            # Check if logged in
            result = subprocess.run(['az', 'account', 'show'], capture_output=True, text=True)
            if result.returncode != 0:
                print("❌ Not logged into Azure. Please run 'az login' first.")
                return False
            
            account_info = json.loads(result.stdout)
            print(f"✅ Logged into Azure as: {account_info.get('user', {}).get('name', 'Unknown')}")
            print(f"✅ Subscription: {account_info.get('name', 'Unknown')}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error checking prerequisites: {e}")
            return False
    
    def create_resource_group(self):
        """Create Azure Resource Group"""
        print(f"📦 Creating resource group: {self.resource_group}")
        
        cmd = [
            'az', 'group', 'create',
            '--name', self.resource_group,
            '--location', self.location
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Resource group created successfully")
            self.created_resources.append(f"Resource Group: {self.resource_group}")
            return True
        else:
            print(f"❌ Failed to create resource group: {result.stderr}")
            return False
    
    def create_cosmos_db(self):
        """Create Cosmos DB account, database, and container"""
        print(f"🌌 Creating Cosmos DB account: {self.cosmos_account}")
        
        # Create Cosmos DB account
        cmd = [
            'az', 'cosmosdb', 'create',
            '--resource-group', self.resource_group,
            '--name', self.cosmos_account,
            '--default-consistency-level', 'Session',
            '--locations', f'regionName={self.location}',
            '--enable-automatic-failover'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create Cosmos DB account: {result.stderr}")
            return False
        
        print("✅ Cosmos DB account created")
        self.created_resources.append(f"Cosmos DB Account: {self.cosmos_account}")
        
        # Create database
        print(f"📚 Creating database: {self.cosmos_database}")
        cmd = [
            'az', 'cosmosdb', 'sql', 'database', 'create',
            '--resource-group', self.resource_group,
            '--account-name', self.cosmos_account,
            '--name', self.cosmos_database
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create database: {result.stderr}")
            return False
        
        print("✅ Database created")
        
        # Create container
        print(f"📋 Creating container: {self.cosmos_container}")
        cmd = [
            'az', 'cosmosdb', 'sql', 'container', 'create',
            '--resource-group', self.resource_group,
            '--account-name', self.cosmos_account,
            '--database-name', self.cosmos_database,
            '--name', self.cosmos_container,
            '--partition-key-path', '/id',
            '--throughput', '400'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create container: {result.stderr}")
            return False
        
        print("✅ Container created")
        return True
    
    def create_storage_account(self):
        """Create Storage Account with Hot and Cool tiers"""
        print(f"💾 Creating storage account: {self.storage_account}")
        
        cmd = [
            'az', 'storage', 'account', 'create',
            '--resource-group', self.resource_group,
            '--name', self.storage_account,
            '--location', self.location,
            '--sku', 'Standard_LRS',
            '--access-tier', 'Hot',
            '--kind', 'StorageV2'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to create storage account: {result.stderr}")
            return False
        
        print("✅ Storage account created")
        self.created_resources.append(f"Storage Account: {self.storage_account}")
        
        # Create blob containers
        containers = ['billing-hot', 'billing-cold']
        
        for container in containers:
            print(f"📦 Creating blob container: {container}")
            cmd = [
                'az', 'storage', 'container', 'create',
                '--name', container,
                '--account-name', self.storage_account,
                '--public-access', 'off'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"❌ Failed to create container {container}: {result.stderr}")
                return False
            
            print(f"✅ Container {container} created")
        
        return True
    
    def setup_lifecycle_policy(self):
        """Apply lifecycle management policy to storage account"""
        print("📋 Setting up blob lifecycle management policy...")
        
        # Read the policy file
        try:
            with open('Blob_Storage_Policy.json', 'r') as f:
                policy = json.load(f)
        except FileNotFoundError:
            print("⚠️  Blob_Storage_Policy.json not found, creating default policy...")
            policy = {
                "rules": [
                    {
                        "name": "MoveHotToColdAfter90Days",
                        "enabled": True,
                        "type": "Lifecycle",
                        "definition": {
                            "filters": {
                                "blobTypes": ["blockBlob"],
                                "prefixMatch": ["billing-hot/"]
                            },
                            "actions": {
                                "baseBlob": {
                                    "tierToCool": {
                                        "daysAfterModificationGreaterThan": 90
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        
        # Write policy to temporary file
        policy_file = 'temp_policy.json'
        with open(policy_file, 'w') as f:
            json.dump(policy, f, indent=2)
        
        try:
            cmd = [
                'az', 'storage', 'account', 'management-policy', 'create',
                '--account-name', self.storage_account,
                '--resource-group', self.resource_group,
                '--policy', f'@{policy_file}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Lifecycle policy applied successfully")
            else:
                print(f"⚠️  Warning: Failed to apply lifecycle policy: {result.stderr}")
        finally:
            # Clean up temporary file
            if os.path.exists(policy_file):
                os.remove(policy_file)
        
        return True
    
    def get_connection_strings(self):
        """Retrieve connection strings for created resources"""
        print("🔗 Retrieving connection strings...")
        
        connection_info = {}
        
        # Get Cosmos DB connection details
        try:
            cmd = [
                'az', 'cosmosdb', 'show',
                '--resource-group', self.resource_group,
                '--name', self.cosmos_account,
                '--query', 'documentEndpoint',
                '--output', 'tsv'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                connection_info['cosmos_endpoint'] = result.stdout.strip()
            
            cmd = [
                'az', 'cosmosdb', 'keys', 'list',
                '--resource-group', self.resource_group,
                '--name', self.cosmos_account,
                '--query', 'primaryMasterKey',
                '--output', 'tsv'
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                connection_info['cosmos_key'] = result.stdout.strip()
        
        except Exception as e:
            print(f"⚠️  Warning: Could not retrieve Cosmos DB connection info: {e}")
        
        # Get Storage Account
