# Golden Axe Blockchain Syncing Guide

## How Golden Axe Avoids Rate Limiting

Golden Axe **does NOT get rate limited** because:

1. **You control your own RPC endpoints** - The application syncs data from RPC URLs that YOU configure
2. **Self-hosted infrastructure** - When you run Golden Axe with Docker, you're running your own indexer
3. **Configurable concurrency & batch sizes** - You can tune the sync parameters to match your RPC provider's limits
4. **Built-in retry logic** - Automatically backs off when encountering errors

This is different from using a hosted service where you share rate limits with other users.

## Adding Custom RPC URLs

RPC URLs are stored in the `config` table in the **frontend database** (`fe`). Here's how to add/modify them:

### 1. Connect to the Database

```bash
# Using Docker Compose:
docker compose exec postgres psql -U golden_axe -d fe

# Or from your host (if port 5432 is mapped):
psql -U golden_axe -h localhost -d fe
```

### 2. View Current Configuration

```sql
SELECT
    enabled,
    chain,
    name,
    url,
    start_block,
    batch_size,
    concurrency
FROM config
ORDER BY chain;
```

### 3. Add a New Chain

```sql
INSERT INTO config (enabled, chain, name, url, start_block, batch_size, concurrency)
VALUES (
    true,                          -- enabled: set to true to start syncing
    1,                             -- chain: chain ID (e.g., 1 for Ethereum mainnet)
    'Ethereum Mainnet',            -- name: human-readable name
    'https://eth.llamarpc.com',    -- url: your RPC endpoint
    19000000,                      -- start_block: block to start syncing from (optional)
    100,                           -- batch_size: blocks per batch (default: 2000)
    5                              -- concurrency: parallel requests (default: 10)
);
```

### 4. Update Existing Configuration

```sql
-- Enable/disable syncing for a chain
UPDATE config SET enabled = true WHERE chain = 1;

-- Update RPC URL
UPDATE config SET url = 'https://your-custom-rpc.com' WHERE chain = 1;

-- Adjust performance parameters
UPDATE config
SET batch_size = 500, concurrency = 20
WHERE chain = 8453;  -- Base
```

### 5. Common RPC Providers

```sql
-- Using your own node
UPDATE config SET url = 'http://localhost:8545' WHERE chain = 1;

-- Using Alchemy
UPDATE config SET url = 'https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY' WHERE chain = 1;

-- Using Infura
UPDATE config SET url = 'https://mainnet.infura.io/v3/YOUR_PROJECT_ID' WHERE chain = 1;

-- Using Ankr (public, may have rate limits)
UPDATE config SET url = 'https://rpc.ankr.com/eth' WHERE chain = 1;

-- Using LlamaNodes
UPDATE config SET url = 'https://eth.llamarpc.com' WHERE chain = 1;
```

## Configuration Parameters Explained

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `enabled` | Whether to sync this chain | false | Set to `true` to start syncing |
| `chain` | Chain ID | - | Must match the actual chain ID |
| `name` | Display name | - | For reference only |
| `url` | RPC endpoint URL | - | Must be a valid JSON-RPC endpoint |
| `start_block` | Starting block number | null | If null, starts from latest block |
| `batch_size` | Blocks per request | 2000 | Lower = less load, slower sync |
| `concurrency` | Parallel requests | 10 | Higher = faster, more RPC load |

### Tuning for Your RPC Provider

**If you have rate limits:**
```sql
UPDATE config
SET batch_size = 100, concurrency = 2
WHERE chain = 1;
```

**If you have a dedicated/unlimited RPC:**
```sql
UPDATE config
SET batch_size = 5000, concurrency = 50
WHERE chain = 1;
```

**For public RPC endpoints (conservative):**
```sql
UPDATE config
SET batch_size = 50, concurrency = 1
WHERE chain = 1;
```

## Monitoring Sync Progress

### 1. Real-time Status Endpoint (Server-Sent Events)

The backend exposes a `/status` endpoint that streams real-time updates:

```bash
# Monitor sync progress in real-time
curl -N http://localhost:8000/status
```

**Example output:**
```json
{"new_block":"local","chain":7777777,"num":25384729}
{"active_connections":2}
{"database_size_pretty":"1234 MB","database_size":1293942784}
{"new_block":"local","chain":7777777,"num":25384730}
{"active_connections":2}
```

### 2. Check Logs for Sync Speed

```bash
# View backend logs
docker compose logs -f be

# Filter for sync-related logs
docker compose logs be | grep -E "chain|block|sync"
```

**Log indicators:**
- `initializing blocks table at: <block_number>` - Starting point
- Frequent "new_block" messages = active syncing
- Errors = sync issues (will auto-retry with backoff)

### 3. Query Database Directly

```bash
# Connect to database
docker compose exec postgres psql -U golden_axe -d be
```

**Check current block height per chain:**
```sql
SELECT
    chain,
    MAX(num) as latest_block,
    COUNT(*) as total_blocks,
    MAX(timestamp) as latest_timestamp
FROM blocks
GROUP BY chain
ORDER BY chain;
```

**Calculate sync speed (blocks per minute):**
```sql
SELECT
    chain,
    COUNT(*) as blocks_last_minute,
    COUNT(*) / 1.0 as blocks_per_minute,
    COUNT(*) * 60.0 as blocks_per_hour
FROM blocks
WHERE timestamp > NOW() - INTERVAL '1 minute'
GROUP BY chain;
```

**Check recent sync activity:**
```sql
SELECT
    chain,
    num as block_number,
    timestamp,
    AGE(NOW(), timestamp) as age
FROM blocks
ORDER BY timestamp DESC
LIMIT 20;
```

**Estimate time to catch up:**
```sql
-- Compare your latest block vs current chain height
-- (You'll need to know the current block height from the RPC)
SELECT
    chain,
    MAX(num) as your_latest,
    -- Assuming current block is ~20,000,000
    20000000 - MAX(num) as blocks_behind,
    -- If syncing at 100 blocks/min
    (20000000 - MAX(num)) / 100.0 / 60.0 as hours_to_catch_up
FROM blocks
WHERE chain = 1
GROUP BY chain;
```

### 4. Monitor Database Size

```bash
# Check database size
docker compose exec postgres psql -U golden_axe -d be -c "
SELECT pg_size_pretty(pg_database_size('be')) as db_size;
"
```

### 5. Create a Monitoring Dashboard Script

Save this as `monitor-sync.sh`:

```bash
#!/bin/bash
# Monitor Golden Axe sync progress

while true; do
    clear
    echo "=== Golden Axe Sync Monitor ==="
    echo ""

    # Check current blocks
    docker compose exec -T postgres psql -U golden_axe -d be -c "
    SELECT
        chain,
        MAX(num) as latest_block,
        COUNT(*) as total_blocks,
        MAX(timestamp) as latest_timestamp
    FROM blocks
    GROUP BY chain
    ORDER BY chain;
    " 2>/dev/null

    echo ""
    echo "=== Sync Speed (last minute) ==="
    docker compose exec -T postgres psql -U golden_axe -d be -c "
    SELECT
        chain,
        COUNT(*) as blocks_per_minute
    FROM blocks
    WHERE timestamp > NOW() - INTERVAL '1 minute'
    GROUP BY chain;
    " 2>/dev/null

    echo ""
    echo "=== Database Size ==="
    docker compose exec -T postgres psql -U golden_axe -d be -c "
    SELECT pg_size_pretty(pg_database_size('be')) as size;
    " 2>/dev/null

    sleep 10
done
```

```bash
chmod +x monitor-sync.sh
./monitor-sync.sh
```

## Disabling Sync (For Testing)

To run the backend without syncing (useful for testing):

```bash
docker compose run --rm -e NO_SYNC=true be
```

Or in docker-compose.yml:
```yaml
environment:
  NO_SYNC: "true"
```

## Troubleshooting Sync Issues

### Sync is slow
1. Increase `batch_size` and `concurrency` in config table
2. Check RPC provider rate limits
3. Verify network connectivity to RPC endpoint
4. Monitor CPU/memory usage of container

### Sync stopped
1. Check logs: `docker compose logs be`
2. Verify RPC endpoint is responsive: `curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' YOUR_RPC_URL`
3. Check database space: `df -h`
4. Restart sync: `docker compose restart be`

### Rate limiting errors
1. Reduce `batch_size` and `concurrency`
2. Switch to a different RPC provider
3. Use your own node
4. Enable `start_block` to skip old blocks

## Best Practices

1. **Start with recent blocks**: Set `start_block` to a recent block number to avoid syncing years of history
2. **Use dedicated RPC endpoints**: Public endpoints have rate limits
3. **Monitor database growth**: Blockchain data grows quickly
4. **Tune parameters gradually**: Start conservative, increase as needed
5. **Multiple chains**: Be careful syncing many chains simultaneously - may overwhelm your system

## Example: Setting Up Base Chain

```sql
-- Connect to fe database
-- docker compose exec postgres psql -U golden_axe -d fe

-- Enable Base with custom RPC
UPDATE config
SET
    enabled = true,
    url = 'https://mainnet.base.org',
    start_block = 10000000,  -- Start from recent block
    batch_size = 1000,
    concurrency = 10
WHERE chain = 8453;
```

Then restart the backend:
```bash
docker compose restart be
```

Monitor progress:
```bash
curl -N http://localhost:8000/status | grep '"chain":8453'
```
