Index Supply is a hosted HTTP API for running SQL queries on Ethereum Blocks, Transactions, and Logs.

You can use this API from your backend or from your user's browser.

Here is an example query

```
curl -G https://api.indexsupply.net/v2/query?api-key=secret \
    --data-urlencode 'sql=select "from", "to", tokens from transfer where chain = 8453 limit 1' \
    --data-urlencode 'signatures=Transfer(address indexed from, address indexed to, uint tokens)' \ | jq .
```

And the response

```
[{
  "cursor": "8453-18479546",
  "columns": [
    {"name": "from",   "pgtype": "bytea"},
    {"name": "to",     "pgtype": "bytea"},
    {"name": "tokens", "pgtype": "numeric"}
  ],
  "rows":[[
        "0x0000000000000000000000000000000000000000",
        "0xdaabdaac8073a7dabdc96f6909e8476ab4001b34",
        "0"
  ]]
}]
```

## TypeScript {#typescript}

There is a TS package [hosted on NPM](https://www.npmjs.com/package/@indexsupply/indexsupply.js) that will enable both node (and possibly other runtimes) and browser access to the Index Supply API.

See the repository for docs and examples: [https://github.com/indexsupply/indexsupply.js](https://github.com/indexsupply/indexsupply.js)

## Chains {#chains}

| Name                         | Id     |
|------------------------------|--------|
{{#each chains }}
|{{name}} | {{chain}} |
{{/each}}

It is possible to query data across multiple chains. This is accomplished by adding a `where chain = $1` or `where chain in ($1)` predicate to queries.

For example

```
select a from foo where chain in (1, 10, 8453)
```

To query multiple chains starting at multiple block heights, use the following pattern

```
select a
from foo
where (
  (chain = 8453 and block_num > 42)
  or (chain = 10 and block_num > 100)
)
```

Email [support@indexsupply.com](mailto:support@indexsupply.com) to request new chains.

## Queries {#queries}

The API offers 3 basic types of queries: Single, Batch, and Live.

### Single {#queries-single}

A single query is executed against the latest state of Index Supply's indexer.

### Batch {#queries-batch}

Batch queries are useful when you have several queries that you would like to run within the same database transaction. This allows you to get a consistent view of the chain across multiple queries. The response includes the block height at which the queries were run. You can use this value to start your live queries.

### Live {#queries-live}

A live query takes a query and a block height and will send HTTP SSE events as new events matching the query are indexed.

It is common to do a single (or batch) query on page load and then subscribe to updates once the page state has been initialized.

### Reorgs {#reorgs}

In the case of a chain reorg, clients will receive a block height that is lower than previously received. Clients should keep a single value pointer to the latest block height and when a new block is lower, the client should discard the entire state and start over. This should be fast, bug free, and consistent with crash-only software design principles!

<hr>

## Request {#query-request .reference}

A request consists of the following fields (either form encoded or JSON encoded)

| Field | Type | Description |
| - | - | - |
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |
| cursor | string | Optional. See [cursor](#cursor) |
| signatures | []string | Optional. [human readable abi signatures][3] |
| query | string | SQL referencing tables/columns from `signatures`|


### Cursor {#cursor}

The cusror enables synchronization between your app and the Index Supply API. When you make a request without a cursor, the query will be executed on all indexed blocks available to Index Supply. The [Response](#response) will contain a `cursor` string mapping the chains referenced in the query and the latest `block_height` of the referenced chain at the time of query execution.

For example

```
select "from", "to", value from transfer where chain = 8453
```

The response will contain a cusror indicating that the chain `8453` was at block `42` at the time of query execution.

```
[
  {
    "cursor": "8453-42",
    "columns": [...],
    "rows": [[...], ...]
  }
]
```

The cursor is a string encode of: `chain-num-chain-num-...`. The string encoding is used to make it easy for GET requests -- since they are required for SSE in the browser.

Subsequent requests including the cursor, will return data where `block_num > 42`.

### GET `/v2/query` {#get-query}

Executes the supplied query against the latest block (or the block height specified by the `cursor`) and returns a JSON encoded [Response](#query-response).

**URL Request Fields**

| Field | Type | Description |
| - | - | - |
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |
| cursor | string | Optional. See [cursor](#cursor) |
| signatures | []string | Optional. [Human readable abi signatures][3] |
| sql | string | SQL referencing tables/columns from `signatures`|

**Example**

```
curl -G https://api.indexsupply.net/v2/query?api-key=secret \
    --data-urlencode 'sql=select a from foo where chain = 8453' \
    --data-urlencode 'signatures=Foo(uint a)'
```

**Multiple Signatures Example**

```
curl -G https://api.indexsupply.net/v2/query?api-key=secret \
    --data-urlencode 'sql=select a, b from foo, bar where foo.c = bar.c' \
    --data-urlencode 'signatures=Foo(uint a, uint c)' \
    --data-urlencode 'signatures=Bar(uint b, uint c)'
```

### GET `/v2/query-live` {#get-query-live}

Executes the supplied query against the latest block and returns an HTTP SSE stream of JSON encoded [response bodies](#query-response). The HTTP SSE stream will include the results of the query for the entire range of blocks in the chain (unless a block predicate was added to the SQL query) and so long as the connection is open it will stream new results for newly indexed blocks.

**URL Request Fields**

| Field | Type | Description |
| - | - | - |
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |
| cursor | string | Optional. See [cursor](#cursor) |
| signatures | []string | [human readable event signatures][3] |
| query | string  | SQL referencing tables/columns from `signatures`|

The response is a standard [response](#query-response) object but delivered via HTTP SSE. The SSE protocol will keep the connection open indefinitely and each new block will trigger a new event. Events are plain text, prefixed with `data: ` and separated by a `\n\n`.

```
curl -G https://api.indexsupply.net/v2/query-live?api-key=secret \
    --data-urlencode '8453-0' \
    --data-urlencode 'query=select a from transfer limit 1' \
    --data-urlencode 'signatures=Transfer(address indexed a, address indexed b, uint c)'
```

### POST `/v2/query` {#post-query}

This endpoind accepts a JSON array of objects with the following fields. The array may contain more than one request object if callers would like for the queries to run inside of a database transaction. This enables a consistent reads.

Similar to `GET` requests which accept the `api-key` and `chain` in the URL, when making a `POST` request the `api-key` and `chain` must also be included in the URL.

**JSON Body**

An array of objects with the following fields:

| Field | Type | Description |
| - | - | - |
| cursor | string | Optional. See [cursor](#cursor) |
| signatures | []string | [human readable event signatures][3] |
| query | string  | SQL referencing tables/columns from `signatures`|

**URL Request Fields**

| Field | Type | Description |
| - | - | - |
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |

**Example**

```
curl -X POST https://api.indexsupply.net/v2/query?api-key=secret \
    -H "Content-Type: application/json" \
    -d '[
        {
            "cursor": "8453-0",
            "signatures": ["Foo(uint a)"],
            "query": "select a from foo"
        },
        {
            "cursor": "8453-0",
            "signatures": ["Bar(uint b)"],
            "query": "select b from bar"
        }
    ]'
```

<hr>

## Response {#query-response .reference}

Regardless of the [Request](#request) there is a single response. The response is always a JSON array containing individual response objects. When using the POST endpoint with multiple requests the array represents responses to each request. For single requests using the GET endpoint the outer array will simply contain a single element.

```
[
  {
    "cursor": "chainid-blocknum",
    "columns": [{name: string, type: string}],
    "rows": [
      [col1, col2, colN],
      [col1, col2, colN],
    ]
  }
]
```

The `cursor` string can be copied verbaitum into a subsequent request to query for new data. See [Cursor](#cursor) for more detail.

The `columns` field contains an array of objects mapping the name of the column (derived from the query) to its postgres type (ie `bytea`, `numeric`, `json`, etc.).

The order of the `columns` array matches the order of the column data in `rows`.

| ABI Type | JSON Type           |
|----------|---------------------|
| bool     | bool                |
| bytesN   | hexadecimal string  |
| string   | string              |
| intN     | decimal string      |
| uintN    | decimal string      |

The `rows` field contains a 2-dimensional array where the outer array represents the number of rows in the query's result set and the inner arrays are the columns of data for each row. The length of the inner arrays are awlays be equal to the number elements in the `columns` object.

<hr>

## SQL {#sql .reference}

When you provide a signature `Foo(uint indexed bar, uint baz)` you effectively have a table named `foo` with a numeric columns named `bar` and `baz` that you can query:

```
select baz from foo where bar = 1
```

### EVM Tables and Columns {#evm-data}

When requests include a signature and a query, it is assumed that the query is operating on a virtual table of event logs or transaction inputs. (depending on the `event` or `function` prefix) However, it is also possible to query the base tables directly.

#### EVM Tables {#evm-tables}

| Table |
|--|
| blocks |
| txs |
| logs |

#### Blocks {#evm-blocks}

Table name: `blocks`

| Column | Type |
|--|--|
| chain | int8 |
| num | int8 |
| timestamp | timestamptz |
| gas_limit | numeric |
| gas_used | numeric |
| nonce | bytea |
| hash | bytea |
| receipts_root | bytea |
| state_root | bytea |
| extra_data | bytea |
| miner | bytea |

#### Transactions {#evm-txs}

Table name: `txs`

| Column | Type |
|--|--|
| chain | int8 |
| block_num | int8 |
| block_timestamp | timestamptz |
| idx | int4 |
| type | int2 |
| gas | numeric |
| gas_price | numeric |
| nonce | bytea |
| hash | bytea |
| from | bytea |
| to | bytea |
| input | bytea |
| value | numeric |

#### Logs {#evm-logs}

Table name: `logs`

| Column | Type |
|--|--|
| chain | int8 |
| block_num | int8 |
| block_timestamp | timestamptz |
| log_idx | int4 |
| tx_hash | bytea |
| address | bytea |
| topics | bytea[] |
| data | bytea |

### SQL Details {#sql-details}

Index Supply supports a subset of the Postgres SQL language. Here is a brief overview of the supported syntax:

```
SELECT select_list
FROM from_item
WHERE condition
GROUP BY grouping_column_reference [, …]
HAVING group_condition
LIMIT count
OFFSET start

where select_list is: [[expression [AS output_name]], …]

  [[expression [AS output_name]], …]

  Comma-separated list of value expressions. Value expression
  can be one of: column reference or aggregate function

  If a non-column-reference value expression is used in the select list,
  it conceptually adds a new virtual column to the returned table.
  The value expression is evaluated once for each result row, with the
  row's values substituted for any column references.

  If a column name reference is used and if more than one table
  has a column of the same name, the column name reference must
  be qualified with the table name. Eg table.column

where aggregate function can be one of:
  sum(), count(), avg(), min(), max()

where from_item can be one of:
  [[event_name [AS name]], …]

  [
      [event_name [AS name]]
      join_type from_item
      ON join_condition
  ]

  where event_name is an ascii string representing the
  name of the Ethereum Event. For example: "transfer" for
  `Transfer(address indexed from, address indexed to, uint tokens)`

  If more than one table is specified then the tables are CROSS JOIN-ed.
  A WHERE clause can be used to reduce the number of
  returned rows in the CROSS JOIN.

  where join_type is: { [INNER] | { LEFT | RIGHT | FULL } [OUTER] }

  where join_condition is a boolean expression

where group_column_reference is a column name reference

group_condition filters group rows created by GROUP BY

Expressions may include scalar sub-queries or table expression
sub-queries when combined with the EXISTS, NOT EXISTS,
IN, and NOT IN operators. Other operators include:

    ^
    *
    /
    %
    +
    -
    <
    >
    =
    <=
    >=
    <>
    IS
    IS NULL
    IS NOT NULL
    NOT
    AND
    OR
```

<hr>

## White Label API {#white-label-api .reference}

Index Supply can be white labeled. Please reach out to setup an account: [support@indexsupply.com](mailto:support@indexsupply.com)

APIs require an provisioning key that will be established out-of-band.

Authentication is *http basic authentication* with the provisioning key set in the user portion of the authorzation header. The Authorization header should be base64 encoded per RFC 7617. For example: `curl https://$secret:@www.indexsupply.net`.

All requests should be sent to: `www.indexsupply.net` Normal, user based api traffic is sent to `api.indexsupply.net`.

### POST /wl/add-chain {#add-chain .whitelabel}

**JSON Request Fields**

| Field       | Type    | Description                                |
|-------------|---------|---------------------                       |
| name        | string  | Name of the chain. Will show up in the UI |
| chain       | int     | Unique ID for the chain. Must match rpc response |
| url         | string  | JSON RPC API url for chain |
| start_block | int     | Optional. Defaults to the latest at time of deployment. Use start_block=1 to index from beginning. |

You will get an empty 200 response if it worked. You can check the status of indexing by either visiting: [www.indexsupply.net/status](https://www.indexsupply.net/status) or you can use the SQL API to query the latest block.

**Example**

```
curl https://$secret@www.indexsupply.net/wl/add-chain \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{
     "name": "Forty Two",
     "chain": 4242,
     "url":"https://foo.com",
     "start_block": 1
  }'
```


### GET /wl/chains {#get-chains .whitelabel}

You can list all of the chains that Index Supply is currently indexing. This is the same list that is in the Chain select dropdown menu on the home page.

There are no arguments or query parameters.

**JSON Response Fields**

| Field       | Type    | Description         |
|-------------|---------|---------------------|
| enabled     | bool    | If the chain is currently being indexed. Sometimes chains misbehave and we have to disable them. |
| name        | string  | Name of the chain. |
| chain       | int     | Unique ID for the chain. |
| popular     | bool    | If the chain is popular |
| start_block | int     | If null then it started somewhere in the middle. You can query for the smallest block_num in the blocks table to find out exact value |

**Example**

```
curl https://www.indexsupply.net/wl/list-chains | jq '.[0]'

{
  "name": "Main",
  "enabled": true,
  "popular": true,
  "chain": 1,
  "start_block": null
}
```

### POST /wl/enable-chain {#enable-chain .whitelabel}

You can only enable or disable a chain that you have added. The check is tied to the secret used when adding the chain.

The specific status of a chain can be found using the `GET /chain` endpoint.

**JSON Request Fields**

| Field       | Type    | Description              |
|-------------|---------|---------------------     |
| chain       | int     | Unique ID for the chain. |

**Example**

```
curl -v http://$secret@www.indexsupply.net/wl/enable-chain \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"chain": 42}'
```

### POST /wl/disable-chain {#disable-chain .whitelabel}

You can only enable or disable a chain that you have added. The check is tied to the secret used when adding the chain.

The specific status of a chain can be found using the `GET /chain` endpoint.

Disabling a chain only pauses indexing. It does not delete data. Enabling it will result in Index Supply resuming where it left off.

**JSON Request Fields**

| Field       | Type    | Description              |
|-------------|---------|---------------------     |
| chain       | int     | Unique ID for the chain. |

**Example**

```
curl -v http://$secret@www.indexsupply.net/wl/disable-chain \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"chain": 42}'
```

### POST /wl/create-api-key {#create-api-key .whitelabel}

**JSON Request Fields**

| Field       | Type    | Description |
|-------------|---------|---------------------|
| org         | string  | A value to group multiple api keys |
| name        | string  | An optional label for the api key |
| hard_limit  | boolean | Reject requests once plan limit has been reached |
| origins     | []string | An optional list of allowed origins for the key. This prevents people from stealing the key for browser use. |

**JSON Response Fields**

A JSON object is returned with the following object fields

| Field       | Type    | Description         |
|-------------|---------|---------------------|
| secret      | string  | An api key ready for Index Supply API access |

**Example**

```
curl https://$secret@www.indexsupply.net/wl/create-api-key \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{
     "org": "my-customer-42",
     "hard_limit": true,
     "name": "my-prod-server",
     "origins": ["facebeef.com"]
  }'

{"secret":"wlad6a25c102590ce83d52a41203904d72"}
```

### POST /wl/list-api-keys {#list-api-keys .whitelabel}

**JSON Request Fields**

| Field       | Type    | Description                        |
|-------------|---------|---------------------               |
| org         | string  | A value to group multiple api keys |

**JSON Response Fields**

A JSON array is returned with the following object fields

| Field       | Type    | Description         |
|-------------|---------|---------------------|
| org         | string  | A value to group multiple api keys |
| name        | string  | Optional label for the api key            |
| hard_limit  | boolean | Reject requests once plan limit has been reached |
| secret      | string  | The value provided when the key was created |
| origins     | []string| The value provided when the key was created |
| created_at  | int     | UNIX time when key was created |
| deleted_at  | null or int | UNIX time when key was deleted or null if active |

**Example**

```
curl https://$secret@www.indexsupply.net/wl/list-api-keys \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"org": "my-customer-42"}'

[
  {
    "org":"my-customer-42",
    "name": "my-prod-server",
    "hard_limit": true,
    "secret":"facebeef",
    "created_at":1742940562,
    "deleted_at":null
  }
]
```

### POST /wl/delete-api-key {#delete-api-key .whitelabel}

**JSON Request Fields**

| Field       | Type    | Description                        |
|-------------|---------|---------------------               |
| secret      | string  | The value provided when the key was created |

Returns an empty 200 response if successful.

**Example**
```
curl http://$secret@www.indexsupply.net/wl/delete-api-key \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"secret": "facebeef"}'
```

### POST /wl/update-api-key-hard-limit {#update-api-key-hard-limit .whitelabel}

Updates the hard limit feature of the api key.

**JSON Request Fields**

| Field       | Type    | Description |
|-------------|---------|---------------------|
| secret      | string  | The value provided when the key was created |
| hard_limit  | boolean | Reject requests once plan limit has been reached |

Returns an empty 200 response if successful.

**Example**
```
curl http://$secret@www.indexsupply.net/wl/update-api-key-hard-limit \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"secret": "facebeef", "hard_limit": false}'
```

### POST /wl/update-api-key-origins {#update-api-key-origins .whitelabel}

This endpoint will overwrite the previous list of origins with the provided list.

**JSON Request Fields**

| Field       | Type    | Description                        |
|-------------|---------|---------------------               |
| secret      | string  | The value provided when the key was created |
| origins     | string[]| A list of origins that are allowed to use the key |

Returns an empty 200 response if successful.

**Example**
```
curl http://$secret@www.indexsupply.net/wl/update-api-key-origins \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"secret": "facebeef", "origins": ["https://example.com"]}'
```

### POST /wl/usage {#usage .whitelabel}

Usage data is kept for the current and previous months. Callers should save the data if they would like to keep track of historical usage.

**JSON Request Fields**

| Field       | Type    | Description                        |
|-------------|---------|---------------------               |
| org         | string  | A value to group multiple api keys |
| month       | int     | The month of usage. 1-12           |
| year        | int     | The year of usage. Really only useful for January |

**JSON Response Fields**

Returns a single JSON object

| Field       | Type    | Description                        |
|-------------|---------|---------------------               |
| num_reqs    | int     | Numher of requests made during the month. |

**Example**
```
curl http://$secret@www.indexsupply.net/wl/usage \
  -X POST \
  -H "Content-Type: application/json" \
  --data '{"org": "my-customer-42", "month": 3, "year": 2025}'

{"num_reqs":0}
```

<br>
<br>
<hr>
<p>Thank you for reading. This is the end.</p>

[3]: https://docs.ethers.org/v5/api/utils/abi/formats/#abi-formats--human-readable-abi
