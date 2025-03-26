Index Supply is a hosted HTTP API for running SQL queries on Ethereum Logs.

You can use this API from your backend or from your user's browser.

Here is an example query

```
curl -G https://api.indexsupply.net/query \
    --data-urlencode 'chain=8453' \
    --data-urlencode 'query=select "from", "to", tokens from transfer limit 1' \
    --data-urlencode 'event_signatures=Transfer(address indexed from, address indexed to, uint tokens)' \
    | jq .
```
And the response
```
{
  "block_height": 18479546,
  "result": [
    [
      [
        "from",
        "to",
        "tokens"
      ],
      [
        "0x0000000000000000000000000000000000000000",
        "0xdaabdaac8073a7dabdc96f6909e8476ab4001b34",
        "0"
      ]
    ]
  ]
}
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

## Response {#query-response .reference}

Regardless of the kind of query (ie get, post, single, batch) there is a single response object. The response is JSON and includes the block height at which the query(s) were executed and a 3-dimensional array.

```
{
  block_height: {},
  result: [[[]]]
}
```

The first, outer array dimension relates to the number of queries submitted (one for single and many for batch). In the case of `GET /query` and `GET /query-live` this outer array will always have `length=1` (meaning the result is: `result[0]`).

The second array dimension represents the number of rows returned from the query. This array can be empty (`length=0`) in the case that the query returned no rows.

If a query did return a set of rows, then the second array will always contain at least 2 items. The first is an array of the column names and the rest are arrays of column values.

The third array dimension represents column values. In the case of the column names, this will be an array of strings. In the case of column values, it will be an array with the following types:

| ABI Type | JSON Type           |
|----------|---------------------|
| bool     | bool                |
| bytesN   | hexadecimal string  |
| string   | string              |
| intN     | decimal string      |
| uintN    | decimal string      |

Arrays of these types will be a JSON array of the type.

All inner (3rd dimension) arrays will have the same length.

### GET /query {#get-query .reference }

Executes the supplied query against the latest block and returns a JSON encoded [response body](#query-response)

**URL Request Fields**

| Field            | Type     | Description         |
|-------------     |--------- |---------------------|
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |
| chain            | int      | Chain id. See [chains](#chains) |
| event_signatures | []string | [human readable event signatures][3] |
| query | string  | SQL referencing tables/columns from `event_signatures`|

**Example**

```
curl -G https://api.indexsupply.net/query \
    --data-urlencode 'chain=8453' \
    --data-urlencode 'query=select a from foo' \
    --data-urlencode 'event_signatures=Foo(uint a)'
```

Multiple event signatures enabled by adding more `event_signatures` query parameters.

**Example**

```
curl -G https://api.indexsupply.net/query \
    --data-urlencode 'chain=8453' \
    --data-urlencode 'query=select a, b from foo, bar where foo.c = bar.c' \
    --data-urlencode 'event_signatures=Foo(uint a, uint c)' \
    --data-urlencode 'event_signatures=Bar(uint b, uint c)'
```

### GET /query-live {#get-query-live .reference }

Executes the supplied query against the latest block and returns an HTTP SSE stream of JSON encoded [response bodies](#query-response). The HTTP SSE stream will include the results of the query for the entire range of blocks in the chain (unless a block predicate was added to the SQL query) and so long as the connection is open it will stream new results subsequently indexed blocks.

**URL Request Fields**

| Field            | Type     | Description         |
|-------------     |--------- |---------------------|
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |
| chain            | int      | Chain id. See [chains](#chains) |
| event_signatures | []string | [human readable event signatures][3] |
| query | string  | SQL referencing tables/columns from `event_signatures`|

The response is a standard [response](#query-response) object but delivered via HTTP SSE. The SSE protocol will keep the connection open indefinitely and each new block will trigger a new event. Events are plain text, prefixed with `data: ` and separated by a `\n\n`.

```
curl -G https://api.indexsupply.net/query-live \
    --data-urlencode 'chain=8453' \
    --data-urlencode 'query=select a from transfer limit 1' \
    --data-urlencode 'event_signatures=Transfer(address indexed a, address indexed b, uint c)'
```

### POST /query {#post-query .reference }

This endpoind accepts a JSON array of objects with the following fields. The array may contain more than one request object if callers would like for the queries to run inside of a database transaction. This enables a consistent reads.

Similar to `GET` requests which accept the `api-key` and `chain` in the URL, when making a `POST` request the `api-key` and `chain` must also be included in the URL.

**JSON Body**

An array of objects with the following fields:

| Field            | Type     | Description         |
|-------------     |--------- |---------------------|
| event_signatures | []string | [human readable event signatures][3] |
| query | string  | SQL referencing tables/columns from `event_signatures`|

**URL Request Fields**

| Field            | Type     | Description         |
|-------------     |--------- |---------------------|
| api-key | string | API key from [your account page](https://www.indexsupply.net/account) |
| chain            | int      | Chain id. See [chains](#chains) |

**Example**

```
curl -X POST https://api.indexsupply.net/query?chain=8453 \
    -H "Content-Type: application/json" \
    -d '[
        {
            "event_signatures": ["Foo(uint a)"],
            "query": "select a from foo"
        },
        {
            "event_signatures": ["Bar(uint b)"],
            "query": "select b from bar"
        }
    ]'
```

<hr>

## SQL {#sql .reference}

When you provide an event signature `Foo(uint indexed bar, uint baz)` you effectively have a table named `foo` with a numeric columns named `bar` and `baz` that you can query:

```
select baz from foo where bar = 1
```

### EVM Columns {#evm-columns}

In addition to event data, there are other EVM columns available:

| Column    | Type    | Description                        |
|-----------|---------|---------------------               |
| address   | bytea   | contract address emitting the event|
| block_num | numeric |                                    |
| log_idx   | numeric |                                    |
| tx_hash   | bytea   |                                    |

These can be used with the event data. For example:

```
select block_num, log_idx, baz
from foo
where address = 0x0000000000000000000000000000000000000000
and bar = 1
```

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

where select_list is one of: *  | [[expression [AS output_name]], …]

  * Project all column references for all from_items

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

You will get an empty 200 response if it worked. You can check the status of indexing by either visiting: [api.indexsupply.net/status](https://api.indexsupply.net/status) or you can use the SQL API to query the latest block.

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

| Field       | Type    | Description                        |
|-------------|---------|---------------------               |
| org         | string  | A value to group multiple api keys |
| origins     | []string | Optional. A list of allowed origins for the key. This prevents people from stealing the key for browser use. |

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
