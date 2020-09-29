# Read micro-batching

## About

Contains two Go files:

   * `batchread.go` - Implementation of micro-batching, using Postgres DB
   * `batchread_test.go` - Test code for `batchread.go`

## Configuring

Configure an environment variable for Postgres URL

```shell
DATABASE_URL=postgres://user:password@server:port/database_name?search_path=schema_name
```

`batchread_test.go` contains constants that configure other aspects.  look them up.

### Running

Using the Go run test under the source code directory

```shell
$ go test -v
```
