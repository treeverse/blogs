## Read micro-batching

### About

Contains two Go files:

   * batchread.go - Implementation of micro-batching, using Postgres DB
   * batchread_test.go - Go test file for batchread.go
   
### Configuring the run
configure an environment variable for Postgres URL in the form:

DATABASE_URL=postgres://user:password@server:port/database_name?search_path=schema_name

batchread_test contains constants that configure other aspects. look them up
 