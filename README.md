### Usage
#### Installation
```shell
go get github.com/firebolt-db/firebolt-gorm
```
#### Connecting to firebolt
This example shows how to define you connection parameters, construct a connection string and connect to Firebolt using GORM

```go
import firebolt "github.com/firebolt-db/firebolt-gorm"  

// Your credentials to use for connection
username := "my_username"
password := "my_password"
// Name of the database to connect to
database := "my_database_name"
// Name of the engine to use
engine_name := "my_engine_name"
// Name of the account to use
account_name := "my_firebolt_account"

// Construct a connection string
conn_string := fmt.Sprintf("firebolt://%s:%s@%s/%s?account_name=%s", username, password, database, engine_name, account_name)

// Connect to Firebolt
Db, err := gorm.Open(firebolt.Open(dsn), &gorm.Config{NamingStrategy: schema.NamingStrategy{SingularTable: true}})
if err != nil {
    log.Panicf("Failed to connect to a database: %w", err)
}
```


### Development

For running pre-commit hooks, first do `go install github.com/lietu/go-pre-commit@latest`
