bq-client
=========
 
[![Build Status](https://travis-ci.org/sk88ks/bq-client.svg?branch=master)](https://travis-ci.org/sk88ks/bq-client)
[![Coverage Status](https://coveralls.io/repos/sk88ks/bq-client/badge.svg?branch=master)](https://coveralls.io/r/sk88ks/bq-client?branch=master)

Bq-client is a client for Google bigquery with golang.
This works on progress.

Current API Documents:

* bq-client: [![GoDoc](https://godoc.org/github.com/sk88ks/bq-client?status.svg)](https://godoc.org/github.com/sk88ks/bq-client)

Installation
----

```
go get github.com/sk88ks/bq-client
```

Quick start
----

To create a new client,

```go
import(
  "fmt"
  bqc "github.com/sk88ks/bq-client"
)

type Response struct {
  ....
}

const (
 keyPath = "pathToKeyPemfile"
 accountEmail = "your account email"
 gcpProject = "your gcp projact ID"
 bqDataset = "your data set name"
)

func main() {
	key, err := bqc.GetPrivateKeyByPEM(keyPath)
	if err != nil {
		return err
	}
	bqClient := bqc.New(accountEmail, key, "")
	bqClient.Dataset(gcpProject, bqDataset)
	
	var res []Response
	queryString := "SELECT * FROM test.test_table WHERE num > 0"
	err = bqClient.Query(query).Execute(&res)
	fmt.Println(res)
	
}
```
