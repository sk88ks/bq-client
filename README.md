bq-client
=========

Bq-client is a client for Google bigquery with gokang.
This works on progress.

Current API Documents:

* go-worker: [![GoDoc](https://godoc.org/github.com/sk88ks/bq-client?status.svg)](https://godoc.org/github.com/sk88ks/bq-client)

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
 ketPath = "pathToKeyPemfile"
 accountEmail = "your account email"
 gcpProject = "your gcp projact ID"
 bqDataset = "your data set name"
)

func main() {
	key, err := bqc.GetPrivateKeyByPEM(keyPath)
	if err != nil {
		return err
	}
	bqClient = bqc.New(accountEmail, key, "")
	bqClient.Dataset(gcpProject, bqDataset)
	
	var res []Response
	err = bqClient.Query(query, &res)
  fmt.Println(res)
	
}
```
