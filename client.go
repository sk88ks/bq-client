package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	googleTokenURL  = "https://accounts.google.com/o/oauth2/token"
	defaultPageSize = 5000
)

// Client is a client for google bigquery
type Client struct {
	jwtConfig  *jwt.Config
	datasetRef *bigquery.DatasetReference
}

type ClientData struct {
	Headers []string
	Rows    [][]interface{}
	Err     error
}

type ResponseData struct {
}

// GetPrivateKeyByPEM gets a byte slice as key from a given PEM file
func GetPrivateKeyByPEM(pemPath string) ([]byte, error) {
	return ioutil.ReadFile(pemPath)
}

// New generates a new client for bigquery with google oauth2 by jwt
func New(email string, privteKey []byte, subject string) *Client {
	return &Client{
		jwtConfig: &jwt.Config{
			Email:      email,
			PrivateKey: privteKey,
			Subject:    subject,
			Scopes:     []string{bigquery.BigqueryScope},
			TokenURL:   googleTokenURL,
		},
	}
}

func (c *Client) getService() (*bigquery.Service, error) {
	if c.jwtConfig == nil {
		return nil, errors.New("Not initialized")
	}

	client := c.jwtConfig.Client(oauth2.NoContext)
	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}
	return service, nil
}

// Dataset sets a target dataset reference
func (c *Client) Dataset(projectID string, datasetID string) *Client {
	c.datasetRef = &bigquery.DatasetReference{
		DatasetId: datasetID,
		ProjectId: projectID,
	}
	return c
}

func (c *Client) Query() {

}

func (c *Client) queryPaging(queryString string, size int64) (chan ResponseData, error) {
	service, err := c.getService()
	if err != nil {
		return nil, err
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: c.datasetRef,
		MaxResults:     size,
		Kind:           "json",
		Query:          queryString,
	}

	queryResponse, err := service.Jobs.Query(c.datasetRef.ProjectId, query).Do()
	spew.Dump(queryResponse)

	response := make(chan ResponseData, 1)
	return response, nil
}

// Query load the data for the query paging if necessary and return the data rows, headers and error
//func (c *Client) Query(dataset, project, queryStr string) ([][]interface{}, []string, error) {
//	return c.pagedQuery(DefaultPageSize, dataset, project, queryStr, nil)
//}

// pagedQuery executes the query using bq's paging mechanism to load all results and sends them back via dataChan if available, otherwise it returns the full result set, headers and error as return values
func (c *Client) pagedQuery(pageSize int, dataset, project, queryStr string, dataChan chan ClientData) ([][]interface{}, []string, error) {
	// connect to service
	service, err := c.getService()
	if err != nil {
		if dataChan != nil {
			dataChan <- ClientData{Err: err}
		}
		return nil, nil, err
	}

	datasetRef := &bigquery.DatasetReference{
		DatasetId: dataset,
		ProjectId: project,
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		MaxResults:     int64(pageSize),
		Kind:           "json",
		Query:          queryStr,
	}

	// start query
	qr, err := service.Jobs.Query(project, query).Do()

	if err != nil {
		fmt.Println("Error loading query: ", err)
		if dataChan != nil {
			dataChan <- ClientData{Err: err}
		}

		return nil, nil, err
	}

	var headers []string
	rows := [][]interface{}{}

	// if query is completed process, otherwise begin checking for results
	if qr.JobComplete {
		headers = c.headersForResults(qr)
		rows = c.formatResults(qr, len(qr.Rows))
		if dataChan != nil {
			dataChan <- ClientData{Headers: headers, Rows: rows}
		}
	}

	if qr.TotalRows > uint64(pageSize) || !qr.JobComplete {
		resultChan := make(chan processedData)

		go c.pageOverJob(len(rows), qr.JobReference, qr.PageToken, resultChan)

	L:
		for {
			select {
			case data, ok := <-resultChan:
				if !ok {
					break L
				}
				if dataChan != nil {
					if len(data.headers) > 0 {
						headers = data.headers
					}
					dataChan <- ClientData{Headers: headers, Rows: data.rows}
				} else {
					headers = data.headers
					rows = append(rows, data.rows...)
				}
			}
		}
	}

	if dataChan != nil {
		close(dataChan)
	}

	return rows, headers, nil
}

type processedData struct {
	rows    [][]interface{}
	headers []string
}

// pageOverJob loads results for the given job reference and if the total results has not been hit continues to load recursively
func (c *Client) pageOverJob(rowCount int, jobRef *bigquery.JobReference, pageToken string, resultChan chan processedData) error {
	service, err := c.getService()
	if err != nil {
		return err
	}

	qrc := service.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId)
	if len(pageToken) > 0 {
		qrc.PageToken(pageToken)
	}

	qr, err := qrc.Do()
	if err != nil {
		fmt.Println("Error loading additional data: ", err)
		close(resultChan)
		return err
	}

	if qr.JobComplete {
		// send back the rows we got
		headers := c.headersForJobResults(qr)
		rows := c.formatResultsFromJob(qr, len(qr.Rows))
		resultChan <- processedData{rows, headers}
		rowCount = rowCount + len(rows)
	}

	if qr.TotalRows > uint64(rowCount) || !qr.JobComplete {
		if qr.JobReference == nil {
			c.pageOverJob(rowCount, jobRef, pageToken, resultChan)
		} else {
			c.pageOverJob(rowCount, qr.JobReference, qr.PageToken, resultChan)
		}
	} else {
		close(resultChan)
		return nil
	}

	return nil
}

// SyncQuery executes an arbitrary query string and returns the result synchronously (unless the response takes longer than the provided timeout)
func (c *Client) SyncQuery(dataset, project, queryStr string, maxResults int64) ([][]interface{}, error) {
	service, err := c.getService()
	if err != nil {
		return nil, err
	}

	datasetRef := &bigquery.DatasetReference{
		DatasetId: dataset,
		ProjectId: project,
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		MaxResults:     maxResults,
		Kind:           "json",
		Query:          queryStr,
	}

	results, err := service.Jobs.Query(project, query).Do()
	if err != nil {
		fmt.Println("Query Error: ", err)
		return nil, err
	}

	// credit to https://github.com/getlantern/statshub for the row building approach
	numRows := int(results.TotalRows)
	if numRows > int(maxResults) {
		numRows = int(maxResults)
	}

	rows := c.formatResults(results, numRows)
	return rows, nil
}

// headersForResults extracts the header slice from a QueryResponse
func (c *Client) headersForResults(results *bigquery.QueryResponse) []string {
	headers := []string{}
	numColumns := len(results.Schema.Fields)
	for c := 0; c < numColumns; c++ {
		headers = append(headers, results.Schema.Fields[c].Name)
	}
	return headers
}

// formatResults extracts the result rows from a QueryResponse
func (c *Client) formatResults(results *bigquery.QueryResponse, numRows int) [][]interface{} {
	rows := make([][]interface{}, numRows)
	for r := 0; r < int(numRows); r++ {
		numColumns := len(results.Schema.Fields)
		dataRow := results.Rows[r]
		row := make([]interface{}, numColumns)
		for c := 0; c < numColumns; c++ {
			row[c] = dataRow.F[c].V
		}
		rows[r] = row
	}
	return rows
}

// formatResultsFromJob extracts the result rows from a GetQueryResultsResponse
func (c *Client) formatResultsFromJob(results *bigquery.GetQueryResultsResponse, numRows int) [][]interface{} {
	rows := make([][]interface{}, numRows)
	for r := 0; r < int(numRows); r++ {
		numColumns := len(results.Schema.Fields)
		dataRow := results.Rows[r]
		row := make([]interface{}, numColumns)
		for c := 0; c < numColumns; c++ {
			row[c] = dataRow.F[c].V
		}
		rows[r] = row
	}
	return rows
}

// headersForJobResults extracts the header slice from a GetQueryResultsResponse
func (c *Client) headersForJobResults(results *bigquery.GetQueryResultsResponse) []string {
	headers := []string{}
	numColumns := len(results.Schema.Fields)
	for c := 0; c < numColumns; c++ {
		headers = append(headers, results.Schema.Fields[c].Name)
	}
	return headers
}

// Count loads the row count for the provided dataset.tablename
func (c *Client) Count(dataset, project, datasetTable string) int64 {
	qstr := fmt.Sprintf("select count(*) from [%s]", datasetTable)
	res, err := c.SyncQuery(dataset, project, qstr, 1)
	if err == nil {
		if len(res) > 0 {
			val, _ := strconv.ParseInt(res[0][0].(string), 10, 64)
			return val
		}
	}
	return 0
}
