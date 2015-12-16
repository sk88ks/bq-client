package client

import (
	"errors"
	"io/ioutil"
	"math"
	"reflect"
	"strconv"
	"strings"

	bigquery "google.golang.org/api/bigquery/v2"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
)

const (
	googleTokenURL  = "https://accounts.google.com/o/oauth2/token"
	defaultPageSize = 5000

	fieldTypeString    = "STRING"
	fieldTypeInteger   = "INTEGER"
	fieldTypeFloat     = "FLOAT"
	fieldTypeBoolean   = "BOOLEAN"
	fieldTypeRecord    = "RECORD"
	fieldTypeTimestamp = "TIMESTAMP"
)

const (
	// WriteTruncate is truncate option
	WriteTruncate WriteDisp = "WRITE_TRUNCATE"
	// WriteAppend is append option
	WriteAppend WriteDisp = "WRITE_APPEND"
	// WriteEmpty is empty option
	WriteEmpty WriteDisp = "WRITE_EMPTY"

	// CreateIfNeeded is option to create a new if table not exists
	CreateIfNeeded CreateDisp = "CREATE_IF_NEEDED"
	// CreateNever is option not to create a new table
	CreateNever CreateDisp = "CREATE_NEVER"
)

// Client is a client for google bigquery
type Client struct {
	jwtConfig  *jwt.Config
	datasetRef *bigquery.DatasetReference
	service    *bigquery.Service
}

// Query is a query with client
type Query struct {
	Client      *Client
	QueryString string
	JobConfig   *JobConfiguration
	size        int64
}

// WriteDisp expresses create disposition
type WriteDisp string

// CreateDisp expresses create disposition
type CreateDisp string

// JobConfiguration for bigquery client
type JobConfiguration struct {
	AllowLargeResults bool
	TempTableName     string
	WriteDisposition  WriteDisp
	CreateDisposition CreateDisp
}

// ResponseData is a data set for response from bigquery
type ResponseData struct {
	Fields []*bigquery.TableFieldSchema
	Rows   []*bigquery.TableRow
	Err    error
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
	c.service = service
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

// Query issues a new query instance
func (c *Client) Query(queryString string) *Query {
	return &Query{
		Client:      c,
		QueryString: queryString,
		size:        defaultPageSize,
	}
}

// SetJobConfig sets job Configuration
func (q *Query) SetJobConfig(config *JobConfiguration) *Query {
	q.JobConfig = config
	return q
}

// Execute execute a given query
func (q *Query) Execute(result interface{}) error {
	var fields []*bigquery.TableFieldSchema
	var rows []*bigquery.TableRow
	var err error
	if q.JobConfig != nil {
		fields, rows, err = q.retrieveRowsWithJobConfig(nil)
	} else {
		fields, rows, err = q.retrieveRows(nil)
	}
	if err != nil {
		return err
	}
	err = Convert(fields, rows, result)
	if err != nil {
		return err
	}
	return nil
}

// ExecuteWithChannel execute a given query with chan
// Channel has ResponseData that can be converted to optional struct array with Convert
func (q *Query) ExecuteWithChannel(resChan chan ResponseData) {
	if q.JobConfig != nil {
		go q.retrieveRowsWithJobConfig(resChan)
	} else {
		go q.retrieveRows(resChan)
	}
}

func (q *Query) retrieveRows(receiver chan ResponseData) ([]*bigquery.TableFieldSchema, []*bigquery.TableRow, error) {
	service, err := q.Client.getService()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: q.Client.datasetRef,
		MaxResults:     q.size,
		Kind:           "json",
		Query:          q.QueryString,
	}

	qr, err := service.Jobs.Query(query.DefaultDataset.ProjectId, query).Do()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	if qr.JobComplete && qr.TotalRows <= uint64(q.size) {
		if receiver != nil {
			receiver <- ResponseData{
				Fields: qr.Schema.Fields,
				Rows:   qr.Rows,
			}
			close(receiver)
		}

		return qr.Schema.Fields, qr.Rows, nil
	}

	var rowCount int
	rows := make([]*bigquery.TableRow, 0, int(qr.TotalRows))
	if qr.JobComplete {
		if receiver != nil {
			receiver <- ResponseData{
				Rows:   qr.Rows,
				Fields: qr.Schema.Fields,
			}
		} else {
			rows = append(rows, qr.Rows...)
		}
		rowCount = len(qr.Rows)
	}

	jobRef := qr.JobReference
	pageToken := qr.PageToken
	for {
		qrc := service.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId)
		if len(pageToken) != 0 {
			qrc.PageToken(pageToken)
		}
		qrr, err := qrc.Do()
		if err != nil {
			if receiver != nil {
				receiver <- ResponseData{
					Err: err,
				}
			}
			return nil, nil, err
		}

		res := ResponseData{}

		if qrr.Schema != nil {
			res.Fields = qrr.Schema.Fields
		}

		if qrr.JobComplete {
			rowCount += len(qrr.Rows)
			if receiver != nil {
				res.Rows = qrr.Rows
				receiver <- res
			} else {
				rows = append(rows, qrr.Rows...)
			}
		}

		if qrr.JobComplete && uint64(rowCount) >= qrr.TotalRows {
			if receiver != nil {
				close(receiver)
			}
			return res.Fields, rows, nil
		}

		if qrr.JobReference != nil {
			jobRef = qrr.JobReference
			pageToken = qrr.PageToken
		}
	}
}

func (q *Query) retrieveRowsWithJobConfig(receiver chan ResponseData) ([]*bigquery.TableFieldSchema, []*bigquery.TableRow, error) {
	service, err := q.Client.getService()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	jobConfigQuery := bigquery.JobConfigurationQuery{
		Query: q.QueryString,
	}
	if q.JobConfig != nil {
		jobConfigQuery.AllowLargeResults = q.JobConfig.AllowLargeResults
		jobConfigQuery.WriteDisposition = string(q.JobConfig.WriteDisposition)
		jobConfigQuery.CreateDisposition = string(q.JobConfig.CreateDisposition)
		jobConfigQuery.DestinationTable = &bigquery.TableReference{DatasetId: q.Client.datasetRef.DatasetId, ProjectId: q.Client.datasetRef.ProjectId, TableId: q.JobConfig.TempTableName}
	}

	job := bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Query: &jobConfigQuery,
		},
	}

	insertedJob, err := service.Jobs.Insert(q.Client.datasetRef.ProjectId, &job).Do()
	if err != nil {
		return nil, nil, err
	}

	qr, err := service.Jobs.GetQueryResults(q.Client.datasetRef.ProjectId, insertedJob.JobReference.JobId).Do()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	if qr.JobComplete && qr.TotalRows <= uint64(len(qr.Rows)) {
		if receiver != nil {
			receiver <- ResponseData{
				Fields: qr.Schema.Fields,
				Rows:   qr.Rows,
			}
			close(receiver)
		}

		return qr.Schema.Fields, qr.Rows, nil
	}

	var rowCount int
	rows := make([]*bigquery.TableRow, 0, int(qr.TotalRows))
	if qr.JobComplete {
		if receiver != nil {
			receiver <- ResponseData{
				Rows:   qr.Rows,
				Fields: qr.Schema.Fields,
			}
		} else {
			rows = append(rows, qr.Rows...)
		}
		rowCount = len(qr.Rows)
	}

	jobRef := insertedJob.JobReference
	pageToken := qr.PageToken
	for {
		qrc := service.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId)
		if len(pageToken) != 0 {
			qrc.PageToken(pageToken)
		}
		qrr, err := qrc.Do()
		if err != nil {
			if receiver != nil {
				receiver <- ResponseData{
					Err: err,
				}
			}
			return nil, nil, err
		}

		res := ResponseData{}

		if qrr.Schema != nil {
			res.Fields = qrr.Schema.Fields
		}

		if qrr.JobComplete {
			rowCount += len(qrr.Rows)
			if receiver != nil {
				res.Rows = qrr.Rows
				res.Fields = qrr.Schema.Fields
				receiver <- res
			} else {
				rows = append(rows, qrr.Rows...)
			}
		}

		if qrr.JobComplete && uint64(rowCount) >= qrr.TotalRows {
			if receiver != nil {
				close(receiver)
			}
			return res.Fields, rows, nil
		}

		if qrr.JobReference != nil {
			jobRef = qrr.JobReference
			pageToken = qrr.PageToken
		}
	}
}

// Convert converts bigquery data to a given slice of a struct
// Compare bq type with struct property type
// ex..
// STRING -> string
// INTEGER -> int, int8, int16, int32, int64
// FLOAT -> float32, float64
// TIMESTAMP -> int64 //timestamp string is converted to unixtime milli seconds
// BOOLEAN -> bool
// TODO RECORD -> not supported yet
func Convert(fields []*bigquery.TableFieldSchema, rows []*bigquery.TableRow, result interface{}) error {
	resultV := reflect.ValueOf(result)
	if resultV.Kind() != reflect.Ptr || resultV.Elem().Kind() != reflect.Slice {
		return errors.New("Not pointer")
	}

	sliceV := resultV.Elem()
	sliceV = sliceV.Slice(0, sliceV.Cap())
	elemT := sliceV.Type().Elem()

	var count int
	for i := 0; i < len(rows); i++ {
		if elemT.NumField() != len(rows[i].F) {
			return errors.New("Invalid result element")
		}
		elemP := reflect.New(elemT)

		if len(fields) != len(rows[i].F) {
			return errors.New("Invalid fields")
		}

		for j := 0; j < len(rows[i].F); j++ {
			elemF := elemP.Elem().Field(j)
			var isSet bool
			record, ok := rows[i].F[j].V.(string)
			if !ok {
				continue
			}

			switch fields[j].Type {
			case fieldTypeString:
				switch elemF.Kind() {
				case reflect.String:
					isSet = true
					elemF.SetString(record)
				}
			case fieldTypeInteger:
				switch elemF.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int64:
					r, err := strconv.ParseInt(record, 10, 64)
					if err != nil {
						return err
					}
					isSet = true
					elemF.SetInt(r)
				}
			case fieldTypeFloat:
				switch elemF.Kind() {
				case reflect.Float32, reflect.Float64:
					r, err := strconv.ParseFloat(record, 64)
					if err != nil {
						return err
					}
					isSet = true
					elemF.SetFloat(r)
				}
			//case fieldTypeRecord:
			// not supported yet
			case fieldTypeTimestamp:
				switch elemF.Kind() {
				case reflect.Int64:
					r, err := convertExpornent(record)
					if err != nil {
						return err
					}
					isSet = true
					elemF.SetInt(r)
				}
			case fieldTypeBoolean:
				switch elemF.Kind() {
				case reflect.Bool:
					var r bool
					if record == "true" || record == "1" {
						r = true
					}
					isSet = true
					elemF.SetBool(r)
				}
			}

			if !isSet {
				return errors.New("Invalid elememt type")
			}
		}
		sliceV = reflect.Append(sliceV, elemP.Elem())
		count++
	}
	resultV.Elem().Set(sliceV.Slice(0, count))
	return nil
}

func convertExpornent(ex string) (int64, error) {
	eIndex := strings.LastIndex(ex, "E")
	if eIndex < 0 {
		return 0, errors.New("Invalid timestamp format")
	}

	dIndex := strings.LastIndex(ex[:eIndex], ".")
	if dIndex < 0 {
		return 0, errors.New("Invalid timestamp format")
	}
	e, err := strconv.Atoi(ex[eIndex+1:])
	if err != nil {
		return 0, errors.New("Invalid timestamp format")
	}

	base, err := strconv.ParseFloat(ex[:eIndex], 64)
	if err != nil {
		return 0, err
	}
	return int64(base * math.Pow10(e)), nil
}

// InsertRowsByJSON inserts a new row into the desired project, dataset and table or returns an error
func (c *Client) InsertRowsByJSON(tableID string, rows []map[string]interface{}) error {
	service, err := c.getService()
	if err != nil {
		return err
	}

	requestRows := make([]*bigquery.TableDataInsertAllRequestRows, 0, len(rows))
	for i := range rows {
		data := make(map[string]bigquery.JsonValue, len(rows[i]))
		for key := range rows[i] {
			data[key] = bigquery.JsonValue(rows[i][key])
		}
		requestRows = append(requestRows, &bigquery.TableDataInsertAllRequestRows{
			Json: data,
		})
	}

	insertRequest := &bigquery.TableDataInsertAllRequest{Rows: requestRows}

	result, err := service.Tabledata.InsertAll(c.datasetRef.ProjectId, c.datasetRef.DatasetId, tableID, insertRequest).Do()
	if err != nil {
		return err
	}

	if len(result.InsertErrors) > 0 {
		return errors.New("Failed to Insert")
	}

	return nil
}
