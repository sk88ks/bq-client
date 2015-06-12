package client

import (
	"errors"
	"io/ioutil"
	"math"
	"reflect"
	"strconv"
	"strings"

	bigquery "github.com/google/google-api-go-client/bigquery/v2"

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

// Client is a client for google bigquery
type Client struct {
	jwtConfig  *jwt.Config
	datasetRef *bigquery.DatasetReference
	service    *bigquery.Service
	JobConfig  *JobConfiguration
}

// JobConfiguration for bigquery client
type JobConfiguration struct {
	AllowLargeResults bool
	TempTableName     string
	// WRITE_TRUNCATE
	// WRITE_APPEND
	// WRITE_EMPTY
	WriteDisposition string
	// CREATE_IF_NEEDED
	// CREATE_NEVER
	CreateDisposition string
}

// ResponseData is a data set for response from bigquery
type ResponseData struct {
	Fields  []*bigquery.TableFieldSchema
	Rows    []*bigquery.TableRow
	AllRows bool
	Err     error
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

// SetJobConfig sets job Configuration
func (c *Client) SetJobConfig(config *JobConfiguration) *Client {
	c.JobConfig = config
	return c
}

// Query execute a given query
func (c *Client) Query(queryString string, result interface{}) error {
	var fields []*bigquery.TableFieldSchema
	var rows []*bigquery.TableRow
	var err error
	if c.JobConfig != nil {
		fields, rows, err = c.retrieveRowsWithJobConfig(queryString, defaultPageSize, nil)
	}
	if err != nil {
		return err
	}
	Convert(fields, rows, result)
	return nil
}

func (c *Client) retrieveRows(queryString string, size int64, receiver chan ResponseData) ([]*bigquery.TableFieldSchema, []*bigquery.TableRow, error) {
	service, err := c.getService()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: c.datasetRef,
		MaxResults:     size,
		Kind:           "json",
		Query:          queryString,
	}

	qr, err := service.Jobs.Query(c.datasetRef.ProjectId, query).Do()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	if qr.JobComplete && qr.TotalRows <= uint64(size) {
		if receiver != nil {
			receiver <- ResponseData{
				Fields:  qr.Schema.Fields,
				Rows:    qr.Rows,
				AllRows: true,
			}
		}

		return qr.Schema.Fields, qr.Rows, nil
	}

	jobRef := qr.JobReference
	pageToken := qr.PageToken
	rows := make([]*bigquery.TableRow, 0, int(qr.TotalRows))
	rows = append(rows, qr.Rows...)
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

		res := ResponseData{
			Rows: qrr.Rows,
		}

		if qrr.Schema != nil {
			res.Fields = qrr.Schema.Fields
		}

		if qrr.JobComplete {
			rows = append(rows, qrr.Rows...)
			if receiver != nil {
				res.Rows = rows
				receiver <- res
			}
		}

		if qrr.JobComplete && uint64(len(rows)) >= qrr.TotalRows {
			res.AllRows = true
			res.Rows = rows
			if receiver != nil {
				res.Rows = rows
				receiver <- res
			}
			return res.Fields, res.Rows, nil
		}

		if qrr.JobReference != nil {
			jobRef = qrr.JobReference
			pageToken = qrr.PageToken
		}
	}
}

func (c *Client) retrieveRowsWithJobConfig(queryString string, size int64, receiver chan ResponseData) ([]*bigquery.TableFieldSchema, []*bigquery.TableRow, error) {
	service, err := c.getService()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	jobConfigQuery := bigquery.JobConfigurationQuery{
		Query: queryString,
	}
	if c.JobConfig != nil {
		jobConfigQuery.AllowLargeResults = c.JobConfig.AllowLargeResults
		jobConfigQuery.WriteDisposition = c.JobConfig.WriteDisposition
		jobConfigQuery.CreateDisposition = c.JobConfig.CreateDisposition
		jobConfigQuery.DestinationTable = &bigquery.TableReference{DatasetId: c.datasetRef.DatasetId, ProjectId: c.datasetRef.ProjectId, TableId: c.JobConfig.TempTableName}
	}

	job := bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Query: &jobConfigQuery,
		},
	}

	insertedJob, err := service.Jobs.Insert(c.datasetRef.ProjectId, &job).Do()
	if err != nil {
		return nil, nil, err
	}

	qr, err := service.Jobs.GetQueryResults(c.datasetRef.ProjectId, insertedJob.JobReference.JobId).Do()
	if err != nil {
		if receiver != nil {
			receiver <- ResponseData{
				Err: err,
			}
		}
		return nil, nil, err
	}

	if qr.JobComplete && qr.TotalRows <= uint64(size) {
		if receiver != nil {
			receiver <- ResponseData{
				Fields:  qr.Schema.Fields,
				Rows:    qr.Rows,
				AllRows: true,
			}
		}

		return qr.Schema.Fields, qr.Rows, nil
	}

	//jobRef := qr.JobReference
	jobRef := insertedJob.JobReference
	pageToken := qr.PageToken
	rows := make([]*bigquery.TableRow, 0, int(qr.TotalRows))
	rows = append(rows, qr.Rows...)
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

		res := ResponseData{
			Rows: qrr.Rows,
		}

		if qrr.Schema != nil {
			res.Fields = qrr.Schema.Fields
		}

		if qrr.JobComplete {
			rows = append(rows, qrr.Rows...)
			if receiver != nil {
				res.Rows = rows
				receiver <- res
			}
		}

		if qrr.JobComplete && uint64(len(rows)) >= qrr.TotalRows {
			res.AllRows = true
			res.Rows = rows
			if receiver != nil {
				res.Rows = rows
				receiver <- res
			}
			return res.Fields, res.Rows, nil
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
		return errors.New("Failed to insert")
	}

	return nil
}
