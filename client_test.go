package client

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/api/bigquery/v2"
)

type convertRec struct {
	Name      string
	Age       int
	Score     float32
	Timestamp int64
	IsDeleted bool
}

func TestNew(t *testing.T) {
	Convey("Given necessary data for client", t, func() {
		email := "example@gmail.com"
		key := []byte("this is test pem dummy")
		subject := ""

		Convey("When create a new client", func() {
			c := New(email, key, subject)

			Convey("Then client has a config", func() {
				So(c.jwtConfig, ShouldNotBeNil)
				So(c.jwtConfig.Email, ShouldEqual, email)

			})
		})
	})
}

func TestDataset(t *testing.T) {
	Convey("Given necessary data for client and dataset", t, func() {
		email := "example@gmail.com"
		key := []byte("this is test pem dummy")
		subject := ""
		projectID := "winter_test00"
		datasetID := "bq_test"

		Convey("When create a new client", func() {
			c := New(email, key, subject)
			c.Dataset(projectID, datasetID)

			Convey("Then client has a config", func() {
				So(c.datasetRef, ShouldNotBeNil)
				So(c.datasetRef.ProjectId, ShouldEqual, projectID)
				So(c.datasetRef.DatasetId, ShouldEqual, datasetID)

			})
		})
	})
}

func TestGetService(t *testing.T) {
	Convey("Given initialized client", t, func() {
		email := "example@gmail.com"
		key := []byte("this is test pem dummy")
		subject := ""
		projectID := "winter_test00"
		datasetID := "bq_test"
		c := New(email, key, subject)
		c.Dataset(projectID, datasetID)

		Convey("When get a new service", func() {
			service, err := c.getService()

			Convey("Then client has a config", func() {
				So(err, ShouldBeNil)
				So(service, ShouldNotBeNil)
				So(c.service, ShouldNotBeNil)
			})
		})
	})
}

func TestConvert(t *testing.T) {
	Convey("Given invalid fields", t, func() {
		fields := []*bigquery.TableFieldSchema{
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "name",
				Type:        "STRING",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "age",
				Type:        "INTEGER",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "score",
				Type:        "FLOAT",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "timestamp",
				Type:        "TIMESTAMP",
			},
		}

		rows := []*bigquery.TableRow{
			&bigquery.TableRow{
				F: []*bigquery.TableCell{
					{
						V: "test_name",
					},
					{
						V: "26",
					},
					{
						V: "12.34",
					},
					{
						V: "1.422943323461E9",
					},
				},
			},
		}

		res := []convertRec{}

		Convey("When convert bigquery data", func() {
			err := Convert(fields, rows, &res)

			Convey("Then err is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "Invalid result element")

			})
		})
	})

	Convey("Given bigquery fields, rows and result receiver containing nil", t, func() {
		fields := []*bigquery.TableFieldSchema{
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "name",
				Type:        "STRING",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "age",
				Type:        "INTEGER",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "score",
				Type:        "FLOAT",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "timestamp",
				Type:        "TIMESTAMP",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "idDeleted",
				Type:        "BOOLEAN",
			},
		}

		rows := []*bigquery.TableRow{
			&bigquery.TableRow{
				F: []*bigquery.TableCell{
					{
						V: nil,
					},
					{
						V: "26",
					},
					{
						V: "12.34",
					},
					{
						V: "1.422943323461E9",
					},
					{
						V: "true",
					},
				},
			},
		}

		res := []convertRec{}

		Convey("When convert bigquery data", func() {
			err := Convert(fields, rows, &res)

			Convey("Then results resolved nil are set into a given receiver", func() {
				So(err, ShouldBeNil)
				So(len(res), ShouldEqual, 1)
				So(res[0].Name, ShouldEqual, "")
				So(res[0].Age, ShouldEqual, 26)
				So(res[0].Score, ShouldEqual, 12.34)
				So(res[0].Timestamp, ShouldEqual, 1422943323461)
				So(res[0].IsDeleted, ShouldEqual, true)

			})
		})
	})

	Convey("Given bigquery fields, rows and result receiver", t, func() {
		fields := []*bigquery.TableFieldSchema{
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "name",
				Type:        "STRING",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "age",
				Type:        "INTEGER",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "score",
				Type:        "FLOAT",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "timestamp",
				Type:        "TIMESTAMP",
			},
			{
				Description: "",
				Fields:      nil,
				Mode:        "NULLABLE",
				Name:        "idDeleted",
				Type:        "BOOLEAN",
			},
		}

		rows := []*bigquery.TableRow{
			&bigquery.TableRow{
				F: []*bigquery.TableCell{
					{
						V: "test_name",
					},
					{
						V: "26",
					},
					{
						V: "12.34",
					},
					{
						V: "1.422943323461E9",
					},
					{
						V: "true",
					},
				},
			},
		}

		res := []convertRec{}

		Convey("When convert bigquery data", func() {
			err := Convert(fields, rows, &res)

			Convey("Then results are set into a given receiver", func() {
				So(err, ShouldBeNil)
				So(len(res), ShouldEqual, 1)
				So(res[0].Name, ShouldEqual, "test_name")
				So(res[0].Age, ShouldEqual, 26)
				So(res[0].Score, ShouldEqual, 12.34)
				So(res[0].Timestamp, ShouldEqual, 1422943323461)
				So(res[0].IsDeleted, ShouldEqual, true)

			})
		})
	})
}
