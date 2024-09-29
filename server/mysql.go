package server

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/volkszaehler/mbmd/meters"
)

type InsertData struct {
	fields string
	values []interface{}
}

// MySQL publisher
type MySQL struct {
	client           *sql.DB
	insertOnlyFields []string
	insertData       []InsertData
}

// NewMySQLClient creates new publisher for MySQL
func NewMySQLClient(
	host string,
	user string,
	password string,
	database string,
	includeMeasurements string,
) *MySQL {
	connString := user + ":" + password + "@tcp(" + host + ")/" + database
	db, err := sql.Open("mysql", connString)
	if err != nil {
		panic(err.Error())
	}

	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	insertOnlyFields := []string{}
	if includeMeasurements != "" {
		insertOnlyFields = strings.Split(includeMeasurements, ",")
	}

	return &MySQL{
		client:           db,
		insertData:       nil,
		insertOnlyFields: insertOnlyFields,
	}
}

// Run MySQL publisher
func (m *MySQL) Run(in <-chan QuerySnip) {
	var mu sync.Mutex

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case snip, ok := <-in:
			if !ok {
				continue
			}

			if !m.mustInsert(snip) {
				continue
			}

			description, unit := snip.Measurement.DescriptionAndUnit()

			mu.Lock()

			insert := InsertData{
				fields: "(?, ?, ?, ?, ?, ?)",
				values: []interface{}{snip.Timestamp.Unix(), snip.Device, snip.Measurement.String(), snip.Value, description, unit},
			}

			m.insertData = append(m.insertData, insert)

			mu.Unlock()

		case <-ticker.C:
			if len(m.insertData) == 0 {
				continue
			}

			mu.Lock()

			err := m.InsertValues()
			if err != nil {
				fmt.Println("Error in prepare statement:", err)
			}

			mu.Unlock()
		}
	}
}

func (m *MySQL) mustInsert(s QuerySnip) bool {
	if len(m.insertOnlyFields) == 0 {
		return true
	}
	measurement := s.Measurement.String()

	for _, incldueMeasurement := range m.insertOnlyFields {
		if incldueMeasurement == measurement {
			return true
		}
	}

	return false
}

func (m *MySQL) getInsertBatch(batchNb, batchSize int) (batch []InsertData) {
	for i := batchNb * batchSize; i < len(m.insertData); i++ {
		batch = append(batch, m.insertData[i])
	}

	return batch
}

func (m *MySQL) InsertValues() (err error) {
	for batchNb := 0; ; batchNb++ {
		batch := m.getInsertBatch(batchNb, 100)

		if len(batch) == 0 {
			break
		}

		fields := []string{}
		values := []interface{}{}

		for _, insert := range batch {
			fields = append(fields, insert.fields)
			values = append(values, insert.values...)
		}

		sql := "INSERT IGNORE INTO readings (tstamp, device, measurement, value, description, unit) " +
			" VALUES " + strings.Join(fields, ",")

		stmt, err := m.client.Prepare(sql)
		if err != nil {
			return err
		}
		defer stmt.Close()

		if _, err := stmt.Exec(values...); err != nil {
			return err
		}

		fmt.Println(time.Now().Format("15:04:05"), "Batch:", batchNb, ", items", len(fields))
	}

	m.insertData = nil

	return nil
}

func (m *MySQL) MeasurementReader(device string, measurements string, unixFrom int64, unixTo int64, group int64) (readings []*Readings, err error) {
	mArray := strings.Split(measurements, ",")
	div := strconv.Itoa(int(group))

	sql := `SELECT FLOOR(tstamp / ` + div + `) * ` + div + ` as t, AVG(value), measurement
		    FROM readings
	        WHERE device = ?
			    AND tstamp >= ?
				AND tstamp < ?
				AND measurement in (?` + strings.Repeat(",?", len(mArray)-1) + `)
			GROUP BY t, measurement
			ORDER BY t`

	stmt, err := m.client.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	args := []interface{}{device, unixFrom, unixTo}

	for _, measurement := range mArray {
		args = append(args, strings.Trim(measurement, " "))
	}

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

out:
	for rows.Next() {
		var value float64
		var measurement string
		var tstamp int64

		err = rows.Scan(&tstamp, &value, &measurement)
		if err != nil {
			return nil, err
		}

		m, err := meters.MeasurementString(measurement)
		if err != nil {
			continue
		}

		for i, reading := range readings {
			if reading.Timestamp.Unix() == tstamp {
				readings[i].Values[m] = value
				continue out
			}
		}

		tmp := new(Readings)
		tmp.Timestamp = time.Unix(tstamp, 0)
		tmp.Values = map[meters.Measurement]float64{m: value}

		readings = append(readings, tmp)
	}

	return readings, nil
}
