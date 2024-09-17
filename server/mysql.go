package server

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/volkszaehler/mbmd/meters"
)

// MySQL publisher
type MySQL struct {
	client *sql.DB
}

// NewMySQLClient creates new publisher for MySQL
func NewMySQLClient(
	host string,
	user string,
	password string,
	database string,
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

	return &MySQL{
		client: db,
	}
}

// Run MySQL publisher
func (m *MySQL) Run(in <-chan QuerySnip) {

	var items []string
	var vals []interface{}
	var sql string
	var mu sync.Mutex

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case snip, ok := <-in:
			if !ok {
				return
			}

			description, unit := snip.Measurement.DescriptionAndUnit()

			mu.Lock()
			items = append(items, "(?, ?, ?, ?, ?, ?)")
			vals = append(vals, snip.Device, snip.Measurement.String(), snip.Value, snip.Timestamp.Unix(), description, unit)
			mu.Unlock()

		case <-ticker.C:
			if len(items) == 0 {
				fmt.Println("Nothing to do ...", time.Now().Unix())
				continue
			}

			mu.Lock()

			sql = "INSERT INTO readings (device, measurement, value, tstamp, description, unit) " +
				" VALUES " + strings.Join(items, ",")

			stmt, err := m.client.Prepare(sql)
			if err != nil {
				fmt.Println("Error preparing statement:", err)
				mu.Unlock()
				continue
			}
			if _, err := stmt.Exec(vals...); err != nil {
				fmt.Println("Error executing statement:", err)
			}

			fmt.Println("Added: ", time.Now().Unix(), len(items))

			items = nil
			vals = nil
			mu.Unlock()

			stmt.Close()
		}
	}
}

func (m *MySQL) MeasurementReader(device string, measurements string, unixFrom int64, unixTo int64) (readings []*Readings, err error) {
	mArray := strings.Split(measurements, ",")

	sql := `SELECT MIN(tstamp), AVG(value), measurement
		    FROM readings
	        WHERE device = ?
			    AND tstamp >= ?
				AND tstamp < ?
				AND measurement in (?` + strings.Repeat(",?", len(mArray)-1) + `)
			GROUP BY FROM_UNIXTIME(tstamp, '%Y-%m-%d %H:%i'), measurement
			ORDER BY tstamp`

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
