package export

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	datatype_TBD "github.com/ocmdev/rita/datatypes/TBD"
)

type (
	beaconOutput struct {
		ID            int64   `json:"id"`
		Src           string  `json:"src"`
		Dst           string  `json:"dst"`
		Range         int64   `json:"range"`
		Size          int64   `json:"size"`
		RangeVals     string  `json:"range_vals"`
		Fill          float64 `json:"fill"`
		Spread        float64 `json:"spread"`
		Sum           int64   `json:"range_size"`
		Score         float64 `json:"score"`
		Intervals     []int64 `json:"intervals"`
		InvervalCount []int64 `json:"interval_counts"`
		Tss           []int64 `json:"tss"`
	}
)

// func (e *Exporter) GetBeaconExporter() {
// 	var retrieve func(c chan interface{})
// 	retrieve = func(c chan interface{}) {
// 		iter := e.cfg.Session.
// 			DB(e.db).
// 			C(e.cfg.System.TBDConfig.TBDTable).
// 			Find(nil).
// 			Batch(e.cfg.System.BatchSize).
// 			Prefetch(e.cfg.System.Prefetch).
// 			// Sort("Score").
// 			Iter()
//
// 		var d datatype_TBD.TBD
//
// 		for iter.Next(&d) {
// 			c <- d
// 		}
// 	}
// }

func (e *Exporter) ExportBeacon(lType LogType) {
	myStart := time.Now()
	iter := e.cfg.Session.
		DB(e.db).
		C(e.cfg.System.TBDConfig.TBDTable).
		Find(nil).
		Batch(e.cfg.System.BatchSize).
		Prefetch(e.cfg.System.Prefetch).
		// Sort("Score").
		Iter()

	var d datatype_TBD.TBD
	count := int64(0)
	var exportList []beaconOutput

	for iter.Next(&d) {
		var out beaconOutput
		out.Dst = d.Dst
		out.Fill = d.Fill
		out.ID = count
		out.Intervals = d.Intervals
		out.InvervalCount = d.InvervalCount
		out.Range = d.Range
		out.RangeVals = d.RangeVals
		out.Score = d.Score
		out.Size = d.Size
		out.Spread = d.Spread
		out.Src = d.Src
		out.Sum = d.Sum
		out.Tss = d.Tss

		exportList = append(exportList, out)
		count += 1
	}

	json, err := json.Marshal(exportList)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Marshalling Beacon Results")
		return
	}

	path := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "beacon.json")
	f, err := os.Create(path)

	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return
	}

	f.Write(json)

	avgTime := 0.0
	if count > 0 {
		avgTime = float64(time.Since(myStart).Seconds()) / float64(count)
	}
	e.cfg.Log.WithFields(log.Fields{
		"total_time":        time.Since(myStart).String(),
		"second_per_record": avgTime,
		"count":             count,
	}).Info("Completed beacons export")
}
