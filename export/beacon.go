package export

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	datatype_TBD "github.com/ocmdev/rita/datatypes/TBD"
)

// type (
// 	beaconOutput struct {
// 		ID            int64   `json:"id"`
// 		Src           string  `json:"src"`
// 		Dst           string  `json:"dst"`
// 		Range         int64   `json:"range"`
// 		Size          int64   `json:"size"`
// 		RangeVals     string  `json:"range_vals"`
// 		Fill          float64 `json:"fill"`
// 		Spread        float64 `json:"spread"`
// 		Sum           int64   `json:"range_size"`
// 		Score         float64 `json:"score"`
// 		Intervals     []int64 `json:"intervals"`
// 		InvervalCount []int64 `json:"interval_counts"`
// 		Tss           []int64 `json:"tss"`
// 	}
// )

func (e *Exporter) ExportBeacon(lType LogType) {
	myStart := time.Now()

	beaconPath := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "beacon.csv")
	f_beacon, err := os.Create(beaconPath)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return
	}
	w_beacon := csv.NewWriter(f_beacon)

	beaconTssPath := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "beacon_tss.csv")
	f_tss, err := os.Create(beaconTssPath)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return
	}
	w_tss := csv.NewWriter(f_tss)

	beaconIntPath := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "beacon_interval.csv")
	f_int, err := os.Create(beaconIntPath)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return
	}
	w_int := csv.NewWriter(f_int)

	iter := e.cfg.Session.
		DB(e.db).
		C(e.cfg.System.TBDConfig.TBDTable).
		Find(nil).
		Batch(e.cfg.System.BatchSize).
		Prefetch(e.cfg.System.Prefetch).
		// Sort("Score").
		Iter()

	beaconData := []string{"id", "src", "dst", "score", "coverage"}
	if err := w_beacon.Write(beaconData); err != nil {
		log.Fatalln("error writing record to csv:", err)
	}

	tssData := []string{"id", "ts", "beacon"}
	if err := w_tss.Write(tssData); err != nil {
		log.Fatalln("error writing record to csv:", err)
	}

	intervalData := []string{"id", "interval", "count", "beacon"}
	if err := w_int.Write(intervalData); err != nil {
		log.Fatalln("error writing record to csv:", err)
	}

	var d datatype_TBD.TBD
	count := int64(0)
	tssId := 0
	intId := 0

	for iter.Next(&d) {
		objid := int(count)
		src := d.Src
		dst := d.Dst
		score := d.Score
		coverage := d.Spread

		beaconData = []string{strconv.Itoa(objid), src, dst, strconv.FormatFloat(score, 'f', 4, 64), strconv.FormatFloat(coverage, 'f', 4, 64)}
		if err := w_beacon.Write(beaconData); err != nil {
			log.Fatalln("error writing record to csv:", err)
			continue
		}

		for _, ts := range d.Tss {
			tssData = []string{strconv.Itoa(tssId), strconv.Itoa(int(ts)), strconv.Itoa(objid)}
			if err := w_tss.Write(tssData); err != nil {
				log.Fatalln("error writing record to csv:", err)
			}
			tssId += 1
		}

		for i, interval := range d.Intervals {
			intervalData = []string{strconv.Itoa(intId), strconv.Itoa(int(interval)), strconv.Itoa(int(d.InvervalCount[i])), strconv.Itoa(objid)}
			if err := w_int.Write(intervalData); err != nil {
				log.Fatalln("error writing record to csv:", err)
			}
		}

		count += 1
	}

	w_beacon.Flush()
	w_int.Flush()
	w_tss.Flush()

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
