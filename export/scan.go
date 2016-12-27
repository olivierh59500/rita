package export

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"

	log "github.com/Sirupsen/logrus"
)

type (
	scanOutput struct {
		Src             string  `bson:"src",json:"src"`
		Dst             string  `bson:"dst",json:"dst"`
		ConnectionCount int32   `bson:"connection_count",json:"connection_count"`
		LocalSrc        bool    `bson:"local_src",json:"local_src"`
		LocalDst        bool    `bson:"local_dst",json:"local_dst"`
		TotalBytes      int64   `bson:"total_bytes",json:"total_bytes"`
		AverageBytes    float64 `bson:"avg_bytes",json:"avg_bytes"`
		TotalDuration   float64 `bson:"total_duration",json:"total_duration"`
		PortSet         []int32 `bson:"port_set",json:"port_set"`
		PortCount       int32   `bson:"port_count",json:"port_count"`
		Timestamps      []int64 `bson:"tss",json:"tss"`
	}

	scanOutputs []scanOutput
)

func (slice scanOutputs) Len() int {
	return len(slice)
}

func (slice scanOutputs) Less(i, j int) bool {
	return slice[i].ConnectionCount < slice[j].ConnectionCount
}

func (slice scanOutputs) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (e *Exporter) scanRetrieveThread(c chan scanOutput) {
	iter := e.cfg.Session.
		DB(e.db).
		C(e.cfg.System.ScanningConfig.ScanTable).
		Find(nil).
		Batch(e.cfg.System.BatchSize).
		Prefetch(e.cfg.System.Prefetch).
		// Sort("Score").
		Iter()

	var s scanOutput

	for iter.Next(&s) {
		c <- s
	}
}

func (e *Exporter) scanWorkThread(c chan scanOutput, wc chan scanOutput, wg *sync.WaitGroup) {
	for {
		entry, more := <-c
		if !more {
			wg.Done()
			return
		}
		iter := e.cfg.Session.
			DB(e.db).
			C(e.cfg.System.StructureConfig.ConnTable).
			Find(bson.M{"id_origin_h": entry.Src, "id_resp_h": entry.Dst}).
			Batch(e.cfg.System.BatchSize).
			Prefetch(e.cfg.System.Prefetch).
			// Sort("Score").
			Iter()

		var tss []int64
		// var connData struct {
		// 	ts int64 `bson:"ts"`
		// }
		var connData map[string]interface{}

		for iter.Next(&connData) {
			tss = append(tss, connData["ts"].(int64))
		}
		entry.Timestamps = tss
		wc <- entry
	}
}

func (e *Exporter) scanWriteThread(c chan scanOutput, wg *sync.WaitGroup) int {
	var results scanOutputs
	count := 0

	for {
		entry, more := <-c
		if !more {
			break
		}
		results = append(results, entry)
		count += 1
	}

	e.cfg.Log.Info("Sorting Scan Results")

	sort.Sort(results)

	e.cfg.Log.Info("Marshalling Scan Results")

	json, err := json.Marshal(results)

	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Marshalling Scan Results")
		wg.Done()
		return 0
	}

	path := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "scan.json")

	f, err := os.Create(path)

	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Scan Output File")
		wg.Done()
		return 0
	}

	f.Write(json)

	wg.Done()

	return count
}

func (e *Exporter) ExportScan(lType LogType) {
	myStart := time.Now()
	count := 0
	c := make(chan scanOutput, 1000)
	writeChan := make(chan scanOutput, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go e.scanWorkThread(c, writeChan, &wg)
	}

	var writeWait sync.WaitGroup
	writeWait.Add(1)
	go func() {
		count = e.scanWriteThread(writeChan, &writeWait)
	}()

	e.scanRetrieveThread(c)

	close(c)
	wg.Wait()

	close(writeChan)
	writeWait.Wait()

	avgTime := 0.0
	if count > 0 {
		avgTime = float64(time.Since(myStart).Seconds()) / float64(count)
	}
	e.cfg.Log.WithFields(log.Fields{
		"total_time":        time.Since(myStart).String(),
		"second_per_record": avgTime,
		"count":             count,
	}).Info("Completed scans export")
}
