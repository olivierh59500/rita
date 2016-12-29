package export

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
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
			tss = append(tss, int64(connData["ts"].(float64)))
		}
		entry.Timestamps = tss
		wc <- entry
	}
}

func (e *Exporter) scanWriteThread(c chan scanOutput, wg *sync.WaitGroup) int {

	scanPath := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "scan.csv")
	f_scan, err := os.Create(scanPath)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return 0
	}
	w_scan := csv.NewWriter(f_scan)

	scanTssPath := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "scan_tss.csv")
	f_scanTss, err := os.Create(scanTssPath)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return 0
	}
	w_scanTss := csv.NewWriter(f_scanTss)

	scanPortPath := filepath.Join(e.cfg.System.ExportConfig.ExportPath, "scan_ports.csv")
	f_scanPort, err := os.Create(scanPortPath)
	if err != nil {
		e.cfg.Log.WithFields(log.Fields{
			"Error": err,
		}).Error("Error Opening Beacon Output File")
		return 0
	}
	w_scanPort := csv.NewWriter(f_scanPort)

	scanData := [][]string{{"id", "src", "dst", "port_count"}}
	scanTssData := [][]string{{"id", "ts", "scan"}}
	scanPortData := [][]string{{"id", "port", "scan"}}

	count := 0
	tssId := 0
	portId := 0

	for {
		entry, more := <-c
		if !more {
			break
		}
		scanData = append(scanData, []string{strconv.Itoa(count), entry.Src, entry.Dst, strconv.Itoa(int(entry.PortCount))})
		for _, ts := range entry.Timestamps {
			scanTssData = append(scanTssData, []string{strconv.Itoa(tssId), strconv.Itoa(int(ts)), strconv.Itoa(count)})
			tssId += 1
		}

		for _, port := range entry.PortSet {
			scanPortData = append(scanPortData, []string{strconv.Itoa(portId), strconv.Itoa(int(port)), strconv.Itoa(count)})
			portId += 1
		}

		count += 1
	}

	w_scan.WriteAll(scanData)
	w_scanPort.WriteAll(scanPortData)
	w_scanTss.WriteAll(scanTssData)

	// e.cfg.Log.Info("Sorting Scan Results")

	// sort.Sort(results)

	// e.cfg.Log.Info("Marshalling Scan Results")

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
