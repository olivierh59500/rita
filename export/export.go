package export

import (
	log "github.com/Sirupsen/logrus"
	"github.com/ocmdev/rita/config"
)

type (
	Exporter struct {
		cfg  *config.Resources
		db   string
		path string
	}

	LogType int
)

const JSON = 0
const CSV = 1

func NewExporter(c *config.Resources, d string) *Exporter {
	return &Exporter{
		cfg:  c,
		db:   d,
		path: c.System.ExportConfig.ExportPath,
	}
}

func (e *Exporter) ExportAll(lType LogType) {
	e.cfg.Log.WithFields(log.Fields{"Database": e.db}).Info("Starting Export")

	if len(e.db) == 0 {
		e.cfg.Log.Error("No database name provided")
		return
	}

	// var wg sync.WaitGroup
	// wg.Add(1)

	// go func() {
	// 	defer wg.Done()
	// 	e.ExportBeacon(lType)
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	e.ExportScan(lType)
	// }()

	// wg.Wait()

	// e.ExportBeacon(lType)
	e.ExportScan(lType)

	e.cfg.Log.Infoln("Finished Export")
}
