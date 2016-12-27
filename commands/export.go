package commands

import (
	"github.com/ocmdev/rita/config"
	"github.com/ocmdev/rita/export"

	"github.com/urfave/cli"
)

func init() {
	exportCommand := cli.Command{
		Name:  "export",
		Usage: "Export results",
		Flags: []cli.Flag{
			configFlag,
			databaseFlag,
			outPathFlag,
		},
		Action: doExport,
	}

	bootstrapCommands(exportCommand)
}

// doExport runs the exporter
func doExport(c *cli.Context) error {
	conf := config.InitConfig(c.String("config"))
	conf.System.ExportConfig.ExportPath = c.String("out")
	exp := export.NewExporter(conf, c.String("dataset"))
	exp.ExportAll(export.JSON)
	return nil
}
