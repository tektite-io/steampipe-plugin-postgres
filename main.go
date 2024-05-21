package main

import (
	"github.com/tektite-io/steampipe-plugin-postgres/postgres"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		PluginFunc: postgres.Plugin})
}
