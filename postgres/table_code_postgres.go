package postgres

import (
	"context"
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func tablePostgres(ctx context.Context, connection *plugin.Connection) (*plugin.Table, error) {
	view := ctx.Value(keyTable).(View)
	name := view.Name

	return &plugin.Table{
		Name:        name[1],
		Description: "", //eAtlas.Attrs),
		List: &plugin.ListConfig{
			Hydrate:    ListTableRecords,
			KeyColumns: makeKeyColumns(ctx, view),
		},
		Columns: makeColumns(ctx, view),
	}, nil
}

func getMapKey(ctx context.Context, d *transform.TransformData) (interface{}, error) {

	asMap, ok := d.HydrateItem.(map[string]any)

	if !ok {
		plugin.Logger(ctx).Error("postgres.hydrate.getMapKey", "err", "invalid type", "expected", "map[string]any", "actual", fmt.Sprintf("%T", d.HydrateItem))
		return nil, fmt.Errorf("can't convert hydrate item %v to map", d.HydrateItem)
	}

	key := d.Param.(string)

	plugin.Logger(ctx).Debug("postgres.getMapKey", "key", key)
	plugin.Logger(ctx).Debug("postgres.getMapKey", "asMap[key]", asMap[key])

	v := detectAndProcessJSON(ctx, asMap[key])
	return v, nil
}

func makeColumns(ctx context.Context, view View) []*plugin.Column {
	columns := make([]*plugin.Column, 0, len(view.Columns))

	for _, col := range view.Columns {
		postgresType := PostgresColTypeToSteampipeColType(ctx, col)
		if postgresType == proto.ColumnType_UNKNOWN {
			plugin.Logger(ctx).Warn("postgres.makeColumns", "msg", "unknown type, skipping column!", "column", col.name, "type", col.colScanType)
			continue
		}

		columns = append(columns, &plugin.Column{
			Name:        col.name,
			Type:        postgresType,
			Description: "", //TODO
			Transform:   transform.FromP(getMapKey, col.name),
		})
	}

	return columns
}

func makeKeyColumns(ctx context.Context, view View) plugin.KeyColumnSlice {
	var all = make([]*plugin.KeyColumn, 0, len(view.Columns))

	for _, col := range view.Columns {
		postgresType := PostgresColTypeToSteampipeColType(ctx, col)
		if postgresType == proto.ColumnType_UNKNOWN {
			plugin.Logger(ctx).Warn("postgres.makeColumns", "msg", "unknown type, skipping column!", "column", col.name, "type", col.colScanType)
			continue
		}
		all = append(all, &plugin.KeyColumn{
			Name:      col.name,
			Operators: plugin.GetValidOperators(), // Everything is valid! Just reuse Steampipe's own "list of all operators that can be handled"
			Require:   plugin.Optional,
		})
	}

	plugin.Logger(ctx).Info("makeKeyColumns.done", "val", all)
	return all
}

func ListTableRecords(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	config := GetConfig(d.Connection)
	connectionString, err := config.GetConnectionString()
	if err != nil {
		return nil, err
	}
	schemaName := config.GetSchema()

	plugin.Logger(ctx).Debug("postgres.ListTable", "quals", d.Quals)
	plugin.Logger(ctx).Debug("postgres.ListTable", "schema", schemaName)

	results, err := MakeSQLQuery(ctx, connectionString, schemaName, d.Table.Name, d.Quals)
	if err != nil {
		return nil, err
	}

	for _, result := range results {
		d.StreamListItem(ctx, result)
	}

	return nil, nil
}
