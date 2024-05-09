package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

type Column struct {
	name        string
	colScanType string
	colDbType   string
}

type View struct {
	Name    [2]string
	Columns []Column
}

func connect(connectionString string) (*sql.DB, error) {
	return sql.Open("pgx", connectionString)
}

func GetViews(db *sql.DB, schema string) ([]View, error) {
	//conn, err := connect(connectionString)
	//if err != nil {
	//	return nil, fmt.Errorf("can't connect to DB: %w", err)
	//}
	var views []View
	var cols []Column

	viewsList, err := Views(db)

	for key, val := range viewsList {
		//fmt.Printf("Key: %s, Value: %T\n", key, val)
		if key[0] != schema {
			continue
		}
		for _, v := range val {
			col := Column{v.Name(), v.ScanType().Name(), v.DatabaseTypeName()}
			//fmt.Printf("%s\n", PostgresColTypeToSteampipeColType(nil, col).String())
			cols = append(cols, col)
			//fmt.Printf("k: %d, Name: %s, Type: %s, DataBase Type Name: %s\n", k, v.Name(), v.ScanType(), v.DatabaseTypeName())
		}
		view := View{key, cols}
		views = append(views, view)
	}

	if err != nil {
		return nil, fmt.Errorf("can't get views: %w", err)
	}
	return views, nil
}

func GetViewsForDBSchema(ctx context.Context, connectionString, schema string) ([]View, error) {
	conn, err := connect(connectionString)
	if err != nil {
		return nil, fmt.Errorf("can't connect to DB: %w", err)
	}

	//_, err = conn.Exec(fmt.Sprintf(`set search_path="%s"`, schema))
	//if err != nil {
	//	return nil, fmt.Errorf("error setting schema: %w", err)
	//}

	views, err := GetViews(conn, schema)
	if err != nil {
		return nil, fmt.Errorf("error inspecting schema: %w", err)
	}

	return views, nil
}

/*
FindCommentOnAttrs tries to locate an Attr among the passed array that corresponds to a comment, and returns it if found.
Otherwise, returns an empty string.
This function can be used to identify the comment that is attached to a schema, table or column.
*/
//func FindCommentOnAttrs(attrs []schema.Attr) string {
//	var comment string
//	for _, attr := range attrs {
//		if _attr, ok := attr.(*schema.Comment); ok {
//			comment = _attr.Text
//		}
//	}
//	return comment
//}

/*
PostgresColTypeToSteampipeColType converts an Atlas column type to a Steampipe column.
Atlas column types correspond almost one-to-one to actual SQL types, either standard SQL or Postgres extensions.
For example, DECIMAL, FLOAT and CURRENCY become DOUBLEs on Steampipe
*/
func PostgresColTypeToSteampipeColType(ctx context.Context, col Column) proto.ColumnType {
	var x proto.ColumnType
	fmt.Printf("colDbType: %s\n", col.colDbType)
	switch col.colDbType {
	case "TEXT", "_TEXT", "UUID":
		x = proto.ColumnType_STRING
	case "BOOL", "BOOLEAN":
		x = proto.ColumnType_BOOL
	case "DOUBLE PRECISION", "FLOAT8", "FLOAT4", "NUMERIC", "DECIMAL", "CURRENCY":
		x = proto.ColumnType_DOUBLE
	case "INT4", "INT8", "INTEGER":
		x = proto.ColumnType_INT
	case "JSON", "JSONB":
		x = proto.ColumnType_JSON
	case "TIMESTAMP":
		x = proto.ColumnType_TIMESTAMP
	case "INET":
		x = proto.ColumnType_INET
	case "CIDR":
		x = proto.ColumnType_CIDR
	default:
		// As of writing this, these are the types that fall here, AKA those that we don't know how to translate
		// *schema.SpatialType, *schema.UnsupportedType, *postgres.TextSearchType, *postgres.ArrayType, *postgres.OIDType, *postgres.RangeType, *postgres.UserDefinedType, *postgres.XMLType
		x = proto.ColumnType_UNKNOWN
	}
	return x
}

/*
Builds a slice to hold the columns of a single result row. Returns an array of pointers, that can be passed to DB.Scan()
*/
func prepareSliceForScanResults(columns []string) []any {
	arr := make([]any, len(columns))

	// Convert arr into an array of pointers, so we can save the results there
	for i := range arr {
		arr[i] = &arr[i]
	}

	return arr
}

func protoToPostgresValue(val *proto.QualValue) string {
	switch val.GetValue().(type) {
	case *proto.QualValue_BoolValue:
		return fmt.Sprintf("%t", val.GetBoolValue())
	case *proto.QualValue_DoubleValue:
		return fmt.Sprintf("%f", val.GetDoubleValue())
	case *proto.QualValue_InetValue:
		return fmt.Sprintf("'%s'", val.GetInetValue().GetCidr())
	case *proto.QualValue_Int64Value:
		return fmt.Sprintf("%d", val.GetInt64Value())
	case *proto.QualValue_JsonbValue:
		return fmt.Sprintf("'%s'", val.GetJsonbValue())
	case *proto.QualValue_StringValue:
		return fmt.Sprintf("'%s'", val.GetStringValue())
	case *proto.QualValue_TimestampValue:
		return fmt.Sprintf("'%s'", val.GetTimestampValue().AsTime().Format(time.RFC3339))
	default:
		return "<INVALID>" // this will probably cause an error on the query, which is OK
	}
}

/*
MakeSQLQuery composes a SQL query from a set of quals, sends it to a remote DB, and returns any results
*/
func MakeSQLQuery(ctx context.Context, connectionString, schema string, table string, quals plugin.KeyColumnQualMap) ([]map[string]any, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s", schema, table)

	// If there are any quals, build up a WHERE clause
	conds := make([]string, 0)
	for _, qualsForCol := range quals {
		for _, qual := range qualsForCol.Quals {
			if qual.Value.Value == nil {
				conds = append(conds, fmt.Sprintf("%s %s", qual.Column, qual.Operator))
			} else {
				conds = append(conds, fmt.Sprintf("%s %s %s", qual.Column, qual.Operator, protoToPostgresValue(qual.Value)))
			}
		}
	}
	// If there are any quals, actually append the WHERE ... part to the query string (which currently has SELECT * FROM tablename)
	if len(conds) > 0 {
		query = query + " WHERE " + strings.Join(conds, " AND ")
	}

	return MakeRawSQLQuery(ctx, connectionString, schema, table, query)
}

/*
MakeRawSQLQuery sends a raw SQL query to a remote DB, and returns any results
*/
func MakeRawSQLQuery(ctx context.Context, connectionString, schema string, table string, query string) ([]map[string]any, error) {
	conn, err := connect(connectionString)
	if err != nil {
		return nil, fmt.Errorf("can't connect to DB: %w", err)
	}
	defer conn.Close()

	plugin.Logger(ctx).Debug("MakeRawSQLQuery.beforeExec", "query", query)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error while making query \"%s\": %w", query, err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error while reading column names: %w", err)
	}

	// The code here that stores results on a slice of map[string]any was inspired by https://lazypro.medium.com/make-sql-scan-result-be-map-in-golang-e04f0de5950f
	var results []map[string]any
	for rows.Next() {
		rowData := make(map[string]any)
		cols := prepareSliceForScanResults(colNames)

		if err := rows.Scan(cols...); err != nil {
			return nil, fmt.Errorf("error while reading columns: %w", err)
		}

		for i, v := range cols {
			rowData[colNames[i]] = v
		}
		plugin.Logger(ctx).Debug("Scan", "data", cols, "mapData", rowData)
		results = append(results, rowData)
	}

	// This must always be called after the for rows.Next() loop, since it may have terminated with an error
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error while reading columns: %w", err)
	}
	return results, nil
}
