package postgres

import (
	"database/sql"
	"strings"
)

type dialect interface {
	escapeIdent(ident string) string

	ColumnTypes(db *sql.DB, schema, name string) ([]*sql.ColumnType, error)
	PrimaryKey(db *sql.DB, schema, name string) ([]string, error)
	TableNames(db *sql.DB) ([][2]string, error)
	ViewNames(db *sql.DB) ([][2]string, error)
	ViewNamesSchema(db *sql.DB, schema string) ([][2]string, error)
}

// driverDialect is a registry, mapping database/sql driver names to database dialects.
// This is somewhat fragile.
var driverDialect = map[string]dialect{
	"*pq.Driver":                  postgresDialect{}, // github.com/lib/pq
	"*stdlib.Driver":              postgresDialect{}, // github.com/jackc/pgx
	"*pgsqldriver.postgresDriver": postgresDialect{}, // github.com/jbarham/gopgsqldriver
}

// TODO Should we expose a method of registering a driver string/dialect in our registry?
// -- It would allow folk to work around the fragility. e.g.
//
// func Register(driver sql.Driver, d *Dialect) {}
//

// // pack a string, normalising its whitespace.
// func pack(s string) string {
// 	return strings.Join(strings.Fields(s), " ")
// }

// escapeWithDoubleQuotes implements double-quote escaping of a string,
// in accordance with SQL:1999 standard.
func escapeWithDoubleQuotes(s string) string {
	return escape(s, '"', '"')
}

// escapeWithBackticks implements backtick escaping of a string.
func escapeWithBackticks(s string) string {
	return escape(s, '`', '`')
}

// escapeWithBrackets implements bracket escaping of a string.
func escapeWithBrackets(s string) string {
	return escape(s, '[', ']')
}

// escapeWithBraces implements brace escaping of a string.
func escapeWithBraces(s string) string {
	return escape(s, '{', '}')
}

// escape escapes a string identifier.
func escape(s string, escBegin, escEnd byte) string {
	// It would be nice to know when not to escape,
	// but a regex (e.g. "^[a-zA-Z_][a-zA-Z0-9_#@$]*$")
	// doesn't solve this, because it would not catch keywords.
	// Which is why we simply always escape identifiers.

	// TODO(js) Correct handling of backslash escaping of identifiers needs
	// further investigation: different dialects look to handle it differently
	// - removed for now.
	// Please file an issue if you encounter a problem regarding backslash escaping.

	var b strings.Builder
	b.WriteByte(escBegin)
	for i := 0; i < len(s); i++ {
		c := s[i]
		b.WriteByte(c)
		if c == escEnd { // || c == '\\' {
			b.WriteByte(c)
		}
	}
	b.WriteByte(escEnd)
	return b.String()
}
