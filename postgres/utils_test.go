package postgres

import (
	"database/sql"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
	"testing"
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	setup := func() (*sql.DB, func()) {
		const (
			host     = "localhost"
			port     = 5432
			user     = "postgres"
			password = "postgres"
			dbname   = "postgres"
		)

		// Construct the connection string
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("pgx", connStr)
		if err != nil {
			log.Fatalf("sql.Open error %v", err)
		}

		//for _, ddl := range params.CreateDDL {
		//	_, err = db.Exec(ddl)
		//	if err != nil {
		//		// log.Fatalf("db.Exec (create) error %v", err)
		//		log.Printf("db.Exec (create) error %v exec %s", err, ddl)
		//	}
		//}

		doneFn := func() {
			//for _, ddl := range params.DropDDL {
			//	_, err = db.Exec(ddl)
			//	if err != nil {
			//		// log.Fatalf("db.Exec (drop) error %v", err)
			//		log.Printf("db.Exec (drop) error %v exec %s", err, ddl)
			//	}
			//}
			err = db.Close()
			if err != nil {
				log.Printf("db.Close error %v", err)
			}
			//if params.DropFn != nil {
			//	params.DropFn()
			//}
		}

		return db, doneFn

	}

	db, done := setup()
	defer done()
	sc, err := GetViews(db)

	//coverage := Coverage {
	//    neoCoverage: []NeoCoverage {
	//        { Name: "xyz", Number: "xyz123" },
	//        { Name: "abc", Number: "abc123" },
	//    },
	//}

	//var res []DataStr
	//for key, value := range data {
	//	res = append(res, DataStr{
	//		key, value,
	//	})
	//}

	//type Column struct {
	//	name        string
	//	colScanType string
	//	colDbType   string
	//	//colType *sql.ColumnType
	//}
	//
	//type View struct {
	//	Name    [2]string
	//	Columns []Column
	//}

	//var views []View
	//var cols []Column
	//for key, val := range sc {
	//	fmt.Printf("Key: %s, Value: %T\n", key, val)
	//
	//	for k, v := range val {
	//		col := Column{v.Name(), v.ScanType().Name(), v.DatabaseTypeName()}
	//		fmt.Printf("%s\n", PostgresColTypeToSteampipeColType(nil, col).String())
	//		cols = append(cols, col)
	//		fmt.Printf("k: %d, Name: %s, Type: %s, DataBase Type Name: %s\n", k, v.Name(), v.ScanType(), v.DatabaseTypeName())
	//	}
	//	view := View{key, cols}
	//	views = append(views, view)
	//}

	fmt.Print(sc)
	fmt.Print(err)

	//Expect(err).To(BeNil())
	////Expect(sc).To(HaveLen(2))

}
