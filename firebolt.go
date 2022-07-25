package firebolt

import (
	"database/sql"

	_ "github.com/yuryfirebolt/firebolt-go-sdk"
	"gorm.io/gorm"
	// 	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DSN string
}

type Dialector struct {
	*Config
	Conn gorm.ConnPool	
}

const (
	driverName = "firebolt"
)

var (
	// CreateClauses create clauses
	CreateClauses = []string{"INSERT", "VALUES", "ON CONFLICT"}
	// QueryClauses query clauses
	QueryClauses = []string{}
	// UpdateClauses update clauses
	UpdateClauses = []string{"UPDATE", "SET", "WHERE", "ORDER BY", "LIMIT"}
	// DeleteClauses delete clauses
	DeleteClauses = []string{"DELETE", "FROM", "WHERE", "ORDER BY", "LIMIT"}
)

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "firebolt"
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	// callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
	// 	CreateClauses: CreateClauses,
	// 	QueryClauses:  QueryClauses,
	// 	UpdateClauses: UpdateClauses,
	// 	DeleteClauses: DeleteClauses,
	// })

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		if db.ConnPool, err = sql.Open(driverName, dialector.DSN); err != nil {
			return err
		}
	}

	//	for k, v := range dialector.ClauseBuilders() {
	//		db.ClauseBuilders[k] = v
	//	}
	return
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
 	return Migrator{
 		Migrator: migrator.Migrator{
 			Config: migrator.Config{
 				DB:        db,
 				Dialector: dialector,
 			},
 		},
 		Dialector: dialector,
 	}
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	return "int"
}


func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
 	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
 	writer.WriteString(str)
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}
