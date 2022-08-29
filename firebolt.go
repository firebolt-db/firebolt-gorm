package firebolt

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/firebolt-db/firebolt-go-sdk"
	"gorm.io/gorm"

	"gorm.io/gorm/callbacks"
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
	CreateClauses = []string{"INSERT", "VALUES"}
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

	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: CreateClauses,
	})

	if db.ConnPool, err = sql.Open(driverName, dialector.DSN); err != nil {
		return err
	}

	for k, v := range dialector.clauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (dialector Dialector) Apply(config *gorm.Config) error {
	// Firebolt doesn't support transactions
	config.SkipDefaultTransaction = true
	return nil
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:        db,
				Dialector: dialector,
			},
		},
	}
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int, schema.Uint:
		return "INT"
	case schema.Float:
		return "FLOAT"
	case schema.String:
		return "STRING"
	case schema.Time:
		return "DATETIME"
	}
	return fmt.Sprintf("UNKNOWN DATETYPE: %s", field.DataType)
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	_ = writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
    // Quoting table and column names
	_ = writer.WriteByte('"')
	_, _ = writer.WriteString(str)
	_ = writer.WriteByte('"')
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

const (
	// ClauseValues for clause.ClauseBuilder VALUES key
	ClauseValues = "VALUES"
)

func (dialector Dialector) clauseBuilders() map[string]clause.ClauseBuilder {
	clauseBuilders := map[string]clause.ClauseBuilder{
		ClauseValues: func(c clause.Clause, builder clause.Builder) {
			if values, ok := c.Expression.(clause.Values); ok && len(values.Columns) == 0 {
				if st, ok := builder.(*gorm.Statement); ok {
					_ = st.AddError(errors.New("Empty insert statements are not supported by Firebolt"))
				}
				return
			}
			c.Build(builder)
		},
	}

	return clauseBuilders
}
