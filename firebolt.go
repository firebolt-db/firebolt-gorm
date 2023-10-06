package firebolt

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
		return "LONG"
	case schema.Float:
		return "DOUBLE"
	case schema.String:
		return "STRING"
	case schema.Time:
		return "TIMESTAMPTZ"
	case schema.Bytes:
		return "BYTEA"
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
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				_, _ = writer.WriteString(".\"")
			}
			_, _ = writer.WriteString(str)
			_ = writer.WriteByte('"')
		}
	} else {
		_, _ = writer.WriteString(str)
		_ = writer.WriteByte('"')
	}
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

const (
	// ClauseValues for clause.ClauseBuilder VALUES key
	ClauseValues  = "VALUES"
	ClauseGroupBy = "GROUP BY"
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
		ClauseGroupBy: func(c clause.Clause, builder clause.Builder) {
			if groupBy, ok := c.Expression.(clause.GroupBy); ok {
				if len(groupBy.Columns) == 1 && strings.ToLower(groupBy.Columns[0].Name) == "all" {
					// If we want to group by all, replace groupBy expression with raw "GROUP BY ALL" sql
					c.Expression = clause.Expr{SQL: "ALL"}
				}
			}
			c.Build(builder)
		},
	}

	return clauseBuilders
}
