package firebolt

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Migrator struct {
	migrator.Migrator
}

// FullDataTypeOf returns field's db full data type
func (m Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	expr.SQL = m.DataTypeOf(field)

	if !field.NotNull {
		expr.SQL += " NULL"
	}

	if field.Unique {
		expr.SQL += " UNIQUE"
	}

	if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
		if field.DefaultValueInterface != nil {
			defaultStmt := &gorm.Statement{Vars: []interface{}{field.DefaultValueInterface}}
			m.Dialector.BindVarTo(defaultStmt, defaultStmt, field.DefaultValueInterface)
			expr.SQL += " DEFAULT " + m.Dialector.Explain(defaultStmt.SQL.String(), field.DefaultValueInterface)
		} else if field.DefaultValue != "(-)" {
			expr.SQL += " DEFAULT " + field.DefaultValue
		}
	}

	return
}

// Database

func (m Migrator) CurrentDatabase() (name string) {
	return m.DB.Name()
}

// Tables

func (m Migrator) CreateTable(models ...interface{}) error {
	for _, model := range models {

		if err := m.RunWithValue(model, func(stmt *gorm.Statement) (err error) {
			// Build columns
			columnSlice := make([]string, 0, len(stmt.Schema.DBNames))
			for _, dbFieldName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbFieldName]
				columnSlice = append(columnSlice, fmt.Sprintf("\"%s\" %s", dbFieldName, m.FullDataTypeOf(field).SQL))
			}

			// Build primary index
			primaryIndexSlice := make([]string, 0, len(stmt.Schema.PrimaryFieldDBNames))
			primaryIndexSlice = append(primaryIndexSlice, stmt.Schema.PrimaryFieldDBNames...)

			createTableSQL := fmt.Sprintf("CREATE FACT TABLE %s (%s) PRIMARY INDEX %s", stmt.Table, strings.Join(columnSlice, ","), strings.Join(primaryIndexSlice, ","))
			fmt.Printf("%s", createTableSQL)

			return m.DB.Exec(createTableSQL).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int64
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {

		return m.DB.Raw(
			"SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
			stmt.Table).Row().Scan(&count)
	})
	return err == nil && count > 0
}

func (m Migrator) GetTables() (tableList []string, err error) {
	err = m.DB.Raw("SELECT table_name FROM information_schema.tables").Scan(&tableList).Error
	return
}

func (m Migrator) RenameTable(oldName, newName interface{}) error {
	return fmt.Errorf("RenameTable is not supported by firebolt")
}

// Constraints (are not supported by firebolt)

func (m Migrator) CreateConstraint(dst interface{}, name string) error {
	return fmt.Errorf("CreateConstraint is not supported by firebolt")
}

func (m Migrator) DropConstraint(dst interface{}, name string) error {
	return fmt.Errorf("DropConstraint is not supported by firebolt")
}

func (m Migrator) HasConstraint(dst interface{}, name string) bool {
	// Not supported by packdb
	return false
}

// Columns

func (m Migrator) AddColumn(dst interface{}, field string) error {
	return fmt.Errorf("AddColumn is not supported by firebolt")
}

func (m Migrator) DropColumn(dst interface{}, field string) error {
	return fmt.Errorf("DropColumn is not supported by firebolt")
}

func (m Migrator) AlterColumn(dst interface{}, field string) error {
	return fmt.Errorf("AlterColumn is not supported by firebolt")
}

func (m Migrator) MigrateColumn(dst interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	return fmt.Errorf("MigrateColumn is not supported by firebolt")
}

func (m Migrator) HasColumn(dst interface{}, field string) bool {
	var count int64
	err := m.RunWithValue(dst, func(stmt *gorm.Statement) error {
		name := field

		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(field); field != nil {
				name = field.DBName
			}
		}

		return m.DB.Raw(
			"SELECT count(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
			stmt.Table, name,
		).Row().Scan(&count)
	})

	return err == nil && count > 0
}

// Indexes

func (m Migrator) CreateIndex(dst interface{}, name string) error {
	return fmt.Errorf("CreateIndex is not implemented")
}
func (m Migrator) DropIndex(dst interface{}, name string) error {
	return fmt.Errorf("DropIndex is not implemented")
}

func (m Migrator) HasIndex(dst interface{}, name string) bool {
	return false
}

func (m Migrator) RenameIndex(dst interface{}, oldName, newName string) error {
	return fmt.Errorf("RenameIndex is not implemented")
}

func (m Migrator) GetIndexes(dst interface{}) ([]gorm.Index, error) {
	return nil, fmt.Errorf("GetIndexes is not implemented")
}
