package firebolt

import (
	"database/sql"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const indexSql = `
SELECT
	TABLE_NAME,
	COLUMN_NAME,
	INDEX_NAME,
	NON_UNIQUE 
FROM
	information_schema.STATISTICS 
WHERE
	TABLE_SCHEMA = ? 
	AND TABLE_NAME = ? 
ORDER BY
	INDEX_NAME,
	SEQ_IN_INDEX`

type Migrator struct {
	migrator.Migrator
	Dialector
}

func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
	expr := m.Migrator.FullDataTypeOf(field)

	if value, ok := field.TagSettings["COMMENT"]; ok {
		expr.SQL += " COMMENT " + m.Dialector.Explain("?", value)
	}

	return expr
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	return nil
	// return m.RunWithValue(value, func(stmt *gorm.Statement) error {
	// 	if field := stmt.Schema.LookUpField(field); field != nil {
	// 		return m.DB.Exec(
	// 			"ALTER TABLE ? MODIFY COLUMN ? ?",
	// 			clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.FullDataTypeOf(field),
	// 		).Error
	// 	}
	// 	return fmt.Errorf("failed to look up field with name: %s", field)
	// })
}

func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return nil
	// return m.RunWithValue(value, func(stmt *gorm.Statement) error {
	// 	if !m.Dialector.DontSupportRenameColumn {
	// 		return m.Migrator.RenameColumn(value, oldName, newName)
	// 	}

	// 	var field *schema.Field
	// 	if f := stmt.Schema.LookUpField(oldName); f != nil {
	// 		oldName = f.DBName
	// 		field = f
	// 	}

	// 	if f := stmt.Schema.LookUpField(newName); f != nil {
	// 		newName = f.DBName
	// 		field = f
	// 	}

	// 	if field != nil {
	// 		return m.DB.Exec(
	// 			"ALTER TABLE ? CHANGE ? ? ?",
	// 			clause.Table{Name: stmt.Table}, clause.Column{Name: oldName},
	// 			clause.Column{Name: newName}, m.FullDataTypeOf(field),
	// 		).Error
	// 	}

	// 	return fmt.Errorf("failed to look up field with name: %s", newName)
	// })
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return nil
}

func (m Migrator) DropTable(values ...interface{}) error {
	return nil
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return nil
}

// ColumnTypes column types return columnTypes,error
func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	columnTypes := make([]gorm.ColumnType, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var (
			currentDatabase, table = m.CurrentSchema(stmt, stmt.Table)
			columnTypeSQL          = "SELECT column_name, column_default, is_nullable = 'YES', data_type, character_maximum_length, column_type, column_key, extra, column_comment, numeric_precision, numeric_scale "
			rows, err              = m.DB.Session(&gorm.Session{}).Table(table).Limit(1).Rows()
		)

		if err != nil {
			return err
		}

		rawColumnTypes, err := rows.ColumnTypes()

		if err != nil {
			return err
		}

		if err := rows.Close(); err != nil {
			return err
		}

		columnTypeSQL += "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ORDINAL_POSITION"

		columns, rowErr := m.DB.Raw(columnTypeSQL, currentDatabase, table).Rows()
		if rowErr != nil {
			return rowErr
		}

		defer columns.Close()

		for columns.Next() {
			var (
				column            migrator.ColumnType
				datetimePrecision sql.NullInt64
				extraValue        sql.NullString
				columnKey         sql.NullString
				values            = []interface{}{
					&column.NameValue, &column.DefaultValueValue, &column.NullableValue, &column.DataTypeValue, &column.LengthValue, &column.ColumnTypeValue, &columnKey, &extraValue, &column.CommentValue, &column.DecimalSizeValue, &column.ScaleValue,
				}
			)

			if scanErr := columns.Scan(values...); scanErr != nil {
				return scanErr
			}

			column.PrimaryKeyValue = sql.NullBool{Bool: false, Valid: true}
			column.UniqueValue = sql.NullBool{Bool: false, Valid: true}
			switch columnKey.String {
			case "PRI":
				column.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
			case "UNI":
				column.UniqueValue = sql.NullBool{Bool: true, Valid: true}
			}

			if strings.Contains(extraValue.String, "auto_increment") {
				column.AutoIncrementValue = sql.NullBool{Bool: true, Valid: true}
			}

			column.DefaultValueValue.String = strings.Trim(column.DefaultValueValue.String, "'")

			if datetimePrecision.Valid {
				column.DecimalSizeValue = datetimePrecision
			}

			for _, c := range rawColumnTypes {
				if c.Name() == column.NameValue.String {
					column.SQLColumnType = c
					break
				}
			}

			columnTypes = append(columnTypes, column)
		}

		return nil
	})

	return columnTypes, err
}

func (m Migrator) CurrentDatabase() (name string) {
	baseName := m.Migrator.CurrentDatabase()
	m.DB.Raw(
		"SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC,SCHEMA_NAME limit 1",
		baseName+"%", baseName).Scan(&name)
	return
}

func (m Migrator) GetTables() (tableList []string, err error) {
	err = m.DB.Raw("SELECT TABLE_NAME FROM information_schema.tables where TABLE_SCHEMA=?", m.CurrentDatabase()).
		Scan(&tableList).Error
	return
}

func (m Migrator) GetIndexes(value interface{}) ([]gorm.Index, error) {
	indexes := make([]gorm.Index, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {

		result := make([]*Index, 0)
		schema, table := m.CurrentSchema(stmt, stmt.Table)
		scanErr := m.DB.Raw(indexSql, schema, table).Scan(&result).Error
		if scanErr != nil {
			return scanErr
		}
		indexMap := groupByIndexName(result)

		for _, idx := range indexMap {
			tempIdx := &migrator.Index{
				TableName: idx[0].TableName,
				NameValue: idx[0].IndexName,
				PrimaryKeyValue: sql.NullBool{
					Bool:  idx[0].IndexName == "PRIMARY",
					Valid: true,
				},
				UniqueValue: sql.NullBool{
					Bool:  idx[0].NonUnique == 0,
					Valid: true,
				},
			}
			for _, x := range idx {
				tempIdx.ColumnList = append(tempIdx.ColumnList, x.ColumnName)
			}
			indexes = append(indexes, tempIdx)
		}
		return nil
	})
	return indexes, err
}

// Index table index info
type Index struct {
	TableName  string `gorm:"column:TABLE_NAME"`
	ColumnName string `gorm:"column:COLUMN_NAME"`
	IndexName  string `gorm:"column:INDEX_NAME"`
	NonUnique  int32  `gorm:"column:NON_UNIQUE"`
}

func groupByIndexName(indexList []*Index) map[string][]*Index {
	columnIndexMap := make(map[string][]*Index, len(indexList))
	for _, idx := range indexList {
		columnIndexMap[idx.IndexName] = append(columnIndexMap[idx.IndexName], idx)
	}
	return columnIndexMap
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (string, string) {
	if strings.Contains(table, ".") {
		if tables := strings.Split(table, `.`); len(tables) == 2 {
			return tables[0], tables[1]
		}
	}

	return m.CurrentDatabase(), table
}
