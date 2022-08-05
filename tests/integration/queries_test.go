package firebolt_integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleRawQuery(t *testing.T) {
	sql := "SELECT 1 AS id, 'name' AS name"
	var id int
	var name string
	err := DB.Raw(sql).Row().Scan(&id, &name)
	if assert.NoError(t, err) {
		assert.Equal(t, id, 1, "Invalid id returned")
		assert.Equal(t, name, "name", "Invalid name returned")
	}
}

type testCreateStatement struct {
	Id      int
	TextVal string
}

func TestCreateStatement(t *testing.T) {

	if !assert.NoError(t, DB.Raw("DROP TABLE IF EXISTS test_create_statement").Row().Err()) {
		// failed to drop table
		return
	}

	create_sql := `
        CREATE FACT TABLE test_create_statement (
            id int, 
            text_val string
        ) PRIMARY INDEX id
    `
	if !assert.NoError(t, DB.Raw(create_sql).Row().Err()) {
		// failed to create table
		return
	}

	obj := testCreateStatement{Id: 3, TextVal: "test_create_statement"}
	DB.Create(obj)
	assert.NoError(t, DB.Error)

	//	DB.Select("id", "text_val").Create(obj)
	//	assert.NoError(t, DB.Error)

	//	rc = DB.Select()
}
