package firebolt_integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleRawQuery(t *testing.T) {
	sql := "select 1 as id, 'name' as name"
	var id int
	var name string
	err := DB.Raw(sql).Row().Scan(&id, &name)
	if assert.NoError(t, err) {
		assert.Equal(t, id, 1, "Invalid id returned")
		assert.Equal(t, name, "name", "Invalid name returned")
	}
}
