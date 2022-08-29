package firebolt

import (
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"testing"
)

func runTestQuoteTo(t *testing.T, input, expected string) {
	var d Dialector
	var w gorm.Statement
	d.QuoteTo(&w, input)
	assert.EqualValues(t, w.SQL.String(), expected)
}

func TestQuoteTo(t *testing.T) {
	runTestQuoteTo(t, "name", "\"name\"")
	runTestQuoteTo(t, "nested.name", "\"nested\".\"name\"")
	runTestQuoteTo(t, "nested.nested.name", "\"nested\".\"nested\".\"name\"")
	runTestQuoteTo(t, "", "\"\"")
}
