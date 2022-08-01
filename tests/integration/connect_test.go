package firebolt_integration

import "testing"

type selectResult struct {
	field int
}

func TestSimpleQuery(t *testing.T) {
	r := DB.Raw("select 1").Row()
	t.Logf("select 1 returned %v", r)
}
