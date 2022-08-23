//go:build integration
// +build integration

package firebolt

import (
	"testing"
	"time"
)

func TestGroupBy(t *testing.T) {
	users := []MockUser{{
		Name:     "groupby",
		Age:      10,
		Birthday: time.Now(),
		Active:   true,
	}, {
		Name:     "groupby",
		Age:      20,
		Birthday: time.Now(),
	}, {
		Name:     "groupby",
		Age:      30,
		Birthday: time.Now(),
		Active:   true,
	}, {
		Name:     "groupby1",
		Age:      110,
		Birthday: time.Now(),
	}, {
		Name:     "groupby1",
		Age:      220,
		Birthday: time.Now(),
		Active:   true,
	}, {
		Name:     "groupby1",
		Age:      330,
		Birthday: time.Now(),
		Active:   true,
	}}

	if err := mockDB.Create(&users).Error; err != nil {
		t.Errorf("errors happened when create: %v", err)
	}

	var name string
	var total int
	if err := mockDB.Model(&MockUser{}).Select("name, sum(age)").Where("name = ?", "groupby").Group("name").Row().Scan(&name, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby" || total != 60 {
		t.Errorf("name should be groupby, but got %v, total should be 60, but got %v", name, total)
	}

	if err := mockDB.Model(&MockUser{}).Select("name, sum(age)").Where("name = ?", "groupby").Group("mock_users.name").Row().Scan(&name, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby" || total != 60 {
		t.Errorf("name should be groupby, but got %v, total should be 60, but got %v", name, total)
	}

	if err := mockDB.Model(&MockUser{}).Select("name, sum(age) as total").Where("name LIKE ?", "groupby%").Group("name").Having("name = ?", "groupby1").Row().Scan(&name, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby1" || total != 660 {
		t.Errorf("name should be groupby, but got %v, total should be 660, but got %v", name, total)
	}

	result := struct {
		Name  string
		Total int64
	}{}

	if err := mockDB.Model(&MockUser{}).Select("name, sum(age) as total").Where("name LIKE ?", "groupby%").Group("name").Having("name = ?", "groupby1").Find(&result).Error; err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if result.Name != "groupby1" || result.Total != 660 {
		t.Errorf("name should be groupby, total should be 660, but got %+v", result)
	}

	if err := mockDB.Model(&MockUser{}).Select("name, sum(age) as total").Where("name LIKE ?", "groupby%").Group("name").Having("name = ?", "groupby1").Scan(&result).Error; err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if result.Name != "groupby1" || result.Total != 660 {
		t.Errorf("name should be groupby, total should be 660, but got %+v", result)
	}

	var active bool
	if err := mockDB.Model(&MockUser{}).Select("name, active, sum(age)").Where("name = ? and active = ?", "groupby", true).Group("name").Group("active").Row().Scan(&name, &active, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby" || active != true || total != 40 {
		t.Errorf("group by two columns, name %v, age %v, active: %v", name, total, active)
	}
}
