//go:build integration
// +build integration

/*
The MIT License (MIT)

Copyright (c) 2013-NOW  Jinzhu <wosmvp@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
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

	// GROUP BY ALL
	res, err := mockDB.Model(&MockUser{}).Select("name, active, sum(age)").Where("name = ?", "groupby").Group("ALL").Rows()
	if err != nil {
		t.Errorf("no error should happen during group by all, but got %v", err)
	}
	count := 0
	for res.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("group by all should return 4 rows, but got %v", count)
	}
}
