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
)

func TestRow(t *testing.T) {
	user1 := MockUser{Name: "RowUser1", Age: 1}
	user2 := MockUser{Name: "RowUser2", Age: 10}
	user3 := MockUser{Name: "RowUser3", Age: 20}
	mockDB.Create([]*MockUser{&user1, &user2, &user3})

	row := mockDB.Table("mock_users").Where("name = ?", user2.Name).Select("age").Row()

	var age int64
	if err := row.Scan(&age); err != nil {
		t.Fatalf("Failed to scan age, got %v", err)
	}

	if age != 10 {
		t.Errorf("Scan with Row, age expects: %v, got %v", user2.Age, age)
	}
}

func TestRows(t *testing.T) {
	user1 := MockUser{Name: "RowsUser1", Age: 1}
	user2 := MockUser{Name: "RowsUser2", Age: 10}
	user3 := MockUser{Name: "RowsUser3", Age: 20}
	mockDB.Create([]*MockUser{&user1, &user2, &user3})

	rows, err := mockDB.Table("mock_users").Where("name = ? or name = ?", user2.Name, user3.Name).Select("name, age").Rows()
	if err != nil {
		t.Errorf("Not error should happen, got %v", err)
	}

	count := 0
	for rows.Next() {
		var name string
		var age int64
		rows.Scan(&name, &age)
		count++
	}

	if count != 2 {
		t.Errorf("Should found two records")
	}
}

func TestRaw(t *testing.T) {
	user1 := MockUser{Name: "ExecRawSqlUser1", Age: 1}
	user2 := MockUser{Name: "ExecRawSqlUser2", Age: 10}
	user3 := MockUser{Name: "ExecRawSqlUser3", Age: 20}
	mockDB.Create([]*MockUser{&user1, &user2, &user3})

	type result struct {
		Name  string
		Email string
	}

	var results []result
	mockDB.Raw("SELECT name, age FROM mock_users WHERE name = ? or name = ?", user2.Name, user3.Name).Scan(&results)
	if len(results) != 2 || results[0].Name != user2.Name || results[1].Name != user3.Name {
		t.Errorf("Raw with scan")
	}

	rows, _ := mockDB.Raw("select name, age from mock_users where name = ?", user3.Name).Rows()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("Raw with Rows should find one record with name 3")
	}
}

func TestRowsWithGroup(t *testing.T) {
	users := []MockUser{
		{Name: "having_user_1", Age: 1},
		{Name: "having_user_2", Age: 10},
		{Name: "having_user_1", Age: 20},
		{Name: "having_user_1", Age: 30},
	}

	mockDB.Create(&users)

	rows, err := mockDB.Select("name, count(*) as total").Table("mock_users").Group("name").Having("name IN ?", []string{users[0].Name, users[1].Name}).Rows()
	if err != nil {
		t.Fatalf("got error %v", err)
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		var total int64
		rows.Scan(&name, &total)

		if name == users[0].Name && total != 3 {
			t.Errorf("Should have one user having name %v", users[0].Name)
		} else if name == users[1].Name && total != 1 {
			t.Errorf("Should have two users having name %v", users[1].Name)
		}
	}
}

func TestQueryRaw(t *testing.T) {
	users := []*MockUser{
		&MockUser{ID: 50, Name: "row_query_user"},
		&MockUser{ID: 51, Name: "row_query_user"},
		&MockUser{ID: 52, Name: "row_query_user"},
	}
	mockDB.Create(&users)

	var user MockUser
	mockDB.Raw("select * from mock_users WHERE id = ?", users[1].ID).First(&user)
	CheckUser(t, user, *users[1])
}
