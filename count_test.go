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
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func AssertEqualUsers(t *testing.T, gotUsers, expectedUsers []MockUser) {
	if len(gotUsers) != len(expectedUsers) {
		t.Errorf("len of expected and got users are not equal: %d != %d", len(gotUsers), len(expectedUsers))
	} else {
		for i := 0; i < len(gotUsers); i++ {
			if gotUsers[i].Name != expectedUsers[i].Name {
				t.Errorf("name of got and expected users are different '%s' != '%s'", gotUsers[i].Name, expectedUsers[i].Name)
			}
			if gotUsers[i].Age != expectedUsers[i].Age {
				t.Errorf("age of got and expected users are different '%d' != '%d'", gotUsers[i].Age, expectedUsers[i].Age)
			}
			if gotUsers[i].Birthday != expectedUsers[i].Birthday {
				t.Errorf("birthday of got and expected users are different '%s' != '%s'", gotUsers[i].Birthday, expectedUsers[i].Birthday)
			}
		}
	}

}

func TestCount(t *testing.T) {
	var (
		user1                 = MockUser{ID: 1, Name: "count-1", Age: 18, Birthday: time.Now(), Active: true}
		user2                 = MockUser{ID: 2, Name: "count-2", Age: 18, Birthday: time.Now(), Active: true}
		user3                 = MockUser{ID: 3, Name: "count-3", Age: 18, Birthday: time.Now(), Active: true}
		users                 []MockUser
		count, count1, count2 int64
	)

	mockDB.Create([]MockUser{user1, user2, user3})

	if err := mockDB.Where("name = ?", user1.Name).Or("name = ?", user3.Name).Find(&users).Count(&count).Error; err != nil {
		t.Errorf(fmt.Sprintf("Count should work, but got err %v", err))
	}

	if count != int64(len(users)) {
		t.Errorf("Count() method should get correct value, expect: %v, got %v", count, len(users))
	}

	if err := mockDB.Model(&MockUser{}).Where("name = ?", user1.Name).Or("name = ?", user3.Name).Count(&count).Find(&users).Error; err != nil {
		t.Errorf(fmt.Sprintf("Count should work, but got err %v", err))
	}

	if count != int64(len(users)) {
		t.Errorf("Count() method should get correct value, expect: %v, got %v", count, len(users))
	}

	mockDB.Model(&MockUser{}).Where("name = ?", user1.Name).Count(&count1).Or("name in ?", []string{user2.Name, user3.Name}).Count(&count2)
	if count1 != 1 || count2 != 3 {
		t.Errorf("multiple count in chain should works")
	}

	tx := mockDB.Model(&MockUser{}).Where("name = ?", user1.Name).Session(&gorm.Session{})
	tx.Count(&count1)
	tx.Or("name in ?", []string{user2.Name, user3.Name}).Count(&count2)
	if count1 != 1 || count2 != 3 {
		t.Errorf("count after new session should works")
	}

	var count3 int64
	if err := mockDB.Model(&MockUser{}).Where("name in ?", []string{user2.Name, user2.Name, user3.Name}).Group("name").Count(&count3).Error; err != nil {
		t.Errorf("Error happened when count with group, but got %v", err)
	}

	if count3 != 2 {
		t.Errorf("Should get correct count for count with group, but got %v", count3)
	}

	dryDB := mockDB.Session(&gorm.Session{DryRun: true})
	result := dryDB.Table("users").Select("name").Count(&count)
	if !regexp.MustCompile(`SELECT COUNT\(.name.\) FROM .*users.*`).MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build count with select, but got %v", result.Statement.SQL.String())
	}

	result = dryDB.Table("users").Distinct("name").Count(&count)
	if !regexp.MustCompile(`SELECT COUNT\(DISTINCT\(.name.\)\) FROM .*users.*`).MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build count with select, but got %v", result.Statement.SQL.String())
	}

	var count5 int64
	if err := mockDB.Table("mock_users").Where("mock_users.name = ?", user1.Name).Order("name").Count(&count5).Error; err != nil || count5 != 1 {
		t.Errorf("count with join, got error: %v, count %v", err, count)
	}

	var count6 int64
	if err := mockDB.Model(&MockUser{}).Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).Select(
		"(CASE WHEN name=? THEN ? ELSE ? END) as name", "count-1", "main", "other",
	).Count(&count6).Find(&users).Error; err != nil || count6 != 3 {
		t.Fatalf(fmt.Sprintf("Count should work, but got err %v", err))
	}

	expects := []MockUser{{Name: "main"}, {Name: "other"}, {Name: "other"}}
	sort.SliceStable(users, func(i, j int) bool {
		return strings.Compare(users[i].Name, users[j].Name) < 0
	})

	AssertEqualUsers(t, users, expects)

	var count7 int64
	if err := mockDB.Model(&MockUser{}).Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).Select(
		"(CASE WHEN name=? THEN ? ELSE ? END) as name, age", "count-1", "main", "other",
	).Count(&count7).Find(&users).Error; err != nil || count7 != 3 {
		t.Fatalf(fmt.Sprintf("Count should work, but got err %v", err))
	}

	expects = []MockUser{{Name: "main", Age: 18}, {Name: "other", Age: 18}, {Name: "other", Age: 18}}
	sort.SliceStable(users, func(i, j int) bool {
		return strings.Compare(users[i].Name, users[j].Name) < 0
	})

	AssertEqualUsers(t, users, expects)

	var count8 int64
	if err := mockDB.Model(&MockUser{}).Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).Select(
		"(CASE WHEN age=18 THEN 1 ELSE 2 END) as age", "name",
	).Count(&count8).Find(&users).Error; err != nil || count8 != 3 {
		t.Fatalf("Count should work, but got err %v", err)
	}

	expects = []MockUser{{Name: "count-1", Age: 1}, {Name: "count-2", Age: 1}, {Name: "count-3", Age: 1}}
	sort.SliceStable(users, func(i, j int) bool {
		return strings.Compare(users[i].Name, users[j].Name) < 0
	})

	AssertEqualUsers(t, users, expects)

	var count9 int64
	if err := mockDB.Scopes(func(tx *gorm.DB) *gorm.DB {
		return tx.Table("mock_users")
	}).Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).Count(&count9).Find(&users).Error; err != nil || count9 != 3 {
		t.Fatalf("Count should work, but got err %v", err)
	}

	var count10 int64
	if err := mockDB.Model(&MockUser{}).Select("*").Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).Count(&count10).Error; err != nil || count10 != 3 {
		t.Fatalf("Count should be 3, but got count: %v err %v", count10, err)
	}

	var count11 int64
	sameUsers := make([]*MockUser, 0)
	for i := 0; i < 3; i++ {
		sameUsers = append(sameUsers, &MockUser{Name: "count-4", Age: 20, Birthday: time.Now(), Active: true})
	}
	mockDB.Create(sameUsers)

	if err := mockDB.Model(&MockUser{}).Where("name = ?", "count-4").Group("name").Count(&count11).Error; err != nil || count11 != 1 {
		t.Fatalf("Count should be 3, but got count: %v err %v", count11, err)
	}

	var count12 int64
	if err := mockDB.Table("users").
		Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).
		Preload("Toys", func(db *gorm.DB) *gorm.DB {
			return db.Table("toys").Select("name")
		}).Count(&count12).Error; err == nil {
		t.Errorf("error should raise when using preload without schema")
	}

	var count13 int64
	if err := mockDB.Model(MockUser{}).
		Where("name in ?", []string{user1.Name, user2.Name, user3.Name}).
		Preload("Toys", func(db *gorm.DB) *gorm.DB {
			return db.Table("toys").Select("name")
		}).Count(&count13).Error; err != nil {
		t.Errorf("no error should raise when using count with preload, but got %v", err)
	}
}
