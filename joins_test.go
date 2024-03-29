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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func CheckUser(t *testing.T, gotUser, expectedUser MockUser) {
	assert.Equal(t, gotUser.Name, expectedUser.Name, "name of got and expected users are different")
	assert.Equal(t, gotUser.Age, expectedUser.Age, "name of got and expected users are different")
	//assert.Equal(t, gotUser.Birthday, expectedUser.Birthday, "name of got and expected users are different")
	if gotUser.Company != nil {
		assert.Equal(t, gotUser.Company, expectedUser.Company, "name of got and expected user companies are different")
	}
}

func TestJoins(t *testing.T) {
	user := MockUser{ID: 1, Name: "joins-1", CompanyID: 1, Birthday: time.Now(), Company: &MockCompany{ID: 1, Name: "company"}}

	mockDB.Create(&user)

	var user2 MockUser
	if err := mockDB.Joins("Company").First(&user2, "mock_users.name = ?", user.Name).Error; err != nil {
		t.Fatalf("Failed to load with joins, got error: %v", err)
	}

	CheckUser(t, user2, user)
}

func TestJoinsForSlice(t *testing.T) {
	users := []MockUser{
		MockUser{ID: 6, Name: "slice-joins-1", CompanyID: 2, Company: &MockCompany{ID: 2, Name: "company"}},
		MockUser{ID: 7, Name: "slice-joins-2", CompanyID: 3, Company: &MockCompany{ID: 3, Name: "company"}},
		MockUser{ID: 8, Name: "slice-joins-3", CompanyID: 4, Company: &MockCompany{ID: 4, Name: "company"}},
	}

	mockDB.Create(&users)

	var userIDs []int
	for _, user := range users {
		userIDs = append(userIDs, user.ID)
	}

	var users2 []MockUser
	if err := mockDB.Joins("Company").Find(&users2, "mock_users.id IN ?", userIDs).Error; err != nil {
		t.Fatalf("Failed to load with joins, got error: %v", err)
	} else if len(users2) != len(users) {
		t.Fatalf("Failed to load join users, got: %v, expect: %v", len(users2), len(users))
	}

	sort.Slice(users2, func(i, j int) bool {
		return users2[i].ID > users2[j].ID
	})

	sort.Slice(users, func(i, j int) bool {
		return users[i].ID > users[j].ID
	})

	for idx, user := range users {
		CheckUser(t, user, users2[idx])
	}
}
