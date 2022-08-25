//go:build integration
// +build integration

package firebolt

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

func NameIn1And2(d *gorm.DB) *gorm.DB {
	return d.Where("name in (?)", []string{"ScopeUser1", "ScopeUser2"})
}

func NameIn2And3(d *gorm.DB) *gorm.DB {
	return d.Where("name in (?)", []string{"ScopeUser2", "ScopeUser3"})
}

func NameIn(names []string) func(d *gorm.DB) *gorm.DB {
	return func(d *gorm.DB) *gorm.DB {
		return d.Where("name in (?)", names)
	}
}

func TestScopes(t *testing.T) {
	users := []*MockUser{
		&MockUser{ID: 9, Name: "ScopeUser1"},
		&MockUser{ID: 10, Name: "ScopeUser2"},
		&MockUser{ID: 11, Name: "ScopeUser3"},
	}

	mockDB.Create(&users)

	var users1, users2, users3 []MockUser
	mockDB.Scopes(NameIn1And2).Find(&users1)
	if len(users1) != 2 {
		t.Errorf("Should found two users's name in 1, 2, but got %v", len(users1))
	}

	mockDB.Scopes(NameIn1And2, NameIn2And3).Find(&users2)
	if len(users2) != 1 {
		t.Errorf("Should found one user's name is 2, but got %v", len(users2))
	}

	mockDB.Scopes(NameIn([]string{users[0].Name, users[2].Name})).Find(&users3)
	if len(users3) != 2 {
		t.Errorf("Should found two users's name in 1, 3, but got %v", len(users3))
	}

	mockDB := mockDB.Scopes(func(tx *gorm.DB) *gorm.DB {
		return tx.Table("custom_table")
	}).Session(&gorm.Session{})

	if mockDB.Find(&MockUser{}).Statement.Table != "custom_table" {
		t.Errorf("failed to call Scopes")
	}

	_ = mockDB.Scopes(NameIn1And2, func(tx *gorm.DB) *gorm.DB {
		return tx.Session(&gorm.Session{})
	}).Find(&users1)

	var maxId int64
	userTable := func(db *gorm.DB) *gorm.DB {
		return db.WithContext(context.Background()).Table("users")
	}
	if err := mockDB.Scopes(userTable).Select("max(id)").Scan(&maxId).Error; err != nil {
		t.Errorf("select max(id)")
	}
}
