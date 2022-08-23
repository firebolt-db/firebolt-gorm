//go:build integration
// +build integration

package firebolt

import (
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

type MockUser struct {
	gorm.Model
	Name      string `gorm:"primarykey"`
	Age       uint
	Birthday  time.Time
	CompanyID int
	ManagerID uint
	Active    bool
}

type MockModel struct {
	Code string `gorm:"primarykey"`
	Name string
	Id   int
}

var mockDB *gorm.DB

func TestCreateTable(t *testing.T) {

	if err := mockDB.Migrator().CreateTable(&MockModel{}); err != nil {
		t.Errorf("failed to create a table: %v", err)
	}

	if false == mockDB.Migrator().HasTable(&MockModel{}) {
		t.Errorf("Table created but not found")
	}

	if tableList, err := mockDB.Migrator().GetTables(); err != nil {
		t.Errorf("Table get list returned an error: %v", err)
	} else {
		if -1 == slices.IndexFunc(tableList, func(c string) bool { return c == "mock_models" }) {
			t.Errorf("GetTables didn't return a newly created function")
		}
	}

	if err := mockDB.Migrator().DropTable(&MockModel{}); err != nil {
		t.Errorf("Drop table failed with %v", err)
	}
}

func TestCreateTableTwice(t *testing.T) {
	if err := mockDB.Migrator().CreateTable(&MockModel{}); err != nil {
		t.Errorf("failed to create a table: %v", err)
	}
	if err := mockDB.Migrator().CreateTable(&MockModel{}); err == nil {
		t.Errorf("create table second time didn't result into an error")
	}
	if err := mockDB.Migrator().DropTable(&MockModel{}); err != nil {
		t.Errorf("failed to drop a table: %v", err)
	}
}

func TestHasColumn(t *testing.T) {
	if err := mockDB.Migrator().CreateTable(&MockModel{}); err != nil {
		t.Errorf("failed to create a table: %v", err)
	}

	if false == mockDB.Migrator().HasColumn(&MockModel{}, "code") {
		t.Errorf("HasColumn returned false, but code Column should exist")
	}
	if false == mockDB.Migrator().HasColumn(&MockModel{}, "id") {
		t.Errorf("HasColumn returned false, but id Column should exist")
	}
	if true == mockDB.Migrator().HasColumn(&MockModel{}, "not_exists") {
		t.Errorf("HasColumn returned true, but not_exists Column shouldn't exist")
	}

	if err := mockDB.Migrator().DropTable(&MockModel{}); err != nil {
		t.Errorf("Drop table failed with %v", err)
	}
	if true == mockDB.Migrator().HasColumn(&MockModel{}, "id") {
		t.Errorf("HasColumn returned true, but table doesn't exist anymore")
	}
}

func init() {
	username := os.Getenv("USER_NAME")
	password := os.Getenv("PASSWORD")
	database := os.Getenv("DATABASE_NAME")
	engine := os.Getenv("ENGINE_NAME")
	dsn := fmt.Sprintf("firebolt://%s:%s@%s/%s", username, password, database, engine)

	var err error
	if mockDB, err = gorm.Open(Open(dsn), &gorm.Config{}); err != nil {
		panic(err)
	}

	allModels := []interface{}{&MockUser{}}

	if err = mockDB.Migrator().DropTable(allModels...); err != nil {
		panic(fmt.Sprintf("Failed to drop table, got error %v\n", err))
	}

	if err = mockDB.AutoMigrate(allModels...); err != nil {
		panic(fmt.Sprintf("Failed to auto migrate, but got error %v\n", err))
	}

	for _, m := range allModels {
		if !mockDB.Migrator().HasTable(m) {
			panic(fmt.Sprintf("Failed to create table for %#v\n", m))
		}
	}
}
