package firebolt_integration

import (
	"fmt"
	"os"
	"testing"

	firebolt "github.com/firebolt-db/firebolt-gorm"
	"gorm.io/gorm"
)

var DB *gorm.DB

func setup() {
	username := os.Getenv("USER_NAME")
	password := os.Getenv("PASSWORD")
	database := os.Getenv("DATABASE_NAME")
	engine := os.Getenv("ENGINE_NAME")
	dsn := fmt.Sprintf("firebolt://%s:%s@%s/%s", username, password, database, engine)

	var err error
	if DB, err = gorm.Open(firebolt.Open(dsn), &gorm.Config{}); err != nil {
		panic(err)
	}
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}
