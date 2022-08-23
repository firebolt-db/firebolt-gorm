//go:build integration
// +build integration

package firebolt

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestPreparedStmt(t *testing.T) {
	tx := mockDB.Session(&gorm.Session{PrepareStmt: true})

	if _, ok := tx.ConnPool.(*gorm.PreparedStmtDB); !ok {
		t.Fatalf("should assign PreparedStatement Manager back to database when using PrepareStmt mode")
	}

	user := MockUser{ID: 6, Name: "prepared_stmt"}
	mockDB.Create(&user)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	txCtx := tx.WithContext(ctx)

	var result1 MockUser
	if err := txCtx.Find(&result1, user.ID).Error; err != nil {
		t.Fatalf("no error should happen but got %v", err)
	}

	time.Sleep(time.Second)

	var result2 MockUser
	if err := tx.Find(&result2, user.ID).Error; err != nil {
		t.Fatalf("no error should happen but got %v", err)
	}

	user2 := MockUser{ID: 7, Name: "prepared_stmt"}
	if err := txCtx.Create(&user2).Error; err == nil {
		t.Fatalf("should failed to create with timeout context")
	}

	if err := tx.Create(&user2).Error; err != nil {
		t.Fatalf("failed to create, got error %v", err)
	}

	var result3 MockUser
	if err := tx.Find(&result3, user2.ID).Error; err != nil {
		t.Fatalf("no error should happen but got %v", err)
	}
}
