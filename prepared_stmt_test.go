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
