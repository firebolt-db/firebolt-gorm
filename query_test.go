//go:build integration
// +build integration

package firebolt

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestFind(t *testing.T) {
	Users := []MockUser{
		MockUser{ID: 11, Name: "find"},
		MockUser{ID: 12, Name: "find"},
		MockUser{ID: 13, Name: "find"},
	}

	if err := mockDB.Create(&Users).Error; err != nil {
		t.Fatalf("errors happened when create MockUsers: %v", err)
	}

	t.Run("First", func(t *testing.T) {
		var first MockUser
		if err := mockDB.Where("name = ?", "find").First(&first).Error; err != nil {
			t.Errorf("errors happened when query first: %v", err)
		} else {
			CheckUser(t, first, Users[0])
		}
	})

	t.Run("Last", func(t *testing.T) {
		var last MockUser
		if err := mockDB.Where("name = ?", "find").Last(&last).Error; err != nil {
			t.Errorf("errors happened when query last: %v", err)
		} else {
			CheckUser(t, last, Users[2])
		}
	})

	var all []MockUser
	if err := mockDB.Where("name = ?", "find").Find(&all).Error; err != nil || len(all) != 3 {
		t.Errorf("errors happened when query find: %v, length: %v", err, len(all))
	} else {
		for idx, MockUser := range Users {
			t.Run("FindAll#"+strconv.Itoa(idx+1), func(t *testing.T) {
				CheckUser(t, all[idx], MockUser)
			})
		}
	}

	t.Run("FirstMap", func(t *testing.T) {
		first := map[string]interface{}{}
		if err := mockDB.Model(&MockUser{}).Where("name = ?", "find").First(first).Error; err != nil {
			t.Errorf("errors happened when query first: %v", err)
		} else {
			for _, name := range []string{"Name", "Age"} {
				t.Run(name, func(t *testing.T) {
					mockDBName := mockDB.NamingStrategy.ColumnName("", name)

					switch name {
					case "Name":
						if _, ok := first[mockDBName].(string); !ok {
							t.Errorf("invalid data type for %v, got %#v", mockDBName, first[mockDBName])
						}
					case "Age":
						if _, ok := first[mockDBName].(int); !ok {
							t.Errorf("invalid data type for %v, got %#v", mockDBName, first[mockDBName])
						}
					case "Birthday":
						if _, ok := first[mockDBName].(*time.Time); !ok {
							t.Errorf("invalid data type for %v, got %#v", mockDBName, first[mockDBName])
						}
					}

					reflectValue := reflect.Indirect(reflect.ValueOf(Users[0]))
					assert.Equal(t, first[mockDBName], reflectValue.FieldByName(name).Interface())
				})
			}
		}
	})

	t.Run("FirstMapWithTable", func(t *testing.T) {
		first := map[string]interface{}{}
		if err := mockDB.Table("mock_users").Where("name = ?", "find").Find(first).Error; err != nil {
			t.Errorf("errors happened when query first: %v", err)
		} else {
			for _, name := range []string{"Name", "Age"} {
				t.Run(name, func(t *testing.T) {
					mockDBName := mockDB.NamingStrategy.ColumnName("", name)
					resultType := reflect.ValueOf(first[mockDBName]).Type().Name()

					switch name {
					case "Name":
						if !strings.Contains(resultType, "string") {
							t.Errorf("invalid data type for %v, got %v %#v", mockDBName, resultType, first[mockDBName])
						}
					case "Age":
						if !strings.Contains(resultType, "int") {
							t.Errorf("invalid data type for %v, got %v %#v", mockDBName, resultType, first[mockDBName])
						}
					case "Birthday":
						if !strings.Contains(resultType, "Time") && !(mockDB.Dialector.Name() == "sqlite" && strings.Contains(resultType, "string")) {
							t.Errorf("invalid data type for %v, got %v %#v", mockDBName, resultType, first[mockDBName])
						}
					}

					reflectValue := reflect.Indirect(reflect.ValueOf(Users[0]))
					assert.EqualValues(t, first[mockDBName], reflectValue.FieldByName(name).Interface())
				})
			}
		}
	})

	t.Run("FirstPtrMap", func(t *testing.T) {
		first := map[string]interface{}{}
		if err := mockDB.Model(&MockUser{}).Where("name = ?", "find").First(&first).Error; err != nil {
			t.Errorf("errors happened when query first: %v", err)
		} else {
			for _, name := range []string{"Name", "Age"} {
				t.Run(name, func(t *testing.T) {
					mockDBName := mockDB.NamingStrategy.ColumnName("", name)
					reflectValue := reflect.Indirect(reflect.ValueOf(Users[0]))
					assert.Equal(t, first[mockDBName], reflectValue.FieldByName(name).Interface())
				})
			}
		}
	})

	t.Run("FirstSliceOfMap", func(t *testing.T) {
		allMap := []map[string]interface{}{}
		if err := mockDB.Model(&MockUser{}).Where("name = ?", "find").Find(&allMap).Error; err != nil {
			t.Errorf("errors happened when query find: %v", err)
		} else {
			for idx, MockUser := range Users {
				t.Run("FindAllMap#"+strconv.Itoa(idx+1), func(t *testing.T) {
					for _, name := range []string{"Name", "Age"} {
						t.Run(name, func(t *testing.T) {
							mockDBName := mockDB.NamingStrategy.ColumnName("", name)

							switch name {
							case "Name":
								if _, ok := allMap[idx][mockDBName].(string); !ok {
									t.Errorf("invalid data type for %v, got %#v", mockDBName, allMap[idx][mockDBName])
								}
							case "Age":
								if _, ok := allMap[idx][mockDBName].(int); !ok {
									t.Errorf("invalid data type for %v, got %#v", mockDBName, allMap[idx][mockDBName])
								}
							case "Birthday":
								if _, ok := allMap[idx][mockDBName].(*time.Time); !ok {
									t.Errorf("invalid data type for %v, got %#v", mockDBName, allMap[idx][mockDBName])
								}
							}

							reflectValue := reflect.Indirect(reflect.ValueOf(MockUser))
							assert.Equal(t, allMap[idx][mockDBName], reflectValue.FieldByName(name).Interface())
						})
					}
				})
			}
		}
	})

	t.Run("FindSliceOfMapWithTable", func(t *testing.T) {
		allMap := []map[string]interface{}{}
		if err := mockDB.Table("mock_users").Where("name = ?", "find").Find(&allMap).Error; err != nil {
			t.Errorf("errors happened when query find: %v", err)
		} else {
			for idx, MockUser := range Users {
				t.Run("FindAllMap#"+strconv.Itoa(idx+1), func(t *testing.T) {
					for _, name := range []string{"Name", "Age"} {
						t.Run(name, func(t *testing.T) {
							mockDBName := mockDB.NamingStrategy.ColumnName("", name)
							resultType := reflect.ValueOf(allMap[idx][mockDBName]).Type().Name()

							switch name {
							case "Name":
								if !strings.Contains(resultType, "string") {
									t.Errorf("invalid data type for %v, got %v %#v", mockDBName, resultType, allMap[idx][mockDBName])
								}
							case "Age":
								if !strings.Contains(resultType, "int") {
									t.Errorf("invalid data type for %v, got %v %#v", mockDBName, resultType, allMap[idx][mockDBName])
								}
							case "Birthday":
								if !strings.Contains(resultType, "Time") && !(mockDB.Dialector.Name() == "sqlite" && strings.Contains(resultType, "string")) {
									t.Errorf("invalid data type for %v, got %v %#v", mockDBName, resultType, allMap[idx][mockDBName])
								}
							}

							reflectValue := reflect.Indirect(reflect.ValueOf(MockUser))
							assert.EqualValues(t, allMap[idx][mockDBName], reflectValue.FieldByName(name).Interface())
						})
					}
				})
			}
		}
	})

	var models []MockUser
	if err := mockDB.Where("name in (?)", []string{"find"}).Find(&models).Error; err != nil || len(models) != 3 {
		t.Errorf("errors happened when query find with in clause: %v, length: %v", err, len(models))
	} else {
		for idx, MockUser := range Users {
			t.Run("FindWithInClause#"+strconv.Itoa(idx+1), func(t *testing.T) {
				CheckUser(t, models[idx], MockUser)
			})
		}
	}

	var none []MockUser
	if err := mockDB.Where("name in (?)", []string{}).Find(&none).Error; err != nil || len(none) != 0 {
		t.Errorf("errors happened when query find with in clause and zero length parameter: %v, length: %v", err, len(none))
	}
}

func TestNot(t *testing.T) {
	drymockDB := mockDB.Session(&gorm.Session{DryRun: true})

	result := drymockDB.Not(map[string]interface{}{"name": "jinzhu"}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* <> .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("name = ?", "jinzhu1").Not("name = ?", "jinzhu2").Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* = .+ AND NOT.*name.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where(map[string]interface{}{"name": []string{"jinzhu", "jinzhu 2"}}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* IN \\(.+,.+\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not("name = ?", "jinzhu").Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE NOT.*name.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not(map[string]interface{}{"name": []string{}}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* IS NOT NULL").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not(map[string]interface{}{"name": []string{"jinzhu", "jinzhu 2"}}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* NOT IN \\(.+,.+\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not([]int64{}).First(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .mock_users.\\..deleted_at. IS NULL ORDER BY").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not(MockUser{Name: "jinzhu", Age: 18}).First(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*mock_users.*..*name.* <> .+ AND .*mock_users.*..*age.* <> .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}
}

func TestNotWithAllFields(t *testing.T) {
	drymockDB := mockDB.Session(&gorm.Session{DryRun: true, QueryFields: true})
	MockUserQuery := "SELECT .*mock_users.*id.*mock_users.*created_at.*mock_users.*updated_at.*mock_users.*deleted_at.*mock_users.*name" +
		".*mock_users.*age.*mock_users.*birthday.*mock_users.*company_id.*mock_users.*active.* FROM .*mock_users.* "

	result := drymockDB.Not(map[string]interface{}{"mock_users.name": "jinzhu"}).Find(&MockUser{})

	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*name.* <> .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("mock_users.name = ?", "jinzhu1").Not("mock_users.name = ?", "jinzhu2").Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*name.* = .+ AND NOT .*mock_users.*name.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where(map[string]interface{}{"mock_users.name": []string{"jinzhu", "jinzhu 2"}}).Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*name.* IN \\(.+,.+\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not("mock_users.name = ?", "jinzhu").Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE NOT .*mock_users.*name.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not(map[string]interface{}{"mock_users.name": []string{"jinzhu", "jinzhu 2"}}).Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*name.* NOT IN \\(.+,.+\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not([]int64{}).First(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .mock_users.\\..deleted_at. IS NULL ORDER BY").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Not(MockUser{Name: "jinzhu", Age: 18}).First(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*..*name.* <> .+ AND .*mock_users.*..*age.* <> .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build NOT condition, but got %v", result.Statement.SQL.String())
	}
}

func TestOr(t *testing.T) {
	drymockDB := mockDB.Session(&gorm.Session{DryRun: true})

	var count int64
	result := drymockDB.Model(&MockUser{}).Or("role = ?", "admin").Count(&count)
	if !regexp.MustCompile("SELECT count\\(\\*\\) FROM .*mock_users.* WHERE role = .+ AND .*mock_users.*\\..*deleted_at.* IS NULL").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("role = ?", "admin").Where(mockDB.Or("role = ?", "super_admin")).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*role.* = .+ AND .*role.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("role = ?", "admin").Where(mockDB.Or("role = ?", "super_admin").Or("role = ?", "admin")).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*role.* = .+ AND (.*role.* = .+ OR .*role.* = .+)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("role = ?", "admin").Or("role = ?", "super_admin").Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*role.* = .+ OR .*role.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("name = ?", "jinzhu").Or(MockUser{Name: "jinzhu 2", Age: 18}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* = .+ OR \\(.*name.* AND .*age.*\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("name = ?", "jinzhu").Or(map[string]interface{}{"name": "jinzhu 2", "age": 18}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* WHERE .*name.* = .+ OR \\(.*age.* AND .*name.*\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}
}

func TestOrWithAllFields(t *testing.T) {
	drymockDB := mockDB.Session(&gorm.Session{DryRun: true, QueryFields: true})
	MockUserQuery := "SELECT .*mock_users.*id.*mock_users.*created_at.*mock_users.*updated_at.*mock_users.*deleted_at.*mock_users.*name" +
		".*mock_users.*age.*mock_users.*birthday.*mock_users.*company_id.*mock_users.*active.* FROM .*mock_users.* "

	result := drymockDB.Where("role = ?", "admin").Or("role = ?", "super_admin").Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*role.* = .+ OR .*role.* = .+").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("mock_users.name = ?", "jinzhu").Or(MockUser{Name: "jinzhu 2", Age: 18}).Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*name.* = .+ OR \\(.*mock_users.*name.* AND .*mock_users.*age.*\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Where("mock_users.name = ?", "jinzhu").Or(map[string]interface{}{"name": "jinzhu 2", "age": 18}).Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "WHERE .*mock_users.*name.* = .+ OR \\(.*age.* AND .*name.*\\)").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build OR condition, but got %v", result.Statement.SQL.String())
	}
}

func TestPluck(t *testing.T) {
	Users := []*MockUser{
		&MockUser{ID: 22, Name: "pluck-MockUser1"},
		&MockUser{ID: 21, Name: "pluck-MockUser2"},
		&MockUser{ID: 20, Name: "pluck-MockUser3"},
	}

	mockDB.Create(&Users)

	var names []string
	if err := mockDB.Model(MockUser{}).Where("name like ?", "pluck-MockUser%").Order("name").Pluck("name", &names).Error; err != nil {
		t.Errorf("got error when pluck name: %v", err)
	}

	var names2 []string
	if err := mockDB.Model(MockUser{}).Where("name like ?", "pluck-MockUser%").Order("name desc").Pluck("name", &names2).Error; err != nil {
		t.Errorf("got error when pluck name: %v", err)
	}

	sort.Slice(names2, func(i, j int) bool { return names2[i] < names2[j] })
	assert.Equal(t, names, names2)

	var ids []int
	if err := mockDB.Model(MockUser{}).Where("name like ?", "pluck-MockUser%").Pluck("id", &ids).Error; err != nil {
		t.Errorf("got error when pluck id: %v", err)
	}

	for idx, name := range names {
		if name != Users[idx].Name {
			t.Errorf("Unexpected result on pluck name, got %+v", names)
		}
	}

	for idx, id := range ids {
		if int(id) != int(Users[idx].ID) {
			t.Errorf("Unexpected result on pluck id, got %+v", ids)
		}
	}
}

func TestSelect(t *testing.T) {
	User := MockUser{Name: "SelectMockUser1"}
	mockDB.Create(&User)

	var result MockUser
	mockDB.Where("name = ?", User.Name).Select("name").Find(&result)
	if result.ID != 0 {
		t.Errorf("Should not have ID because only selected name, %+v", result.ID)
	}

	if User.Name != result.Name {
		t.Errorf("Should have MockUser Name when selected it")
	}

	var result2 MockUser
	mockDB.Where("name = ?", User.Name).Select("name as name").Find(&result2)
	if result2.ID != 0 {
		t.Errorf("Should not have ID because only selected name, %+v", result2.ID)
	}

	if User.Name != result2.Name {
		t.Errorf("Should have MockUser Name when selected it")
	}

	drymockDB := mockDB.Session(&gorm.Session{DryRun: true})
	r := drymockDB.Select("name", "age").Find(&MockUser{})
	if !regexp.MustCompile("SELECT .*name.*,.*age.* FROM .*mock_users.*").MatchString(r.Statement.SQL.String()) {
		t.Fatalf("Build Select with strings, but got %v", r.Statement.SQL.String())
	}

	r = drymockDB.Select([]string{"name", "age"}).Find(&MockUser{})
	if !regexp.MustCompile("SELECT .*name.*,.*age.* FROM .*mock_users.*").MatchString(r.Statement.SQL.String()) {
		t.Fatalf("Build Select with slice, but got %v", r.Statement.SQL.String())
	}

	// SELECT COALESCE(age,'42') FROM MockUsers;
	r = drymockDB.Table("mock_users").Select("COALESCE(age,?)", 42).Find(&MockUser{})
	if !regexp.MustCompile(`SELECT COALESCE\(age,.*\) FROM .*mock_users.*`).MatchString(r.Statement.SQL.String()) {
		t.Fatalf("Build Select with func, but got %v", r.Statement.SQL.String())
	}

	// named arguments
	r = drymockDB.Table("mock_users").Select("COALESCE(age, @default)", sql.Named("default", 42)).Find(&MockUser{})
	if !regexp.MustCompile(`SELECT COALESCE\(age,.*\) FROM .*mock_users.*`).MatchString(r.Statement.SQL.String()) {
		t.Fatalf("Build Select with func, but got %v", r.Statement.SQL.String())
	}

	if _, err := mockDB.Table("mock_users").Select("COALESCE(age,?)", "42").Rows(); err != nil {
		t.Fatalf("Failed, got error: %v", err)
	}

	r = drymockDB.Select("u.*").Table("mock_users as u").First(&MockUser{}, User.ID)
	if !regexp.MustCompile(`SELECT u\.\* FROM .*mock_users.*`).MatchString(r.Statement.SQL.String()) {
		t.Fatalf("Build Select with u.*, but got %v", r.Statement.SQL.String())
	}

	r = drymockDB.Select("count(*)").Select("u.*").Table("mock_users as u").First(&MockUser{}, User.ID)
	if !regexp.MustCompile(`SELECT u\.\* FROM .*mock_users.*`).MatchString(r.Statement.SQL.String()) {
		t.Fatalf("Build Select with u.*, but got %v", r.Statement.SQL.String())
	}
}

func TestOmit(t *testing.T) {
	User := MockUser{ID: 24, Name: "OmitMockUser1", Age: 20}
	mockDB.Create(&User)

	var result MockUser
	mockDB.Where("name = ?", User.Name).Omit("name").Find(&result)
	if result.ID == 0 {
		t.Errorf("Should not have ID because only selected name, %+v", result.ID)
	}

	if result.Name != "" || result.Age != 20 {
		t.Errorf("MockUser Name should be omitted, got %v, Age should be ok, got %v", result.Name, result.Age)
	}
}

func TestOmitWithAllFields(t *testing.T) {
	User := MockUser{ID: 25, Name: "OmitMockUser1", Age: 20}
	mockDB.Create(&User)

	var MockUserResult MockUser
	mockDB.Session(&gorm.Session{QueryFields: true}).Where("mock_users.name = ?", User.Name).Omit("name").Find(&MockUserResult)
	if MockUserResult.ID == 0 {
		t.Errorf("Should not have ID because only selected name, %+v", MockUserResult.ID)
	}

	if MockUserResult.Name != "" || MockUserResult.Age != 20 {
		t.Errorf("MockUser Name should be omitted, got %v, Age should be ok, got %v", MockUserResult.Name, MockUserResult.Age)
	}

	drymockDB := mockDB.Session(&gorm.Session{DryRun: true, QueryFields: true})
	MockUserQuery := "SELECT .*mock_users.*id.*mock_users.*created_at.*mock_users.*updated_at.*mock_users.*deleted_at.*mock_users.*birthday" +
		".*mock_users.*company_id.*mock_users.*active.* FROM .*mock_users.* "

	result := drymockDB.Omit("name, age").Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery).MatchString(result.Statement.SQL.String()) {
		t.Fatalf("SQL must include table name and selected fields, got %v", result.Statement.SQL.String())
	}
}

func TestPluckWithSelect(t *testing.T) {
	MockUsers := []MockUser{
		{ID: 26, Name: "pluck_with_select_1", Age: 25},
		{ID: 27, Name: "pluck_with_select_2", Age: 26},
	}

	mockDB.Create(&MockUsers)

	var MockUserAges []int
	err := mockDB.Model(&MockUser{}).Where("name like ?", "pluck_with_select%").Select("age + 1 as MockUser_age").Pluck("MockUser_age", &MockUserAges).Error
	if err != nil {
		t.Fatalf("got error when pluck MockUser_age: %v", err)
	}

	sort.Ints(MockUserAges)

	assert.Equal(t, MockUserAges, []int{26, 27})
}

func TestSelectWithVariables(t *testing.T) {
	mockDB.Create(&MockUser{ID: 28, Name: "select_with_variables"})

	if rows, err := mockDB.Table("mock_users").Where("name = ?", "select_with_variables").Select("? as fake", gorm.Expr("name")).Rows(); err != nil {
		t.Fatalf("query returned an error: %v", err)
	} else {
		if !rows.Next() {
			t.Errorf("Should have returned at least one row")
		} else {
			columns, _ := rows.Columns()
			assert.Equal(t, columns, []string{"fake"})
		}

		rows.Close()
	}
}

func TestSelectWithArrayInput(t *testing.T) {
	mockDB.Create(&MockUser{ID: 29, Name: "select_with_array", Age: 42})

	var User MockUser
	mockDB.Select([]string{"name", "age"}).Where("age = 42 AND name = ?", "select_with_array").First(&User)

	if User.Name != "select_with_array" || User.Age != 42 {
		t.Errorf("Should have selected both age and name")
	}
}

func TestSearchWithEmptyChain(t *testing.T) {
	User := MockUser{Name: "search_with_empty_chain", Age: 1}
	mockDB.Create(&User)

	var result MockUser
	if mockDB.Where("").Where("").First(&result).Error != nil {
		t.Errorf("Should not raise any error if searching with empty strings")
	}

	result = MockUser{}
	if mockDB.Where(&MockUser{}).Where("name = ?", User.Name).First(&result).Error != nil {
		t.Errorf("Should not raise any error if searching with empty struct")
	}

	result = MockUser{}
	if mockDB.Where(map[string]interface{}{}).Where("name = ?", User.Name).First(&result).Error != nil {
		t.Errorf("Should not raise any error if searching with empty map")
	}
}

func TestOrder(t *testing.T) {
	drymockDB := mockDB.Session(&gorm.Session{DryRun: true})

	result := drymockDB.Order("").Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* IS NULL$").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build Order condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Order(nil).Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* IS NULL$").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build Order condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Order("age desc, name").Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* ORDER BY age desc, name").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build Order condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Order("age desc").Order("name").Find(&MockUser{})
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* ORDER BY age desc,name").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build Order condition, but got %v", result.Statement.SQL.String())
	}

	stmt := drymockDB.Clauses(clause.OrderBy{
		Expression: clause.Expr{SQL: "FIELD(id,?)", Vars: []interface{}{[]int{1, 2, 3}}, WithoutParentheses: true},
	}).Find(&MockUser{}).Statement

	explainedSQL := drymockDB.Dialector.Explain(stmt.SQL.String(), stmt.Vars...)
	if !regexp.MustCompile("SELECT \\* FROM .*mock_users.* ORDER BY FIELD\\(id,1,2,3\\)").MatchString(explainedSQL) {
		t.Fatalf("Build Order condition, but got %v", explainedSQL)
	}
}

func TestOrderWithAllFields(t *testing.T) {
	drymockDB := mockDB.Session(&gorm.Session{DryRun: true, QueryFields: true})
	MockUserQuery := "SELECT .*mock_users.*id.*mock_users.*created_at.*mock_users.*updated_at.*mock_users.*deleted_at.*mock_users.*name.*mock_users.*age" +
		".*mock_users.*birthday.*mock_users.*company_id.*mock_users.*active.* FROM .*mock_users.* "

	result := drymockDB.Order("mock_users.age desc, mock_users.name").Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "mock_users.age desc, mock_users.name").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build Order condition, but got %v", result.Statement.SQL.String())
	}

	result = drymockDB.Order("mock_users.age desc").Order("mock_users.name").Find(&MockUser{})
	if !regexp.MustCompile(MockUserQuery + "ORDER BY mock_users.age desc,mock_users.name").MatchString(result.Statement.SQL.String()) {
		t.Fatalf("Build Order condition, but got %v", result.Statement.SQL.String())
	}

	stmt := drymockDB.Clauses(clause.OrderBy{
		Expression: clause.Expr{SQL: "FIELD(id,?)", Vars: []interface{}{[]int{1, 2, 3}}, WithoutParentheses: true},
	}).Find(&MockUser{}).Statement

	explainedSQL := drymockDB.Dialector.Explain(stmt.SQL.String(), stmt.Vars...)
	if !regexp.MustCompile(MockUserQuery + "ORDER BY FIELD\\(id,1,2,3\\)").MatchString(explainedSQL) {
		t.Fatalf("Build Order condition, but got %v", explainedSQL)
	}
}

func TestLimit(t *testing.T) {
	MockUsers := []MockUser{
		{ID: 30, Name: "LimitMockUser1", Age: 1},
		{ID: 31, Name: "LimitMockUser2", Age: 10},
		{ID: 32, Name: "LimitMockUser3", Age: 20},
		{ID: 33, Name: "LimitMockUser4", Age: 10},
		{ID: 34, Name: "LimitMockUser5", Age: 20},
		{ID: 35, Name: "LimitMockUser6", Age: 20},
	}

	mockDB.Create(&MockUsers)

	var MockUsers1, MockUsers2, MockUsers3 []MockUser
	mockDB.Order("age desc").Limit(3).Find(&MockUsers1).Limit(5).Find(&MockUsers2).Limit(-1).Find(&MockUsers3)

	if len(MockUsers1) != 3 || len(MockUsers2) != 5 || len(MockUsers3) <= 5 {
		t.Errorf("Limit should works, MockUsers1 %v MockUsers2 %v MockUsers3 %v", len(MockUsers1), len(MockUsers2), len(MockUsers3))
	}
}

func TestOffset(t *testing.T) {
	mockUsers := make([]*MockUser, 0, 20)
	for i := 0; i < 20; i++ {
		mockUsers = append(mockUsers, &MockUser{Name: fmt.Sprintf("OffsetMockUser%v", i)})
	}
	mockDB.Create(&mockUsers)

	var MockUsers1, MockUsers2, MockUsers3, MockUsers4 []MockUser

	mockDB.Limit(100).Where("name like ?", "OffsetMockUser%").Order("age desc").Find(&MockUsers1).Offset(3).Find(&MockUsers2).Offset(5).Find(&MockUsers3).Offset(-1).Find(&MockUsers4)

	if (len(MockUsers1) != len(MockUsers4)) || (len(MockUsers1)-len(MockUsers2) != 3) || (len(MockUsers1)-len(MockUsers3) != 5) {
		t.Errorf("Offset should work")
	}

	mockDB.Where("name like ?", "OffsetMockUser%").Order("age desc").Find(&MockUsers1).Offset(3).Find(&MockUsers2).Offset(5).Find(&MockUsers3).Offset(-1).Find(&MockUsers4)

	if (len(MockUsers1) != len(MockUsers4)) || (len(MockUsers1)-len(MockUsers2) != 3) || (len(MockUsers1)-len(MockUsers3) != 5) {
		t.Errorf("Offset should work without limit.")
	}
}

func TestSearchWithMap(t *testing.T) {
	Users := []MockUser{
		MockUser{ID: 36, Name: "map_search_MockUser1"},
		MockUser{ID: 37, Name: "map_search_MockUser2"},
		MockUser{ID: 38, Name: "map_search_MockUser3"},
		MockUser{ID: 39, Name: "map_search_MockUser4", CompanyID: 1},
	}

	mockDB.Create(&Users)

	var User MockUser
	mockDB.First(&User, map[string]interface{}{"name": Users[0].Name})
	CheckUser(t, User, Users[0])

	User = MockUser{}
	mockDB.Where(map[string]interface{}{"name": Users[1].Name}).First(&User)
	CheckUser(t, User, Users[1])

	var results []MockUser
	mockDB.Where(map[string]interface{}{"name": Users[2].Name}).Find(&results)
	if len(results) != 1 {
		t.Fatalf("Search all records with inline map")
	}

	CheckUser(t, results[0], Users[2])

	var results2 []MockUser
	mockDB.Find(&results2, map[string]interface{}{"name": Users[3].Name, "company_id": 0})
	if len(results2) != 0 {
		t.Errorf("Search all records with inline map containing null value finding 0 records")
	}

	mockDB.Find(&results2, map[string]interface{}{"name": Users[0].Name, "company_id": 0})
	if len(results2) != 1 {
		t.Errorf("Search all records with inline map containing null value finding 1 record")
	}

	mockDB.Find(&results2, map[string]interface{}{"name": Users[3].Name, "company_id": Users[3].CompanyID})
	if len(results2) != 1 {
		t.Errorf("Search all records with inline multiple value map")
	}
}

func TestSearchWithStruct(t *testing.T) {
	dryRunmockDB := mockDB.Session(&gorm.Session{DryRun: true})

	result := dryRunmockDB.Where(MockUser{Name: "jinzhu"}).Find(&MockUser{})
	if !regexp.MustCompile(`WHERE .mock_users.\..name. = .{1,3} AND .mock_users.\..deleted_at. IS NULL`).MatchString(result.Statement.SQL.String()) {
		t.Errorf("invalid query SQL, got %v", result.Statement.SQL.String())
	}

	result = dryRunmockDB.Where(MockUser{Name: "jinzhu", Age: 18}).Find(&MockUser{})
	if !regexp.MustCompile(`WHERE .mock_users.\..name. = .{1,3} AND .mock_users.\..age. = .{1,3} AND .mock_users.\..deleted_at. IS NULL`).MatchString(result.Statement.SQL.String()) {
		t.Errorf("invalid query SQL, got %v", result.Statement.SQL.String())
	}

	result = dryRunmockDB.Where(MockUser{Name: "jinzhu"}, "name", "Age").Find(&MockUser{})
	if !regexp.MustCompile(`WHERE .mock_users.\..name. = .{1,3} AND .mock_users.\..age. = .{1,3} AND .mock_users.\..deleted_at. IS NULL`).MatchString(result.Statement.SQL.String()) {
		t.Errorf("invalid query SQL, got %v", result.Statement.SQL.String())
	}

	result = dryRunmockDB.Where(MockUser{Name: "jinzhu"}, "age").Find(&MockUser{})
	if !regexp.MustCompile(`WHERE .mock_users.\..age. = .{1,3} AND .mock_users.\..deleted_at. IS NULL`).MatchString(result.Statement.SQL.String()) {
		t.Errorf("invalid query SQL, got %v", result.Statement.SQL.String())
	}
}

func TestSubQuery(t *testing.T) {
	MockUsers := []MockUser{
		{ID: 40, Name: "subquery_1", Age: 10},
		{ID: 41, Name: "subquery_2", Age: 20},
		{ID: 42, Name: "subquery_3", Age: 30},
		{ID: 43, Name: "subquery_4", Age: 40},
	}

	mockDB.Create(&MockUsers)

	if err := mockDB.Select("*").Where("name IN (?)", mockDB.Select("name").Table("mock_users").Where("name LIKE ?", "subquery_%")).Find(&MockUsers).Error; err != nil {
		t.Fatalf("got error: %v", err)
	}

	if len(MockUsers) != 4 {
		t.Errorf("Four MockUsers should be found, instead found %d", len(MockUsers))
	}

	mockDB.Select("*").Where("name LIKE ?", "subquery%").Where("age >= (?)", mockDB.
		Select("AVG(age)").Table("mock_users").Where("name LIKE ?", "subquery%")).Find(&MockUsers)

	if len(MockUsers) != 2 {
		t.Errorf("Two MockUsers should be found, instead found %d", len(MockUsers))
	}
}

func TestSubQueryWithRaw(t *testing.T) {
	MockUsers := []MockUser{
		{ID: 44, Name: "subquery_raw_1", Age: 10},
		{ID: 45, Name: "subquery_raw_2", Age: 20},
		{ID: 46, Name: "subquery_raw_3", Age: 30},
		{ID: 47, Name: "subquery_raw_4", Age: 40},
	}
	mockDB.Create(&MockUsers)

	var count int64
	err := mockDB.Raw("select count(*) from (?) tmp where 1 = ? AND name IN (?)", mockDB.Raw("select name from mock_users where age >= ? and name in (?)", 10, []string{"subquery_raw_1", "subquery_raw_2", "subquery_raw_3"}), 1, mockDB.Raw("select name from mock_users where age >= ? and name in (?)", 20, []string{"subquery_raw_1", "subquery_raw_2", "subquery_raw_3"})).Scan(&count).Error
	if err != nil {
		t.Errorf("Expected to get no errors, but got %v", err)
	}

	if count != 2 {
		t.Errorf("Row count must be 2, instead got %d", count)
	}

	err = mockDB.Raw("select count(*) from (?) tmp",
		mockDB.Table("mock_users").
			Select("name").
			Where("age >= ? and name in (?)", 20, []string{"subquery_raw_1", "subquery_raw_3"}).
			Group("name"),
	).Count(&count).Error

	if err != nil {
		t.Errorf("Expected to get no errors, but got %v", err)
	}

	if count != 1 {
		t.Errorf("Row count must be 1, instead got %d", count)
	}

	err = mockDB.Raw("select count(*) from (?) tmp",
		mockDB.Table("mock_users").
			Select("name").
			Where("name LIKE ?", "subquery_raw%").
			Not("age <= ?", 10).Not("(name IN (?))", []string{"subquery_raw_1", "subquery_raw_3"}).
			Group("name"),
	).Count(&count).Error

	if err != nil {
		t.Errorf("Expected to get no errors, but got %v", err)
	}

	if count != 2 {
		t.Errorf("Row count must be 2, instead got %d", count)
	}
}

func TestSubQueryWithHaving(t *testing.T) {
	MockUsers := []MockUser{
		{ID: 48, Name: "subquery_having_1", Age: 10},
		{ID: 49, Name: "subquery_having_2", Age: 20},
		{ID: 50, Name: "subquery_having_3", Age: 30},
		{ID: 51, Name: "subquery_having_4", Age: 40},
	}
	mockDB.Create(&MockUsers)

	var results []MockUser
	mockDB.Select("AVG(age) as avgage").Where("name LIKE ?", "subquery_having%").Group("name").Having("AVG(age) > (?)", mockDB.
		Select("AVG(age)").Where("name LIKE ?", "subquery_having%").Table("mock_users")).Find(&results)

	if len(results) != 2 {
		t.Errorf("Two MockUser group should be found, instead found %d", len(results))
	}
}

func TestQueryWithTableAndConditions(t *testing.T) {
	result := mockDB.Session(&gorm.Session{DryRun: true}).Table("mock_users").Find(&MockUser{}, MockUser{Name: "jinzhu"})

	if !regexp.MustCompile(`SELECT \* FROM .mock_users. WHERE .mock_users.\..name. = .+ AND .mock_users.\..deleted_at. IS NULL`).MatchString(result.Statement.SQL.String()) {
		t.Errorf("invalid query SQL, got %v", result.Statement.SQL.String())
	}
}

func TestQueryWithTableAndConditionsAndAllFields(t *testing.T) {
	result := mockDB.Session(&gorm.Session{DryRun: true, QueryFields: true}).Table("mock_users").Find(&MockUser{}, MockUser{Name: "jinzhu"})
	MockUserQuery := "SELECT .*mock_users.*id.*mock_users.*created_at.*mock_users.*updated_at.*mock_users.*deleted_at.*mock_users.*name.*mock_users.*age" +
		".*mock_users.*birthday.*mock_users.*company_id.*mock_users.*active.* FROM .mock_users. "

	if !regexp.MustCompile(MockUserQuery + `WHERE .mock_users.\..name. = .+ AND .mock_users.\..deleted_at. IS NULL`).MatchString(result.Statement.SQL.String()) {
		t.Errorf("invalid query SQL, got %v", result.Statement.SQL.String())
	}
}
