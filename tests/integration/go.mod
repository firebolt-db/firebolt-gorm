module github.com/firebolt-db/firebolt-gorm/tests/integration

go 1.17

require (
	github.com/firebolt-db/firebolt-gorm v0.0.0-20220726093105-8fa13df86673
	github.com/stretchr/testify v1.8.0
	gorm.io/gorm v1.23.8
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/firebolt-db/firebolt-go-sdk v0.0.0-20220728145045-3cb1d41fcdbb // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/firebolt-db/firebolt-gorm => ../../
