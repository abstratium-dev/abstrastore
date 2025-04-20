package schema

import (
	"fmt"
	"strings"
)

type Database string

func NewDatabase(name string) Database {
	return Database(name)
}

type Table struct {
	Database Database `json:"database"`
	Name string `json:"name"`
	Indices []Index `json:"indices"`
}

func (t *Table) pathPrefix() string {
	return fmt.Sprintf("%s/%s/data", t.Database, t.Name)
}

func (t *Table) Path(id string) string {
	return fmt.Sprintf("%s/%s.json", t.pathPrefix(), id)
}

func (t *Table) GetIndex(field string) (*Index, error) {
	for _, index := range t.Indices {
		if index.Field == field {
			return &index, nil
		}
	}
	return nil, fmt.Errorf("no such index: %s", field)
}

func NewTable(database Database, name string, indices []string) Table {
	t := Table{
		Database: database,
		Name: name,
		Indices: make([]Index, len(indices)),
	}
	for i, index := range indices {
		t.Indices[i] = Index{Table: t, Field: index}
	}
	return t
}

type Index struct {
	Table Table `json:"table"`
	Field string `json:"field"`
}

func (i *Index) pathPrefix() string {
	return fmt.Sprintf("%s/%s/indices/%s", i.Table.Database, i.Table.Name, i.Field)
}

func (i *Index) Path(fieldValue string) string {
	for len(fieldValue) < 2 {
		fieldValue = "_" + fieldValue
	}
	fieldValue = strings.ToLower(fieldValue)
	return fmt.Sprintf("%s/%s/%s", i.pathPrefix(), fieldValue[:2], fieldValue)
}

	