package schema

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
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

// full path to the object with the given id
func (t *Table) Path(id string) string {
	return fmt.Sprintf("%s/%s.json", t.pathPrefix(), id)
}

// return the index object for the given field name
func (t *Table) GetIndex(field string) (*Index, error) {
	for _, index := range t.Indices {
		if index.Field == field {
			return &index, nil
		}
	}
	return nil, fmt.Errorf("no such index: %s", field)
}

func (t *Table) PathFromIndex(databaseTableIdTuple *DatabaseTableIdTuple) (string, error) {
	if databaseTableIdTuple.Database != string(t.Database) || databaseTableIdTuple.Table != t.Name {
		return "", fmt.Errorf("no such database or table, are you using the right table for the given index entry? %s", *databaseTableIdTuple)
	}

	return fmt.Sprintf("%s/%s.json", t.pathPrefix(), databaseTableIdTuple.Id), nil
}

func DatabaseTableIdTupleFromPath(path string) (*DatabaseTableIdTuple, error) {
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		// try just parsing it
	} else {
		path = path[idx+1:]
	}
	// get the last part of the path, i.e. after the last slash
	parts := strings.SplitN(path, "___", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid path since it does not contain three parts: %s", path)
	}
	database, table, id := parts[0], parts[1], parts[2]
	return &DatabaseTableIdTuple{Database: database, Table: table, Id: id}, nil
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

// path to the folder containing all index entries for a given field value
func (i *Index) PathNoId(fieldValue string) string {
	for len(fieldValue) < 2 {
		fieldValue = "_" + fieldValue
	}
	fieldValue = strings.ToLower(fieldValue)
	return fmt.Sprintf("%s/%s/%s", i.pathPrefix(), fieldValue[:2], fieldValue)
}

// path to the index entry, i.e. the path to the actual record.
// the filename is a combination of the database, table, and entity id, separated by "___", so that a caller
// doesn't need to read the contents in order to identify the database, table, and entity id.
func (i *Index) Path(fieldValue string, entityId string) string {
	database_table_id := fmt.Sprintf("%s___%s___%s", i.Table.Database, i.Table.Name, entityId)
	return fmt.Sprintf("%s/%s", i.PathNoId(fieldValue), database_table_id)
}

type DatabaseTableIdTuple struct {
	Database string
	Table    string
	Id       string
}

type Transaction struct {
	Id string `json:"id"`
	Etag string `json:"etag"`
	StartNanoseconds int64 `json:"startNs"`
	Steps []*TransactionStep `json:"steps"`
}

func NewTransaction(timeout time.Duration) Transaction {
	return Transaction{
		Id: uuid.New().String(), 
		Etag: "*",
		StartNanoseconds: time.Now().Add(timeout).UnixNano(),
		Steps: make([]*TransactionStep, 0, 10),
	}
}

func (t *Transaction) IsExpired() bool {
	return time.Now().UnixNano() > t.StartNanoseconds
}

func (t *Transaction) GetPath() string {
	return fmt.Sprintf("transactions/%d___%s", t.StartNanoseconds, t.Id)
}

// information that is required in order to rollback a transaction
type TransactionStep struct {
	Id string `json:"id"` // used to identify the object which needs to be deleted, if we were not able to update the transaction and a rollback were necessary
	Type string `json:"type"`
	Path string `json:"path"`
	InitialETag string `json:"initialEtag"`
	FinalETag string `json:"finalEtag"`
	FinalVersionId string `json:"finalVersionId"`
	ContentType string `json:"contentType"`
	Data []byte `json:"-"`
}
