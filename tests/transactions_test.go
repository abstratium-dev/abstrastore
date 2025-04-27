package minio

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	m "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	min "github.com/abstratium-informatique-sarl/abstrastore/pkg/minio"
	"github.com/abstratium-informatique-sarl/abstrastore/pkg/schema"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	cleanup()

	m.Run()

	os.Exit(0)
}

func cleanup() {
	repo := getRepo()
	err := repo.DeleteFolder(context.Background(), "transactions", true, true)
	if err != nil {
		panic(err)
	}
	err = repo.DeleteFolder(context.Background(), "transactions-tests", true, true)
	if err != nil {
		panic(err)
	}
}

func TestTransactions_StartCommit(t *testing.T) {
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.StartTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	
	log.Println("transaction started")

	// read the transaction
	var transactions = []schema.Transaction{}
	err = repo.GetOpenTransactions(context.Background(), &transactions)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(transactions))
	assert.Equal(tx.Id, transactions[0].Id)
	assert.Equal(tx.StartMicroseconds, transactions[0].StartMicroseconds)

	// end the transaction
	err = repo.Commit(context.Background(), &tx)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(schema.TransactionAlreadyCommittedError, tx.IsOk())

	// ensure that the path no longer exists
	_, err = repo.Client.StatObject(context.Background(), repo.BucketName, tx.GetPath(), m.StatObjectOptions{})
	if err == nil {
		t.Fatal("transaction path should no longer exist")
	}
}

func TestTransactions_StartRollback(t *testing.T) {
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.StartTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	
	// read the transaction
	var transactions = []schema.Transaction{}
	err = repo.GetOpenTransactions(context.Background(), &transactions)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(transactions))
	assert.Equal(tx.Id, transactions[0].Id)
	assert.Equal(tx.StartMicroseconds, transactions[0].StartMicroseconds)

	// end the transaction
	errs := repo.Rollback(context.Background(), &tx)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	assert.Equal(schema.TransactionAlreadyRolledBackError, tx.IsOk())

	// ensure that the path no longer exists
	_, err = repo.Client.StatObject(context.Background(), repo.BucketName, tx.GetPath(), m.StatObjectOptions{})
	if err == nil {
		t.Fatal("transaction path should no longer exist")
	}

}

func TestTransactions_StartTimeout(t *testing.T) {
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.StartTransaction(context.Background(), 0*time.Second) // <<<<< timed out immediately
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Millisecond)
	
	assert.True(tx.IsExpired())
	assert.Equal(schema.TransactionTimedOutError, tx.IsOk())
}

func TestTransactions_StartInsertCommit(t *testing.T) {
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.StartTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx.Id, // helps with concurrent tests
	}

	err = repo.InsertIntoTable(context.Background(), &tx, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}

	err = repo.Commit(context.Background(), &tx)
	if err != nil {
		t.Fatal(err)
	}

	var accountsRead = []*Account{}
	tx = schema.NewTransaction(10*time.Second)
	err = min.NewTypedQuery(repo, context.Background(), &tx, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(&accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	
	// assert
	assert.Equal(1, len(accountsRead))
	assert.Equal(account1, accountsRead[0])

}

func TestTransactions_StartInsertRollback(t *testing.T) {
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.StartTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx.Id, // helps with concurrent tests
	}

	err = repo.InsertIntoTable(context.Background(), &tx, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}

	errs := repo.Rollback(context.Background(), &tx)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	var accountsRead = []*Account{}
	tx = schema.NewTransaction(10*time.Second)
	err = min.NewTypedQuery(repo, context.Background(), &tx, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(&accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	
	// assert
	assert.Equal(0, len(accountsRead))
}

func TestTransactions_StartInsertCommit_CheckRepeatableReads(t *testing.T) {

	repo := getRepo()

	tx, err := repo.StartTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx.Id, // helps with concurrent tests
	}

	err = repo.InsertIntoTable(context.Background(), &tx, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}

	// now make another transaction insert the same row
	tx2, err := repo.StartTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		errs := repo.Rollback(context.Background(), &tx2)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
		
			errs = repo.Rollback(context.Background(), &tx)
			if len(errs) > 0 {
				t.Fatal(errs)
			}
	}()

	// in mysql this would block and then upon committing tx1, this would fail.
	// the second transaction would however be ok to continue working with.
	// rather than locking, abstrastore uses optimistic locking. while you might say
	// ah this is like a phantom read, abstrastore assumes that t1 will commit, because that is
	// the aim of all transactions. therefore instead of blocking, it fails fast.
	err = repo.InsertIntoTable(context.Background(), &tx2, T_ACCOUNT, account1)
	if err != nil {
		// check it is of type DuplicateKeyError
		var duplicateKeyError *min.DuplicateKeyError
		if !errors.As(err, &duplicateKeyError) {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("should fail because object already exists")
}

func TestTransactions_TODO(t *testing.T) {
	assert.Fail(t, "TODO test rollback works when we were unable to write the final transaction file upon insert")
	assert.Fail(t, "TODO test that a second transaction doesn't read the newly updated version of a row")
	assert.Fail(t, "TODO delete")
	assert.Fail(t, "TODO delete and impact on indices")
	assert.Fail(t, "TODO update")
	assert.Fail(t, "TODO update and impact on indices")
	assert.Fail(t, "TODO range scans")

	// range scans
	// Range conditions are comparisons like: >, <, >=, <=, BETWEEN, or a partial match like LIKE 'abc%'

	// full table scans => not supported!
	// ability to add index later, using a migration
}