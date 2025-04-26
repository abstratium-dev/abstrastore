package minio

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	m "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/abstratium-informatique-sarl/abstrastore/pkg/schema"
	min "github.com/abstratium-informatique-sarl/abstrastore/pkg/minio"
)

func TestMain(m *testing.M) {

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
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	assert := assert.New(t)

	repo := getRepo()

	t.Run("Start transaction", func(t *testing.T) {
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
		assert.Equal(tx, transactions[0])

		// end the transaction
		err = repo.Commit(context.Background(), &tx)
		if err != nil {
			t.Fatal(err)
		}

		// ensure that the path no longer exists
		_, err = repo.Client.StatObject(context.Background(), repo.BucketName, tx.GetPath(), m.StatObjectOptions{})
		if err == nil {
			t.Fatal("transaction path should no longer exist")
		}
	})

}

func TestTransactions_StartInsertCommit(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

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
		Name: "John Doe",
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
		
	err = min.NewTypedQuery(repo, context.Background(), &Account{}).
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

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
		Name: "John Doe",
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
		
	err = min.NewTypedQuery(repo, context.Background(), &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(&accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	
	// assert
	assert.Equal(0, len(accountsRead))
}




