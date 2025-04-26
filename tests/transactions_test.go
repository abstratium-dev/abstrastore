package minio

import (
	"context"
	"log"
	"testing"
	"time"

	min "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/abstratium-informatique-sarl/abstrastore/pkg/schema"
)

func TestTransactions(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	assert := assert.New(t)

	repo := getRepo()

	t.Run("cleanup", func(t *testing.T) {
		err := repo.DeleteFolder(context.Background(), "transactions", true, true)
		if err != nil {
			t.Fatal(err)
		}
	})

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
		err = repo.CommitTransaction(context.Background(), tx)
		if err != nil {
			t.Fatal(err)
		}

		// ensure that the path no longer exists
		_, err = repo.Client.StatObject(context.Background(), repo.BucketName, tx.GetPath(), min.StatObjectOptions{})
		if err == nil {
			t.Fatal("transaction path should no longer exist")
		}
	})

}




