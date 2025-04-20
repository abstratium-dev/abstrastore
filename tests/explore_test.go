package minio

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	min "github.com/abstratium-informatique-sarl/abstrastore/pkg/minio"
	"github.com/abstratium-informatique-sarl/abstrastore/pkg/schema"
)

func TestExplore(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	assert := assert.New(t)

	repo := getRepo()
	client := repo.Client

	// table and index definitions

	start := time.Now()
	var DATABASE schema.Database = schema.NewDatabase(fmt.Sprintf("exploration-tests-%d", start.UnixMilli()))

	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// just because we can... could be very useful for cache eviction on other containers
	go func() {
		// Listen for bucket notifications on "mybucket" filtered by prefix, suffix and events.
		for notificationInfo := range client.ListenNotification(context.Background(), "", "", []string{
			"s3:BucketCreated:*",
			"s3:BucketRemoved:*",
			"s3:ObjectCreated:*",
			"s3:ObjectAccessed:*",
			"s3:ObjectRemoved:*",
		}) {
			if notificationInfo.Err != nil {
				log.Fatalln(notificationInfo.Err)
			}
			for _, record := range notificationInfo.Records {
				log.Println("notification: ", record)
			}
		}
	}()

	// data
	var account = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe",
	}

	t.Run("Create Account Including Indices", func(t *testing.T) {

		err := repo.InsertIntoTable(context.Background(), T_ACCOUNT, account, account.Id)
		if err != nil {
			t.Fatal(err)
		}
		
		log.Println("added account at ", T_ACCOUNT.Path(account.Id))
	})

	t.Run("Read By Exact Name using Index", func(t *testing.T) {
		var accountRead = &Account{}
		err := repo.SelectFromTableWhereIndexedFieldEquals(context.Background(), T_ACCOUNT, "Name", account.Name, accountRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(accountRead, account)
	})

	assert.Fail("TODO add table for issues and relationships and make that work")
	assert.Fail("TODO add update with etags")
	assert.Fail("TODO add delete")
	assert.Fail("TODO add select by fuzzy")
	assert.Fail("TODO add full table scan reading based on list objects and an index of a field")
}

func getRepo() *min.MinioRepository {
	os.Setenv("MINIO_URL", "127.0.0.1:9000")
	os.Setenv("MINIO_ACCESS_KEY_ID", "rootuser")
	os.Setenv("MINIO_SECRET_ACCESS_KEY", "rootpass")
	os.Setenv("MINIO_BUCKET_NAME", "abstrastore-tests")
	os.Setenv("MINIO_USE_SSL", "false")
	min.Setup()
	return min.GetRepository()
}

type Account struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type Issue struct {
	Id   string `json:"id"`
	Title string `json:"title"`
	Body string `json:"body"`
}

type IssueAccountRelationship struct {
	IssueId string `json:"issueId"`
	AccountId string `json:"accountId"`
}




