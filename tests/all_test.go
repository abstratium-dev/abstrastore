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

func TestAll(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	assert := assert.New(t)

	repo := getRepo()

	// table and index definitions

	start := time.Now()
	var DATABASE schema.Database = schema.NewDatabase(fmt.Sprintf("exploration-tests-%d", start.UnixMilli()))

	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// just because we can... could be very useful for cache eviction on other containers
	/* TODO
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
	*/

	// data
	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe",
	}

	t.Run("Create Account Including Indices", func(t *testing.T) {
		tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		
		_, err = repo.InsertIntoTable(context.Background(), &tx, T_ACCOUNT, account1)
		if err != nil {
			t.Fatal(err)
		}
		panic("TODO fix all of the tests in this file, with regards to transactions")
		errs := repo.Commit(context.Background(), &tx)
		if len(errs) != 0 {
			t.Fatal(errs)
		}
		
		log.Println("added account at ", T_ACCOUNT.Path(account1.Id))
	})

	t.Run("Read By Exact Name using Index", func(t *testing.T) {
		var accountsRead = []*Account{}
		
		_, err := min.NewTypedQuery[Account](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_ACCOUNT).
				WhereIndexedFieldEquals("Name", account1.Name).
				Find(&accountsRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(1, len(accountsRead))
		assert.Equal(account1, accountsRead[0])
	})

	// table definition for Issues
	T_ISSUE := schema.NewTable(DATABASE, "issue", []string{"CreatedBy"})

	var issue1 = &Issue{
		Id:   uuid.New().String(),
		Title: "Issue 1",
		Body: "Body 1",
		CreatedBy: account1.Id,
	}

	t.Run("Create Issue", func(t *testing.T) {
		tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		_, err = repo.InsertIntoTable(context.Background(), &tx, T_ISSUE, issue1)
		if err != nil {
			t.Fatal(err)
		}
		
		log.Println("added issue at ", T_ISSUE.Path(issue1.Id))
	})

	t.Run("Read Issue by CreatedBy using Index", func(t *testing.T) {
		var issuesRead = []*Issue{}
		_, err := min.NewTypedQuery[Issue](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_ISSUE).
				WhereIndexedFieldEquals("CreatedBy", account1.Id).
				Find(&issuesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(1, len(issuesRead))
		assert.Equal(issue1, issuesRead[0])
	})

	// table definition for Watches
	T_WATCH := schema.NewTable(DATABASE, "watch", []string{"AccountId", "IssueId"})

	var watch1_account1_issue1 = &Watch{
		Id:   uuid.New().String(),
		IssueId: issue1.Id,
		AccountId: account1.Id,
	}

	t.Run("Create Watch", func(t *testing.T) {
		tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		_, err = repo.InsertIntoTable(context.Background(), &tx, T_WATCH, watch1_account1_issue1)
		if err != nil {
			t.Fatal(err)
		}
		
		log.Println("added watch at ", T_ISSUE.Path(issue1.Id))
	})

	t.Run("Read Who Is Watching Issue By IssueId", func(t *testing.T) {
		var watchesRead = []*Watch{}
		_, err := min.NewTypedQuery[Watch](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_WATCH).
				WhereIndexedFieldEquals("IssueId", issue1.Id).
				Find(&watchesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(1, len(watchesRead))
		assert.Equal(watchesRead[0].AccountId, account1.Id)
		assert.Equal(watchesRead[0].IssueId, issue1.Id)
	})

	t.Run("Read Who Is Watching Issue By AccountId", func(t *testing.T) {
		var watchesRead = []*Watch{}
		_, err := min.NewTypedQuery[Watch](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_WATCH).
				WhereIndexedFieldEquals("AccountId", account1.Id).
				Find(&watchesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(1, len(watchesRead))
		assert.Equal(watchesRead[0].AccountId, account1.Id)
		assert.Equal(watchesRead[0].IssueId, issue1.Id)
	})

	// add a second account, second issue and make new account watch both
	var account2 = &Account{
		Id:   uuid.New().String(),
		Name: "Jane Doe",
	}

	t.Run("Create Second Account", func(t *testing.T) {
		_, err := repo.InsertIntoTable(context.Background(), &schema.Transaction{}, T_ACCOUNT, account2)
		if err != nil {
			t.Fatal(err)
		}
		
		log.Println("added account at ", T_ACCOUNT.Path(account2.Id))
	})

	var issue2 = &Issue{
		Id:   uuid.New().String(),
		Title: "Issue 2",
		Body: "Body 2",
		CreatedBy: account2.Id,
	}

	t.Run("Create Second Issue", func(t *testing.T) {
		_, err := repo.InsertIntoTable(context.Background(), &schema.Transaction{}, T_ISSUE, issue2)
		if err != nil {
			t.Fatal(err)
		}
		
		log.Println("added issue at ", T_ISSUE.Path(issue2.Id))
	})

	var watch2_account2_issue2 = &Watch{
		Id:   uuid.New().String(),
		IssueId: issue2.Id,
		AccountId: account2.Id,
	}

	var watch3_account1_issue2 = &Watch{
		Id:   uuid.New().String(),
		IssueId: issue2.Id,
		AccountId: account1.Id,
	}

	t.Run("Create Second and Third Watch", func(t *testing.T) {
		_, err := repo.InsertIntoTable(context.Background(), &schema.Transaction{}, T_WATCH, watch2_account2_issue2)
		if err != nil {
			t.Fatal(err)
		}
		log.Println("added watch at ", T_WATCH.Path(watch2_account2_issue2.Id))

		_, err = repo.InsertIntoTable(context.Background(), &schema.Transaction{}, T_WATCH, watch3_account1_issue2)
		if err != nil {
			t.Fatal(err)
		}
		log.Println("added watch at ", T_WATCH.Path(watch3_account1_issue2.Id))
	})

	t.Run("Read Who Is Watching Second Issue By IssueId", func(t *testing.T) {
		var watchesRead = []*Watch{}
		_, err := min.NewTypedQuery[Watch](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_WATCH).
				WhereIndexedFieldEquals("IssueId", issue2.Id).
				Find(&watchesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(2, len(watchesRead))
		accounIds := []string{watchesRead[0].AccountId, watchesRead[1].AccountId}
		assert.ElementsMatch(accounIds, []string{account1.Id, account2.Id})
	})

	t.Run("Read Which Issues Are Being Watched By First Account", func(t *testing.T) {
		var watchesRead = []*Watch{}
		_, err := min.NewTypedQuery[Watch](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_WATCH).
				WhereIndexedFieldEquals("AccountId", account1.Id).
				Find(&watchesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(2, len(watchesRead))
		issueIds := []string{watchesRead[0].IssueId, watchesRead[1].IssueId}
		assert.ElementsMatch(issueIds, []string{issue1.Id, issue2.Id})
	})

	t.Run("Read Which Issues Are Being Watched By Second Account", func(t *testing.T) {
		var watchesRead = []*Watch{}
		_, err := min.NewTypedQuery[Watch](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_WATCH).
				WhereIndexedFieldEquals("AccountId", account2.Id).
				Find(&watchesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(1, len(watchesRead))
		assert.Equal(watchesRead[0].IssueId, issue2.Id)
	})

	t.Run("Read Which Issues Are Being Watched By Unknown Account", func(t *testing.T) {
		var watchesRead = []*Watch{}
		_, err := min.NewTypedQuery[Watch](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_WATCH).
				WhereIndexedFieldEquals("AccountId", "unknown").
				Find(&watchesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(0, len(watchesRead))
	})

	t.Run("Read Issue by CreatedBy using Index Now That There Are Several Rows", func(t *testing.T) {
		var issuesRead = []*Issue{}
		_, err := min.NewTypedQuery[Issue](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_ISSUE).
				WhereIndexedFieldEquals("CreatedBy", account2.Id).
				Find(&issuesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(1, len(issuesRead))
		assert.Equal(issue2, issuesRead[0])
	})

	t.Run("Read Issue by Unknown CreatedBy", func(t *testing.T) {
		var issuesRead = []*Issue{}
		_, err := min.NewTypedQuery[Issue](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_ISSUE).
				WhereIndexedFieldEquals("CreatedBy", "unknown").
				Find(&issuesRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(0, len(issuesRead))
	})

	t.Run("Read Account by Id", func(t *testing.T) {
		var accountRead = &Account{}
		_, err := min.NewTypedQuery[Account](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_ACCOUNT).
				WhereIdEquals(account1.Id).
				Find(accountRead)
		if err != nil {
			t.Fatal(err)
		}
		
		// assert
		assert.Equal(account1, accountRead)
	})

	t.Run("Read Account by Unknown Id", func(t *testing.T) {
		var accountRead = &Account{}
		_, err := min.NewTypedQuery[Account](repo, context.Background(), &schema.Transaction{}).
				SelectFromTable(T_ACCOUNT).
				WhereIdEquals("unknown").
				Find(accountRead)
		if err == nil {
			t.Fatal("expected error")
		}
		if duplicateErr, ok := err.(*min.NoSuchKeyErrorWithDetails); ok {
			// assert
			assert.Equal(&min.NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", T_ACCOUNT.Path("unknown"))}, duplicateErr)
		} else {
			t.Fatal("expected NoSuchKeyError")
		}
	})

	t.Run("Insert Duplicate Account", func(t *testing.T) {
		_, err := repo.InsertIntoTable(context.Background(), &schema.Transaction{}, T_ACCOUNT, account1)
		if err == nil {
			t.Fatal("expected error")
		}
		if duplicateErr, ok := err.(*min.DuplicateKeyErrorWithDetails); ok {
			// assert
			assert.Equal(&min.DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("object %s already exists", T_ACCOUNT.Path(account1.Id))}, duplicateErr)
		} else {
			t.Fatal("expected DuplicateKeyError")
		}
	})

	assert.Fail("TODO add update with etags")
	assert.Fail("TODO add delete")
	assert.Fail("TODO add select by fuzzy")
	assert.Fail("TODO tests where there are no results")
	assert.Fail("TODO add full table scan reading based on list objects and an index of a field")
}

type TestCallback struct {
}

func (t *TestCallback) ErrorDuringGc(err error) {
	panic(err)
}

var repo *min.MinioRepository
func getRepo() *min.MinioRepository {
	if repo == nil {
		os.Setenv("MINIO_URL", "127.0.0.1:9000")
		os.Setenv("MINIO_ACCESS_KEY_ID", "rootuser")
		os.Setenv("MINIO_SECRET_ACCESS_KEY", "rootpass")
		os.Setenv("MINIO_BUCKET_NAME", "abstrastore-tests")
		os.Setenv("MINIO_USE_SSL", "false")
		min.Setup(&TestCallback{})
		repo = min.GetRepository()
	}
	return repo
}




