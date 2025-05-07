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

func setupTest(tb *testing.T) func(tb *testing.T) {
	log.Println("setup test")

	// Return a function to teardown the test
	return func(tb *testing.T) {
		log.Println("teardown test")
	}
}

func setupAndTeardown() func() {
	start := time.Now()
	log.Println("setup test")

	// Return a function to teardown the test
	return func() {
		log.Println("teardown test ", time.Since(start))
	}
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("!!! START TEST MAIN")
	cleanup()

	m.Run()

	log.Println("!!! END TEST MAIN")
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

func TestTransactions_BeginCommit(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(tx.StartMicroseconds < time.Now().UnixMicro())
	assert.True(tx.TimeoutMicroseconds > time.Now().UnixMicro())
	assert.True(tx.TimeoutMicroseconds > tx.StartMicroseconds)
	assert.Equal(tx.TimeoutMicroseconds, tx.StartMicroseconds+10*time.Second.Microseconds())

	// read the transaction
	var transactions = []schema.Transaction{}
	err = repo.GetTransactionsInProgress(context.Background(), &transactions)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(transactions))
	assert.Equal(tx.Id, transactions[0].Id)
	assert.Equal(tx.StartMicroseconds, transactions[0].StartMicroseconds)
	assert.Equal(tx.TimeoutMicroseconds, transactions[0].TimeoutMicroseconds)

	// commit the transaction
	errs := repo.Commit(context.Background(), &tx)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	assert.Equal(schema.TransactionAlreadyCommittedError, tx.IsOk())

	// ensure that the path no longer exists, i.e. that the commit really removed all files
	_, err = repo.Client.StatObject(context.Background(), repo.BucketName, tx.GetPath(), m.StatObjectOptions{})
	if err == nil {
		t.Fatal("transaction path should no longer exist")
	}

	// commit again => error
	errs = repo.Commit(context.Background(), &tx)
	assert.Equal(1, len(errs))
	assert.Equal(schema.TransactionAlreadyCommittedError, errs[0])

	// rollback => error
	errs = repo.Rollback(context.Background(), &tx)
	assert.Equal(1, len(errs))
	assert.Equal(schema.TransactionAlreadyCommittedError, errs[0])
}

func TestTransactions_BeginRollback(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	
	// read the transaction
	var transactions = []schema.Transaction{}
	err = repo.GetTransactionsInProgress(context.Background(), &transactions)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(transactions))
	assert.Equal(tx.Id, transactions[0].Id)
	assert.Equal(tx.StartMicroseconds, transactions[0].StartMicroseconds)

	// rollback the transaction
	errs := repo.Rollback(context.Background(), &tx)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	assert.Equal(schema.TransactionAlreadyRolledBackError, tx.IsOk())

	// ensure that the path no longer exists, i.e. that the rollback really removed all files
	_, err = repo.Client.StatObject(context.Background(), repo.BucketName, tx.GetPath(), m.StatObjectOptions{})
	if err == nil {
		t.Fatal("transaction path should no longer exist")
	}

	// commit => error
	errs = repo.Commit(context.Background(), &tx)
	assert.Equal(1, len(errs))
	assert.Equal(schema.TransactionAlreadyRolledBackError, errs[0])

	// rollback again => error
	errs = repo.Rollback(context.Background(), &tx)
	assert.Equal(1, len(errs))
	assert.Equal(schema.TransactionAlreadyRolledBackError, errs[0])
}

func TestTransactions_BeginTimeout(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.BeginTransaction(context.Background(), 0*time.Second) // <<<<< timed out immediately
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		errs := repo.Rollback(context.Background(), &tx)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}()

	time.Sleep(1 * time.Millisecond)
	
	assert.True(tx.IsExpired())
	assert.Equal(schema.TransactionTimedOutError, tx.IsOk())

	// commit => error
	errs := repo.Commit(context.Background(), &tx)
	assert.Equal(1, len(errs))
	assert.Equal(schema.TransactionTimedOutError, errs[0])

	// rollback => is ok and is tested above
}

func TestTransactions_BeginInsertCommit(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	errs := repo.Commit(context.Background(), &tx)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// //////////////////////////////////////////////////////////////////////////////
	// check that the account was inserted, find by index
	// //////////////////////////////////////////////////////////////////////////////
	var accountsRead = []*Account{}
	tx = schema.NewTransaction(10*time.Second)
	_, err = min.NewTypedQuery(repo, context.Background(), &tx, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(&accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	
	// assert
	assert.Equal(1, len(accountsRead))
	assert.Equal(account1, accountsRead[0])

	// //////////////////////////////////////////////////////////////////////////////
	// check that the account was inserted, find by id
	// //////////////////////////////////////////////////////////////////////////////
	var accountRead = &Account{}
	tx = schema.NewTransaction(10*time.Second)
	_, err = min.NewTypedQuery(repo, context.Background(), &tx, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	
	// assert
	assert.Equal(account1, accountRead)

}

func TestTransactions_BeginInsertRollback(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx, err := repo.BeginTransaction(context.Background(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	errs := repo.Rollback(context.Background(), &tx)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// //////////////////////////////////////////////////////////////////////////////
	// check cannot find anything that was rolled back, by index
	// //////////////////////////////////////////////////////////////////////////////
	var accountsRead = []*Account{}
	tx = schema.NewTransaction(10*time.Second)
	_, err = min.NewTypedQuery(repo, context.Background(), &tx, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(&accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	
	// assert
	assert.Equal(0, len(accountsRead))

	// //////////////////////////////////////////////////////////////////////////////
	// check cannot find anything that was rolled back, by id
	// //////////////////////////////////////////////////////////////////////////////
	var accountRead = &Account{}
	tx = schema.NewTransaction(10*time.Second)
	_, err = min.NewTypedQuery(repo, context.Background(), &tx, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
		return
	}
	t.Fatal("no error was returned")
}

// because we optimistically lock, and assume that transactions will commit, this test shows how the framework fails fast
// by returning an ObjectLockedError (rather than locking and then failing). It is a signal to the caller that another transaction
// is about to commit the same object. If the transaction were blocked until the first one finished, it would more than likely 
// fail with a StaleObjectError.
func TestTransactions_T1BeginInsert_T2BeginInsert_ObjectLockedError_BecauseT1IsNotFinished(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		errs := repo.Rollback(context.Background(), &tx2)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
		
		errs = repo.Rollback(context.Background(), &tx1)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}()

	// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// in mysql this would block and then upon committing tx1, this would fail.
	// the second transaction would however be ok to continue working with.
	// rather than locking, abstrastore uses optimistic locking. while you might say
	// ah this is like a phantom read, abstrastore assumes that t1 will commit, because that is
	// the aim of all transactions. therefore instead of blocking, it fails fast.
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	_, err = repo.InsertIntoTable(context.Background(), &tx2, T_ACCOUNT, account1)
	if err != nil {
		if !errors.Is(err, min.ObjectLockedError) {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("should fail because object already exists")
}

func TestTransactions_T1BeginInsertCommit_T2BeginInsert_DuplicateKeyError(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		errs := repo.Rollback(context.Background(), &tx2)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}()

	// ///////////////////////////////////////
	// tx1 committed successfully, so inserting the same object again will always fail
	// ///////////////////////////////////////
	_, err = repo.InsertIntoTable(context.Background(), &tx2, T_ACCOUNT, account1)
	if err != nil {
		if !errors.Is(err, min.DuplicateKeyError) {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("should fail because object already exists")
}

func TestTransactions_T1BeginInsert_T2BeginRead_ShouldNotSeeNonCommittedObject_ButT1StillCan(t *testing.T) {
	defer setupAndTeardown()()

	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	defer func() {
		errs := repo.Rollback(context.Background(), &tx1)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}()

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		errs := repo.Rollback(context.Background(), &tx2)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}()

	// ///////////////////////////////////////
	// tx2 must not see the object inserted by tx1
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
		return
	}
	t.Fatal("should have failed to read")

	// ///////////////////////////////////////
	// tx1 can of course still see it!
	// ///////////////////////////////////////
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(account1, accountRead)
}

// similar to previous test but this time the first transaction must not see the object inserted AND COMMITTED by the second transaction
func TestTransactions_T1BeginInsert_T2BeginInsertCommit_T1ShouldNotSeeNonCommittedObjectFromT2(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	defer func() {
		errs := repo.Rollback(context.Background(), &tx1)
		if len(errs) > 0 {
			t.Fatal(errs)
		}
	}()

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 inserts
	// ///////////////////////////////////////
	var account2 = &Account{
		Id:   uuid.New().String(),
		Name: "Jane Doe " + tx2.Id, // helps with concurrent tests
	}
	etag, err = repo.InsertIntoTable(context.Background(), &tx2, T_ACCOUNT, account2)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx2 commits
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// //////////////////////////////////////////////////////////////////////////////
	// tx1 must not see the committed object inserted by tx2, by id
	// //////////////////////////////////////////////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account2.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
		return
	}
	t.Fatal("should have failed to read")

	// //////////////////////////////////////////////////////////////////////////////
	// tx1 must not see the committed object inserted by tx2, by index
	// //////////////////////////////////////////////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account2.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx3 can of course still see it!
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := repo.Commit(context.Background(), &tx3)
		if err != nil {
			t.Fatal(err)
		}
	}()

	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(account2, accountRead)
}

func TestTransactions_T1BeginInsertCommit_T2BeginUpdate_WithWildcardETag(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Updated " + tx2.Id
	wildcard := "*"
	_, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, &wildcard)
	if err != nil {
		assert.Equal("ADB0031 ETag is '*', which is not allowed for update, use insert instead.", err.Error())
	} else {
		t.Fatal("no error")
	}
}

func TestTransactions_T1BeginInsertCommit_T2BeginUpdate_WithEmptyStringETag(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Updated " + tx2.Id
	updatedName := account1.Name
	emptyString := ""
	_, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, &emptyString)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 read - the update should have 
	// worked, even with an empty string etag,
	// as that means "write in all cases"
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(updatedName, accountRead.Name)

	// ///////////////////////////////////////
	// tx2 commit
	// ///////////////////////////////////////
	errs = repo.Commit(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(updatedName, accountRead.Name)
}

func TestTransactions_T2BeginUpdate_WithEmptyStringETag_UpsertLikeInsert_T2Commit(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + uuid.New().String(), // helps with concurrent tests
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// tx2 update with empty etag => upsert, 
	// which in this case is an insert since 
	// the object doesn't exist
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Inserted " + tx2.Id
	updatedName := account1.Name
	emptyString := ""
	_, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, &emptyString)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 read - the insert should have 
	// worked, even with an empty string etag,
	// as that means "write in all cases"
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(updatedName, accountRead.Name)

	// ///////////////////////////////////////
	// tx2 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(updatedName, accountRead.Name)
}

func TestTransactions_T2BeginUpdate_WithEmptyStringETag_UpsertLikeInsert_T2Rollback(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + uuid.New().String(), // helps with concurrent tests
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// tx2 update with empty etag => upsert, 
	// which in this case is an insert since 
	// the object doesn't exist
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Inserted " + tx2.Id
	updatedName := account1.Name
	emptyString := ""
	_, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, &emptyString)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 read - the insert should have 
	// worked, even with an empty string etag,
	// as that means "write in all cases"
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(updatedName, accountRead.Name)

	// ///////////////////////////////////////
	// tx2 rollback
	// ///////////////////////////////////////
	errs := repo.Rollback(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// cannot see object using id
	// ///////////////////////////////////////
	accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
	} else {
		t.Fatal("object should not exist")
	}

	// ///////////////////////////////////////
	// cannot see object using index
	// ///////////////////////////////////////
	var accountsRead []*Account
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(&accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Empty(accountsRead)
}

func TestTransactions_T1BeginInsertCommit_T2BeginUpdateCommit_CheckObjectAndIndicesAreUpdated(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	var oldName = account1.Name
	account1.Name = "Jane Doe Updated " + tx2.Id
	var updatedEtag *string
	updatedEtag, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag)
	assert.NotEqual(etag, updatedEtag)

	// ///////////////////////////////////////
	// tx2 commits
	// ///////////////////////////////////////
	errs = repo.Commit(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// cannot see object using old index entry
	// ///////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// can see object using new index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1, (*accountsRead)[0])

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(account1, accountRead)
}

func TestTransactions_T1BeginInsertCommit_T2BeginUpdateRollback_CheckObjectAndIndicesAreOriginal(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx1)

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	var oldName = account1.Name
	account1.Name = "Jane Doe Updated " + tx2.Id
	var newName = account1.Name
	var updatedEtag *string
	updatedEtag, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag)
	assert.NotEqual(etag, updatedEtag)

	// ///////////////////////////////////////
	// tx2 rollsback
	// ///////////////////////////////////////
	errs = repo.Rollback(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// cannot see object using new index entry
	// ///////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", newName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// can see object using old index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(&Account{Id: account1.Id, Name: oldName}, (*accountsRead)[0])

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(&Account{Id: account1.Id, Name: oldName}, accountRead)
}

func TestTransactions_T1BeginInsertUpdateUpdateCommit_ChecksMultipleUpdates(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// update 1
	// ///////////////////////////////////////
	var oldName = account1.Name
	account1.Name = "Jane Doe Updated " + tx1.Id
	var middleName = account1.Name
	var updatedEtag *string
	updatedEtag, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag)
	assert.NotEqual(etag, updatedEtag)

	// ///////////////////////////////////////
	// update 2
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Updated Final"
	var finalName = account1.Name
	var updatedEtag2 *string
	updatedEtag2, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, updatedEtag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag2)
	assert.NotEqual(updatedEtag, updatedEtag2)

	// ///////////////////////////////////////
	// commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 for reading
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// cannot see object using old index entry
	// ///////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// cannot see object using middle index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", middleName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// can see object using final index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", finalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(&Account{Id: account1.Id, Name: finalName}, (*accountsRead)[0])

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(&Account{Id: account1.Id, Name: finalName}, accountRead)
}

func TestTransactions_T1BeginInsertUpdateUpdateRollback_ChecksMultipleUpdates(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// update 1
	// ///////////////////////////////////////
	var oldName = account1.Name
	account1.Name = "Jane Doe Updated " + tx1.Id
	var middleName = account1.Name
	var updatedEtag *string
	updatedEtag, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag)
	assert.NotEqual(etag, updatedEtag)

	// ///////////////////////////////////////
	// update 2
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Updated again " + tx1.Id
	var finalName = account1.Name
	var updatedEtag2 *string
	updatedEtag2, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, updatedEtag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag2)
	assert.NotEqual(updatedEtag, updatedEtag2)

	// ///////////////////////////////////////
	// rollback
	// ///////////////////////////////////////
	errs := repo.Rollback(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 for reading
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// cannot see object using middle index entry
	// ///////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", middleName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// cannot see object using new index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", finalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// cannot see object using old index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// cannot see object using id
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
		return
	}
	t.Fatal("no error was returned")
}

func TestTransactions_T1BeginInsertCommit_T2UpdateUpdateRollback_ChecksMultipleUpdates(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 for update 1
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// update 1
	// ///////////////////////////////////////
	var oldName = account1.Name
	account1.Name = "Jane Doe Updated " + tx2.Id
	var middleName = account1.Name
	var updatedEtag *string
	updatedEtag, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag)
	assert.NotEqual(etag, updatedEtag)

	// ///////////////////////////////////////
	// update 2
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Updated again " + tx2.Id
	var finalName = account1.Name
	var updatedEtag2 *string
	updatedEtag2, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, updatedEtag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag2)
	assert.NotEqual(updatedEtag, updatedEtag2)

	// ///////////////////////////////////////
	// rollback
	// ///////////////////////////////////////
	errs = repo.Rollback(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// cannot see object using middle index entry
	// ///////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", middleName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// cannot see object using new index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", finalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// can see object using old index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(&Account{Id: account1.Id, Name: oldName}, (*accountsRead)[0])

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(&Account{Id: account1.Id, Name: oldName}, accountRead)
}

func TestTransactions_T1BeginInsertCommit_T2Update_T3UpdateFailsFast_T2Commit_CheckT2IsGood(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 for update
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	var oldName = account1.Name
	account1.Name = "Jane Doe Updated " + tx2.Id
	var updatedName = account1.Name
	var updatedEtag *string
	updatedEtag, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(updatedEtag)
	assert.NotEqual(etag, updatedEtag)

	// ///////////////////////////////////////
	// tx3 begin
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// tx3 read in order to update
	// ///////////////////////////////////////
	var accountReadTx3 = &Account{}
	var etagForUpdatingInTx3 *string
	etagForUpdatingInTx3, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountReadTx3)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// tx3 update that should fail
	// ///////////////////////////////////////
	account1.Name = "Jane Doe Updated illegally By Tx3 " + tx3.Id
	var tx3UpdatedName = account1.Name
	_, err = repo.UpdateTable(context.Background(), &tx3, T_ACCOUNT, account1, etagForUpdatingInTx3)
	if err == nil {
		t.Fatal("expected error")
	}
	assert.True(errors.Is(err, min.StaleObjectError))

	// ///////////////////////////////////////
	// tx2 commit
	// ///////////////////////////////////////
	errs = repo.Commit(context.Background(), &tx2)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx4 for reading
	// ///////////////////////////////////////
	tx4, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx4)

	// ///////////////////////////////////////
	// cannot see object using old index entry
	// ///////////////////////////////////////
	var accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// cannot see object using tx3 index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", tx3UpdatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// can see object using updated index entry from tx2
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(&Account{Id: account1.Id, Name: updatedName}, (*accountsRead)[0])

	// ///////////////////////////////////////
	// can see object using id
	// ///////////////////////////////////////
	accountRead := &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(&Account{Id: account1.Id, Name: updatedName}, accountRead)
}

func TestTransactions_T0Begin_T1BeginInsertCommit_T0CannotSeeObjectFromT1(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// ///////////////////////////////////////
	// tx0 begin
	// ///////////////////////////////////////
	tx0, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx0)

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx0.Id, // helps with concurrent tests
	}

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// t0 cannot see object using id
	// ///////////////////////////////////////
	var accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx0, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
	} else {
		t.Fatal("expected not found")
	}

	// ///////////////////////////////////////
	// t0 cannot see object using index entry
	// ///////////////////////////////////////
	accountsRead := &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx0, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))
}

func TestTransactions_T0Begin_T1BeginInsertUpdate_T0CannotSeeObjectFromT1(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// ///////////////////////////////////////
	// tx0 begin
	// ///////////////////////////////////////
	tx0, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx0)

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx0.Id, // helps with concurrent tests
	}

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 update
	// ///////////////////////////////////////
	account1.Name = "John Doe Updated " + tx1.Id
	etag, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx1)
	if len(errs) != 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// t0 cannot see object using id
	// ///////////////////////////////////////
	accountRead := &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx0, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
	} else {
		t.Fatal("expected not found")
	}

	// ///////////////////////////////////////
	// t0 cannot see object using index entry
	// ///////////////////////////////////////
	accountsRead := &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx0, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))
}

func TestTransactions_T1BeginInsertUpdate_T1CanSeeOwnVersion_T2BeginCannotSeeT1(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx1)

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag *string
	etag, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 update
	// ///////////////////////////////////////
	account1.Name = "John Doe Updated " + tx1.Id
	etag, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, etag)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag)

	// ///////////////////////////////////////
	// tx1 can read own by id
	// ///////////////////////////////////////
	accountRead := &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(account1.Id, accountRead.Id)
	assert.Equal(account1.Name, accountRead.Name)

	// ///////////////////////////////////////
	// tx1 can read own by index
	// ///////////////////////////////////////
	accountsRead := &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(account1.Name, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx2 begin
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// tx2 cannot see object using id
	// ///////////////////////////////////////
	accountRead = &Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
	} else {
		t.Fatal("expected not found")
	}

	// ///////////////////////////////////////
	// tx2 cannot see object using index entry
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))
}

func TestTransactions_T1BeginInsertUpdate_CheckETagsAreCorrectOnReading(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx1)

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag1 *string
	etag1, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag1)

	// ///////////////////////////////////////
	// tx1 update
	// ///////////////////////////////////////
	account1.Name = "John Doe Updated " + tx1.Id
	var etag2 *string
	etag2, err = repo.UpdateTable(context.Background(), &tx1, T_ACCOUNT, account1, etag1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag2)

	// ///////////////////////////////////////
	// tx1 check read etag matches
	// ///////////////////////////////////////
	accountRead := &Account{}
	etagRead, err := min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(account1.Id, accountRead.Id)
	assert.Equal(account1.Name, accountRead.Name)
	assert.Equal(*etag2, *etagRead)

	// ///////////////////////////////////////
	// tx1 check read etag matches
	// ///////////////////////////////////////
	accountsRead := &[]*Account{}
	etagsRead := &map[string]*string{}
	etagsRead, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", account1.Name).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(account1.Name, (*accountsRead)[0].Name)
	etagRead = (*etagsRead)[account1.Id]
	assert.Equal(*etag2, *etagRead)
}

func TestTransactions_T1BeginInsertCommit_T2BeginUpdate_T3BeginRead_CheckETagsAreCorrectOnReading(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}
	oldName := account1.Name

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag1 *string
	etag1, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag1)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	if errs := repo.Commit(context.Background(), &tx1); len(errs) > 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 begin
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx2)

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	account1.Name = "John Doe Updated " + tx2.Id
	var etag2 *string
	etag2, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag2)

	// ///////////////////////////////////////
	// tx3 begin for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// tx3 check read etag matches committed version since tx3 is after tx2 and tx2 isn't committed yet
	// ///////////////////////////////////////
	accountRead := &Account{}
	etagRead, err := min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIdEquals(account1.Id).
		Find(accountRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(account1.Id, accountRead.Id)
	assert.Equal(oldName, accountRead.Name)
	assert.Equal(*etag1, *etagRead)

	// ///////////////////////////////////////
	// tx3 check read etag matches
	// ///////////////////////////////////////
	accountsRead := &[]*Account{}
	etagsRead := &map[string]*string{}
	etagsRead, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", oldName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(oldName, (*accountsRead)[0].Name)
	etagRead = (*etagsRead)[account1.Id]
	assert.Equal(*etag1, *etagRead)
}

// the aim of this test it so check that each TX uses it's own index entry and not the ones of other TXs.
// the point is that at the time of reading, there are multiple index files. yet because we do repeatable
// reads using versions, we ensure that the tx can only see what the rules dictate.
func TestTransactions_T1BeginInsertCommit_T2BeginUpdate_T3BeginSelectByIndex_T2SelectByIndex_T2Update_CheckT3AndT2_T2Commit_CheckT3AndT2(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ACCOUNT := schema.NewTable(DATABASE, "account", []string{"Name"})

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var account1 = &Account{
		Id:   uuid.New().String(),
		Name: "John Doe " + tx1.Id, // helps with concurrent tests
	}
	originalName := account1.Name

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag1 *string
	etag1, err = repo.InsertIntoTable(context.Background(), &tx1, T_ACCOUNT, account1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag1)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	if errs := repo.Commit(context.Background(), &tx1); len(errs) > 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 begin
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	account1.Name = "John Doe Updated " + tx2.Id
	updatedName := account1.Name
	var etag2 *string
	etag2, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag2)

	// ///////////////////////////////////////
	// tx3 begin for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// tx3 read using original name => success
	// ///////////////////////////////////////
	accountsRead := &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", originalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(originalName, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx3 read using new name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx2 select using new name => success
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(updatedName, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx2 select using original name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", originalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	account1.Name = "John Doe Updated Again " + tx2.Id
	updatedNameAgain := account1.Name
	var etag3 *string
	etag3, err = repo.UpdateTable(context.Background(), &tx2, T_ACCOUNT, account1, etag2)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag3)

	// ///////////////////////////////////////
	// tx3 read using original name => success
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", originalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(originalName, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx3 read using updated name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx3 read using second updated name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedNameAgain).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx2 select using second updated name => success
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedNameAgain).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(updatedNameAgain, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx2 select using original name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", originalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx2 select using updated name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx2 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx2)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 read using original name => success, 
	// because still running, so sees snapshot.
	// mysql works like this!
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", originalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(originalName, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx3 read using updated name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx3 read using second updated name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedNameAgain).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx4 begin for reading after commit
	// ///////////////////////////////////////
	tx4, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx4)

	// ///////////////////////////////////////
	// tx4 select using second updated name => success
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedNameAgain).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*accountsRead))
	assert.Equal(account1.Id, (*accountsRead)[0].Id)
	assert.Equal(updatedNameAgain, (*accountsRead)[0].Name)

	// ///////////////////////////////////////
	// tx4 select using original name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", originalName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))

	// ///////////////////////////////////////
	// tx4 select using updated name => fails
	// ///////////////////////////////////////
	accountsRead = &[]*Account{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Account{}).
		SelectFromTable(T_ACCOUNT).
		WhereIndexedFieldEquals("Name", updatedName).
		Find(accountsRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*accountsRead))
}

func TestTransactions_MultipleIndexedFields_T0Begin_T1BeginInsertCommit_T2BeginUpdate_T3BeginSelectByIndex_T2SelectByIndex(t *testing.T) {
	defer setupAndTeardown()()
	assert := assert.New(t)

	repo := getRepo()

	DATABASE := schema.NewDatabase("transactions-tests")
	T_ISSUE := schema.NewTable(DATABASE, "issue", []string{"Title", "Body"}) // both are indexed

	// ///////////////////////////////////////
	// tx0 begin - used for reading
	// ///////////////////////////////////////
	tx0, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx0)

	// ///////////////////////////////////////
	// tx1 begin
	// ///////////////////////////////////////
	tx1, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var issue = &Issue{
		Id:   uuid.New().String(),
		Title: "Title " + tx1.Id, // helps with concurrent tests
		Body: "Body " + tx1.Id,
	}
	originalTitle := issue.Title

	// ///////////////////////////////////////
	// tx1 insert
	// ///////////////////////////////////////
	var etag1 *string
	etag1, err = repo.InsertIntoTable(context.Background(), &tx1, T_ISSUE, issue)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag1)

	// ///////////////////////////////////////
	// ensure tx0 cannot see object by index
	// ///////////////////////////////////////
	issuesRead := &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx0, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", issue.Title).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// ensure tx0 cannot see object by id
	// ///////////////////////////////////////
	var issueRead *Issue
	_, err = min.NewTypedQuery(repo, context.Background(), &tx0, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIdEquals(issue.Id).
		Find(issueRead)
	if err != nil {
		assert.True(errors.Is(err, min.NoSuchKeyError))
	} else {
		t.Fatal("should not have found issue")
	}
	assert.Nil(issueRead)

	// ///////////////////////////////////////
	// tx1 can see object by index title
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", issue.Title).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))

	// ///////////////////////////////////////
	// tx1 can see object by index body
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx1, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(issue.Title, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx1 commit
	// ///////////////////////////////////////
	if errs := repo.Commit(context.Background(), &tx1); len(errs) > 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx2 begin
	// ///////////////////////////////////////
	tx2, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// ///////////////////////////////////////
	// tx2 update
	// ///////////////////////////////////////
	issue.Title = "Title Updated " + tx2.Id
	updatedTitle := issue.Title
	var etag2 *string
	etag2, err = repo.UpdateTable(context.Background(), &tx2, T_ISSUE, issue, etag1)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag2)

	// ///////////////////////////////////////
	// tx3 begin for reading
	// ///////////////////////////////////////
	tx3, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx3)

	// ///////////////////////////////////////
	// tx3 read using original title => success
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", originalTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(originalTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx3 read using new title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx3 read using body, should get original version of title
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(originalTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx2 select using new title => success
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(updatedTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx2 select using original title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", originalTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx2 select using body should give updated title
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(updatedTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx2 update second time
	// ///////////////////////////////////////
	issue.Title = "Title Updated Again " + tx2.Id
	updatedTitleAgain := issue.Title
	var etag3 *string
	etag3, err = repo.UpdateTable(context.Background(), &tx2, T_ISSUE, issue, etag2)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(etag3)

	// ///////////////////////////////////////
	// tx3 read using original title => success
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", originalTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(originalTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx3 read using updated title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx3 read using second updated title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitleAgain).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx3 read using body => success with original title
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(originalTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx2 select using second updated title => success
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitleAgain).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(updatedTitleAgain, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx2 select using original title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", originalTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx2 select using updated title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx2 select using body => success, reads non-committed title
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx2, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(updatedTitleAgain, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx2 commit
	// ///////////////////////////////////////
	errs := repo.Commit(context.Background(), &tx2)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// ///////////////////////////////////////
	// tx3 read using original title => success, 
	// because still running, so sees snapshot.
	// mysql works like this!
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", originalTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(originalTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx3 read using original body => success, 
	// because still running, so sees snapshot.
	// mysql works like this!
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(originalTitle, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx3 read using updated title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx3 read using second updated title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx3, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitleAgain).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx4 begin for reading after commit
	// ///////////////////////////////////////
	tx4, err := repo.BeginTransaction(context.Background(), 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Rollback(context.Background(), &tx4)

	// ///////////////////////////////////////
	// tx4 select using second updated title => success
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitleAgain).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(updatedTitleAgain, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx4 select using body => success
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Body", issue.Body).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(1, len(*issuesRead))
	assert.Equal(issue.Id, (*issuesRead)[0].Id)
	assert.Equal(updatedTitleAgain, (*issuesRead)[0].Title)
	assert.Equal(issue.Body, (*issuesRead)[0].Body)

	// ///////////////////////////////////////
	// tx4 select using original title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", originalTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))

	// ///////////////////////////////////////
	// tx4 select using updated title => fails
	// ///////////////////////////////////////
	issuesRead = &[]*Issue{}
	_, err = min.NewTypedQuery(repo, context.Background(), &tx4, &Issue{}).
		SelectFromTable(T_ISSUE).
		WhereIndexedFieldEquals("Title", updatedTitle).
		Find(issuesRead)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(0, len(*issuesRead))
}

func TestTransactions_TODO(t *testing.T) {

	assert.Fail(t, "TODO insert and update with no indexed fields, including commit / rollback")

	assert.Fail(t, "TODO delete")

	assert.Fail(t, "TODO relationships and reading Issue and Watch based on AccountId")
	assert.Fail(t, "TODO relationships check rollback works with multiple inserts")
// TODO do we need to document that there is no referential integrity? or do we enforce ref int when inserting we 
// could check for existance of foreign keys, if DDL describes what to check for?

	assert.Fail(t, "TODO test rollback works when we were unable to write the final transaction file upon insert")
	assert.Fail(t, "TODO t1 BeginDelete_T2BeginInsert_FailsWithStaleObject or something because the file still exists")
	assert.Fail(t, "TODO t1 BeginDelete_T2Begin_T1Commit_T2InsertCommit should work fine")

	assert.Fail(t, "TODO upsert and impact on indices")

	assert.Fail(t, "TODO range scans")

	// range scans
	// Range conditions are comparisons like: >, <, >=, <=, BETWEEN, or a partial match like LIKE 'abc%'

	// full table scans => not supported!
	// ability to add index later, using a migration
}


type Account struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

// for testing 1..n relationships, one account can create multiple issues
type Issue struct {
	Id   string `json:"id"`
	Title string `json:"title"`
	Body string `json:"body"`

	// ID of the account that created the issue
	CreatedBy string `json:"createdBy"`
}

// for testing n..m relationships, multiple users can watch multiple issues
type Watch struct {
	Id   string `json:"id"`

	// being watched
	IssueId string `json:"issueId"`

	// watching
	AccountId string `json:"accountId"`
}

