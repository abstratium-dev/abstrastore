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
	defer repo.Rollback(context.Background(), &tx1)

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

func TestTransactions_TODO(t *testing.T) {
	assert.Fail(t, "TODO update twice, are updates by any other txs affected by tx1 having not added indices that are to be deleted, to reverse indices?")
	assert.Fail(t, "TODO update with two fields to ensure that one is left intact")
	assert.Fail(t, "TODO update, check tx1 can only see updated version, if using id or index; ensure new tx2 cannot see new version")
	assert.Fail(t, "TODO update")
	assert.Fail(t, "TODO need to return etags of objects that are read, or optionally provide a map which is filled with etag per id?")
	assert.Fail(t, "TODO a test with many versions of an object since they haven't been garbage collected yet, and ensure it returns the one for this transaction, rather than a new version that isn't committed yet")
	assert.Fail(t, "TODO update StaleObjectException")

	assert.Fail(t, "TODO test rollback works when we were unable to write the final transaction file upon insert")
	assert.Fail(t, "TODO test that a second transaction doesn't read the newly updated version of a row")
	assert.Fail(t, "TODO delete")
	assert.Fail(t, "TODO t1 BeginDelete_T2BeginInsert_FailsWithStaleObject or something because the file still exists")
	assert.Fail(t, "TODO t1 BeginDelete_T2Begin_T1Commit_T2InsertCommit should work fine")

	assert.Fail(t, "TODO delete and impact on indices")
	assert.Fail(t, "TODO update and impact on indices")
	assert.Fail(t, "TODO upsert and impact on indices")
	assert.Fail(t, "TODO range scans")

	// range scans
	// Range conditions are comparisons like: >, <, >=, <=, BETWEEN, or a partial match like LIKE 'abc%'

	// full table scans => not supported!
	// ability to add index later, using a migration
}