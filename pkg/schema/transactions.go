package schema

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const TX_ID = "Txid" // wow, minio doesn't support camel case. the transaction id that wrote this version. used to check if the version needs to be ignored, if the transaction is still in progress.
const LAST_MODIFIED = "Lastmodified" // wow, minio doesn't support camel case
const TIMESTAMP_ID_SEPARATOR = "___"
const TRANSACTIONS_ROOT = "transactions/"

type Transaction struct {
	Id string `json:"id"`
	Etag string `json:"etag"`
	StartMicroseconds int64 `json:"startMicros"`
	TimeoutMicroseconds int64 `json:"timeoutMicros"`
	Steps []*TransactionStep `json:"steps"`
	
	// key is path to object; allows the transaction to avoid reading things that it wrote or already read (enabling repeatable reads)
	Cache map[string]*any `json:"-"`

	// InProgress, Committed, RolledBack
	State string `json:"state"`
}

func NewTransaction(timeout time.Duration) Transaction {
	now := time.Now()
	return Transaction{
		Id: uuid.New().String(), 
		Etag: "*",
		StartMicroseconds: now.UnixMicro(),
		TimeoutMicroseconds: now.Add(timeout).UnixMicro(),
		Steps: make([]*TransactionStep, 0, 10),
		Cache: make(map[string]*any),
		State: "InProgress",
	}
}

func (t *Transaction) IsExpired() bool {
	return time.Now().UnixMicro() > t.TimeoutMicroseconds
}

var TransactionAlreadyCommittedError = fmt.Errorf("Transaction is already committed")
var TransactionAlreadyRolledBackError = fmt.Errorf("Transaction is already rolled back")
var TransactionTimedOutError = fmt.Errorf("Transaction has timed out")

func (t *Transaction) IsOk() error {
	if t.State == "Committed" {
		return TransactionAlreadyCommittedError
	} else if t.State == "RolledBack" {
		return TransactionAlreadyRolledBackError
	} else if t.State != "InProgress" {
		panic("Transaction is in an unknown state")
	}

	if t.IsExpired() {
		return TransactionTimedOutError
	}

	return nil
}

func (t *Transaction) GetPath() string {
	return fmt.Sprintf("%s%d%s%s", TRANSACTIONS_ROOT, t.StartMicroseconds, TIMESTAMP_ID_SEPARATOR, t.Id)
}

func (t *Transaction) GetIdAndTimeoutMicrosFromPath(path string) (string, uint64) {
	filename := path[len(TRANSACTIONS_ROOT):]
	// trim any leading or trailing / characters
	filename = strings.Trim(filename, "/")
	split := strings.Split(filename, TIMESTAMP_ID_SEPARATOR)
	if len(split) != 2 {
		panic("Invalid transaction path " + path)
	}
	id := split[1]
	timeoutAsString := split[0]
	timeout, err := strconv.ParseUint(timeoutAsString, 10, 64)
	if err != nil {
		panic(err)
	}
	return id, timeout
}

func (t *Transaction) GetRootPath() string {
	return TRANSACTIONS_ROOT
}

// Param: Type - the type of the step
// Param: ContentType - the content type of the object
// Param: Path - the path of the object
// Param: InitialETag - the initial ETag of the object, if "" then none is set and a change will always be successful
// Param: Entity - the object itself
// Returns: an error if the transaction is not InProgress or has timed out
func (t *Transaction) AddStep(Type string, ContentType string, Path string, InitialETag string, Entity any) error {
	if err := t.IsOk(); err != nil {
		return err
	}

	userMetadata := map[string]string{
		// don't add amz prefix here, since minio does it automatically
		TX_ID: t.Id,
		LAST_MODIFIED: fmt.Sprintf("%d", time.Now().UnixMicro()),
	}

	data := []byte{}
	if Entity != nil {
		var err error
		data, err = json.Marshal(Entity)
		if err != nil {
			return err
		}
	}

	step := TransactionStep{
		Type: Type,
		ContentType: ContentType,
		Path: Path,
		InitialETag: InitialETag,
		InitialVersionId: "",
		UserMetadata: userMetadata,
		Data: &data,
		Entity: Entity,
		Executed: false,
	}
	t.Steps = append(t.Steps, &step)

	if(Type == "insert-data") {
		t.Cache[Path] = &Entity
	} else if (Type == "insert-index") {
		t.Cache[Path] = &Entity
	}

	return nil
}

// information that is required in order to rollback a transaction
type TransactionStep struct {
	Type string `json:"type"`
	ContentType string `json:"contentType"`
	Path string `json:"path"`
	InitialETag string `json:"initialEtag"`
	// used for deletion
	InitialVersionId string `json:"initialVersionId"`
	UserMetadata map[string]string `json:"userMetadata"`
	Data *[]byte `json:"-"`
	Entity any `json:"-"`
	Executed bool `json:"executed"`

	FinalETag string `json:"finalEtag"`
	FinalVersionId string `json:"finalVersionId"`
}

func (step *TransactionStep) SetFinalETagAndVersionId(finalETag string, finalVersionId string) {
	step.FinalETag = finalETag
	step.FinalVersionId = finalVersionId
}