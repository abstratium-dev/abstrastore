package minio

import (
	"fmt"
)

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Duplicate Key Error - means that an object with that key already exists
// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var DuplicateKeyError = fmt.Errorf("object already exists")

type DuplicateKeyErrorWithDetails struct {
	Details string
}

func (e *DuplicateKeyErrorWithDetails) Error() string {
	return e.Details
}

func (e *DuplicateKeyErrorWithDetails) Unwrap() error {
	return DuplicateKeyError
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NoSuchKeyError - means that an object with that key does not exist
// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var NoSuchKeyError = fmt.Errorf("object does not exist")

type NoSuchKeyErrorWithDetails struct {
	Details string
}

func (e *NoSuchKeyErrorWithDetails) Error() string {
	return e.Details
}

func (e *NoSuchKeyErrorWithDetails) Unwrap() error {
	return NoSuchKeyError
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Stale Object Error - means that a newer version of the object has already been written by a different transaction.
// That transaction may not be committed - which sort of means that this is a phantom read, however, instead
// of blocking the read like an SQL database does, we assume that most of the time, the transaction will 
// commit, as that is the goal and rollbacks are nasty for users. So, assuming that most transactions will
// commit, that would mean that the version that the current transaction has read will not be able to be 
// committed and so instead of blocking and then failing, we just fail fast. Oh, and Minio doesn't support 
// blocking and we don't want to do polling since that is nasty (inpeformant) too.
// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var StaleObjectError = fmt.Errorf("object is stale")

type StaleObjectErrorWithDetails[T any] struct {
	Details string
	Object  T
}

func (e *StaleObjectErrorWithDetails[T]) Error() string {
	return e.Details
}

func (e *StaleObjectErrorWithDetails[T]) GetStaleObject() T {
	return e.Object
}

func (e *StaleObjectErrorWithDetails[T]) Unwrap() error {
	return StaleObjectError
}

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Object Locked Error - means that the object is currently being written by a different transaction,
// and assuming it will commit, this attempt by the caller would fail with a StaleObjectError.
// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var ObjectLockedError = fmt.Errorf("object is locked")

type ObjectLockedErrorWithDetails[T any] struct {
	Details string
	Object  T
	DueByMsEpoch   uint64
}

func (e *ObjectLockedErrorWithDetails[T]) Error() string {
	return e.Details
}

func (e *ObjectLockedErrorWithDetails[T]) GetLockedObject() T {
	return e.Object
}

func (e *ObjectLockedErrorWithDetails[T]) Unwrap() error {
	return ObjectLockedError
}

