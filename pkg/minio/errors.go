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
// Stale Object Error - means that a newer version of the object has already been written by a different transaction
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

