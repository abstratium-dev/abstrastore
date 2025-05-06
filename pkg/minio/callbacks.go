package minio

type Callback interface {
	
	// called if there is an error during garbage collection
	ErrorDuringGc(err error)
}
