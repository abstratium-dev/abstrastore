package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/abstratium-informatique-sarl/abstrastore/pkg/minio"
)

var counter atomic.Uint64 // used to ensure that even if two records for the same id are written in the same nanosecond, they will have different paths 

// Appends an entity to the log. Analagous to inserting the latest version of data into an SQL database
// Param: schema - analagous to the database schema or database name in which to store the data
// Param: table - the name of the "table"
// Param: id - the id of the entity to append
// Param: t - the entity to append
func Append[T any](ctx context.Context, schema string, table string, id string, t T) error {
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	count := counter.Add(1)

	// format: schema/table/_snapshot___<id>___<nanos>___<count>
	path := fmt.Sprintf("%s/%s/_snapshot___%s___%d___%d", schema, table, id, time.Now().UnixNano(), count)
	return minio.GetRepository().CreateFile(ctx, path, data)
}

// TODO when writing a consolidation for a record, we need to first write the new consolidation and then delete the files
// used to make it. what way, if any reads happen during consolidation, they will not be adversely affected because they 
// will then use the already merged data to overwrite what is in the consolidation
