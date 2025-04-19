package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/abstratium-informatique-sarl/abstrastore/pkg/minio"
)


func ReadById[T any](ctx context.Context, schema string, table string, id string, template *T) (error) {

	// TODO one day we will only store diffs?  in such a case we would want to read all files and merge them.
	// today, we simply need to read the latest file.

	// format 1: schema/table/_snapshot___<id>___<nanos>___<count>
	// format 2: schema/table/_consolidation___<id>

	prefix := fmt.Sprintf("%s/%s/_snapshot___%s", schema, table, id)
	// read all files with the given prefix, then merge their contents
	files, err := minio.GetRepository().ListFiles(ctx, prefix)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	} else {
		// sort the files alphabetically
		sort.Strings(files)

		latest := files[len(files)-1]

		data, err := minio.GetRepository().ReadFile(ctx, latest)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(data, template); err != nil {
			return err
		}
		return nil
	}
}