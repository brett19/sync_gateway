// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"

	sgbucket "github.com/couchbase/sg-bucket"
)

// getPreserveExpiry returns whether PreserveExpiry should be set for upsert operations, taking into
// account whether an explicit expiry has been set.
func getPreserveExpiry(ctx context.Context, exp uint32, upsertOptions *sgbucket.UpsertOptions) bool {
	if upsertOptions == nil {
		return false
	}
	if exp != 0 && upsertOptions.PreserveExpiry {
		InfofCtx(ctx, KeyCRUD, "Expiry set on UpsertOptions, but sgbucket.UpsertOptions.PreserveExpiry is false. Force setting PreserveExpiry to false to allow write to proceed.")
		return false
	}
	return upsertOptions.PreserveExpiry
}

// getMutateInPreserveExpiry returns whether PreserveExpiry should be set for mutateIn operations.
func getMutateInPreserveExpiry(ctx context.Context, exp uint32, mutateInOptions *sgbucket.MutateInOptions) bool {
	if mutateInOptions == nil {
		return false
	}
	if exp != 0 && mutateInOptions.PreserveExpiry {
		InfofCtx(ctx, KeyCRUD, "Expiry set on MutateInOptions, but sgbucket.MutateInOptions.PreserveExpiry is false. Force setting PreserveExpiry to false to allow write to proceed.")
		return false
	}
	return mutateInOptions.PreserveExpiry
}
