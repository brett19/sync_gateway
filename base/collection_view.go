/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
)

// View-related functionality for collections.
// Views are deprecated and no longer supported with gocbcorex. All operations return errors.

var errViewsNotSupported = fmt.Errorf("views are not supported in this version of Sync Gateway")

func (c *Collection) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {
	return ddoc, errViewsNotSupported
}

func (c *Collection) GetDDocs() (ddocs map[string]sgbucket.DesignDoc, err error) {
	return nil, errViewsNotSupported
}

func (c *Collection) PutDDoc(ctx context.Context, docname string, sgDesignDoc *sgbucket.DesignDoc) error {
	return errViewsNotSupported
}

func (c *Collection) DeleteDDoc(docname string) error {
	return errViewsNotSupported
}

func (c *Collection) View(ctx context.Context, ddoc, name string, params map[string]any) (sgbucket.ViewResult, error) {
	return sgbucket.ViewResult{}, errViewsNotSupported
}

func (c *Collection) ViewQuery(ctx context.Context, ddoc, name string, params map[string]any) (sgbucket.QueryResultIterator, error) {
	return nil, errViewsNotSupported
}
