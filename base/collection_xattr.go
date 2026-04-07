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
	"errors"
	"fmt"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// IsSupported is a shim that queries the parent bucket's feature
func (c *Collection) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return c.Bucket.IsSupported(feature)
}

var _ sgbucket.XattrStore = &Collection{}

func (c *Collection) GetSpec() BucketSpec {
	return c.Bucket.Spec
}

// InsertTombstoneWithXattrs inserts a new server tombstone with the specified system xattrs
func (c *Collection) InsertTombstoneWithXattrs(ctx context.Context, k string, exp uint32, xattrValue map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {

	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	supportsTombstoneCreation := c.IsSupported(sgbucket.BucketStoreFeatureCreateDeletedWithXattr)

	var docFlags memdx.SubdocDocFlag
	if supportsTombstoneCreation {
		docFlags = memdx.SubdocDocFlagCreateAsDeleted | memdx.SubdocDocFlagAccessDeleted | memdx.SubdocDocFlagAddDoc
	} else {
		docFlags = memdx.SubdocDocFlagMkDoc
	}

	mutateOps := make([]memdx.MutateInOp, 0, len(xattrValue))
	for xattrKey, value := range xattrValue {
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
			Value: value,
		})
	}

	mutateOps = appendMacroExpansions(mutateOps, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          docFlags,
		Expiry:         exp,
		Cas:            0,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

func (c *Collection) DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error {
	return DeleteWithXattrs(ctx, c, k, xattrKeys)
}

func (c *Collection) GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, casOut uint64, err error) {
	_, _, xattrs, casOut, err = c.subdocGetBodyAndXattrs(ctx, k, xattrKeys, false)
	return xattrs, casOut, err
}

func (c *Collection) GetSubDocRaw(ctx context.Context, k string, subdocKey string) ([]byte, uint64, error) {
	return c.SubdocGetRaw(ctx, k, subdocKey)
}

func (c *Collection) WriteSubDoc(ctx context.Context, k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	return c.SubdocWrite(ctx, k, subdocKey, cas, value)
}

func (c *Collection) GetWithXattrs(ctx context.Context, k string, xattrKeys []string) ([]byte, map[string][]byte, uint64, error) {
	_, body, xattrs, cas, err := c.subdocGetBodyAndXattrs(ctx, k, xattrKeys, true)
	return body, xattrs, cas, err
}

func (c *Collection) SetXattrs(ctx context.Context, k string, xattrs map[string][]byte) (casOut uint64, err error) {
	return c.SubdocSetXattrs(k, xattrs)
}

func (c *Collection) RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) (err error) {
	return RemoveXattrs(ctx, c, k, xattrKeys, cas)
}

func (c *Collection) DeleteSubDocPaths(ctx context.Context, k string, paths ...string) (err error) {
	return removeSubdocPaths(ctx, c, k, paths...)
}

func (c *Collection) DeleteXattrs(ctx context.Context, k string, xattrKeys ...string) (err error) {
	return removeSubdocPaths(ctx, c, k, xattrKeys...)
}

func (c *Collection) SubdocGetRaw(ctx context.Context, k string, subdocKey string) ([]byte, uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	var rawValue []byte

	worker := func() (shouldRetry bool, err error, casOut uint64) {
		ops := []memdx.LookupInOp{
			{
				Op:   memdx.LookupInOpTypeGet,
				Path: []byte(subdocKey),
			},
		}

		opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
		defer cancel()

		res, lookupErr := c.agent().LookupIn(opCtx, &gocbcorex.LookupInOptions{
			Key:            []byte(k),
			ScopeName:      c.scopeName,
			CollectionName: c.collectionName,
			Ops:            ops,
		})
		if lookupErr != nil {
			isRecoverable := c.isRecoverableReadError(lookupErr)
			if isRecoverable {
				return isRecoverable, lookupErr, 0
			}

			if errors.Is(lookupErr, memdx.ErrDocNotFound) {
				return false, ErrNotFound, 0
			}

			return false, lookupErr, 0
		}

		if len(res.Ops) > 0 && res.Ops[0].Err != nil {
			return false, res.Ops[0].Err, 0
		}

		if len(res.Ops) > 0 {
			rawValue = res.Ops[0].Value
		}

		return false, nil, res.Cas
	}

	err, casOut := RetryLoopCas(ctx, "SubdocGetRaw", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetRaw with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return rawValue, casOut, err
}

func (c *Collection) SubdocWrite(ctx context.Context, k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	worker := func() (shouldRetry bool, err error, casOut uint64) {
		mutateOps := []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Flags: memdx.SubdocOpFlagMkDirP,
				Path:  []byte(subdocKey),
				Value: value,
			},
		}

		opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
		defer cancel()

		result, err := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
			Key:            []byte(k),
			ScopeName:      c.scopeName,
			CollectionName: c.collectionName,
			Ops:            mutateOps,
			Flags:          memdx.SubdocDocFlagMkDoc,
			Cas:            cas,
		})
		if err == nil {
			return false, nil, result.Cas
		}

		shouldRetry = c.isRecoverableWriteError(err)
		if shouldRetry {
			return shouldRetry, err, 0
		}

		return false, err, 0
	}

	err, casOut := RetryLoopCas(ctx, "SubdocWrite", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocWrite with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return casOut, err
}

// subdocGetBodyAndXattrs retrieves the document body and xattrs in a single LookupIn subdoc operation.  Does not require both to exist.
func (c *Collection) subdocGetBodyAndXattrs(ctx context.Context, k string, xattrKeys []string, fetchBody bool) (isTombstone bool, rawBody []byte, xattrs map[string][]byte, cas uint64, err error) {
	xattrKey2 := ""
	// Backward compatibility for one system xattr and one user xattr support.
	if !c.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
		if len(xattrKeys) > 2 {
			return false, nil, nil, 0, fmt.Errorf("subdocGetBodyAndXattrs: more than 2 xattrKeys %+v not supported in this version of Couchbase Server", xattrKeys)
		}
		if len(xattrKeys) == 2 {
			xattrKey2 = xattrKeys[1]
			xattrKeys = []string{xattrKeys[0]}
		}
	}
	xattrs = make(map[string][]byte, len(xattrKeys))
	worker := func() (shouldRetry bool, err error, value uint64) {

		c.Bucket.waitForAvailKvOp()
		defer c.Bucket.releaseKvOp()

		// Build lookup ops: xattr gets first, then body
		ops := make([]memdx.LookupInOp, 0, len(xattrKeys)+1)
		for _, xattrKey := range xattrKeys {
			ops = append(ops, memdx.LookupInOp{
				Op:    memdx.LookupInOpTypeGet,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte(xattrKey),
			})
		}
		if fetchBody {
			ops = append(ops, memdx.LookupInOp{
				Op: memdx.LookupInOpTypeGetDoc,
			})
		}

		opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
		defer cancel()

		res, lookupErr := c.agent().LookupIn(opCtx, &gocbcorex.LookupInOptions{
			Key:            []byte(k),
			ScopeName:      c.scopeName,
			CollectionName: c.collectionName,
			Ops:            ops,
			Flags:          memdx.SubdocDocFlagAccessDeleted,
		})
		if lookupErr != nil {
			if errors.Is(lookupErr, memdx.ErrDocNotFound) {
				return false, ErrNotFound, 0
			}
			shouldRetry = c.isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

		cas = res.Cas
		isTombstone = res.DocIsDeleted

		// Extract xattr results
		var xattrErrors []error
		for i, xattrKey := range xattrKeys {
			if i < len(res.Ops) {
				if res.Ops[i].Err != nil {
					xattrErrors = append(xattrErrors, res.Ops[i].Err)
					continue
				}
				xattrs[xattrKey] = res.Ops[i].Value
			}
		}

		// Extract body result
		var docErr error
		if fetchBody {
			bodyIdx := len(xattrKeys)
			if bodyIdx < len(res.Ops) {
				if res.Ops[bodyIdx].Err != nil {
					docErr = res.Ops[bodyIdx].Err
				} else {
					rawBody = res.Ops[bodyIdx].Value
				}
			}
		}

		// If doc is a tombstone and all xattrs are not found, treat as ErrNotFound
		if isTombstone && len(xattrErrors) == len(xattrKeys) {
			return false, ErrNotFound, cas
		}

		// If doc not requested and no xattrs are found, treat as ErrXattrNotFound
		if !fetchBody && len(xattrErrors) == len(xattrKeys) {
			return false, ErrXattrNotFound, cas
		}

		_ = docErr // handled via isTombstone flag

		// If BucketStoreFeatureMultiXattrSubdocOperations is not supported, do a second get for the second xattr.
		if xattrKey2 != "" {
			xattrs2, xattr2Cas, xattr2Err := c.GetXattrs(ctx, k, []string{xattrKey2})
			switch pkgerrors.Cause(xattr2Err) {
			case ErrNotFound:
				// If key not found it has been deleted in between the first op and this op.
				return false, err, xattr2Cas
			case ErrXattrNotFound:
				// Xattr doesn't exist, can skip
			case nil:
				if cas != xattr2Cas {
					return true, errors.New("cas mismatch between user xattr and document body"), uint64(0)
				}
			default:
				// Unknown error occurred
				return false, xattr2Err, uint64(0)
			}
			xattr2, ok := xattrs2[xattrKey2]
			if ok {
				xattrs[xattrKey2] = xattr2
			}
		}
		return false, nil, cas
	}

	// Kick off retry loop
	err, cas = RetryLoopCas(ctx, "subdocGetBodyAndXattrs", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "subdocGetBodyAndXattrs %v", UD(k).Redact())
	}

	return isTombstone, rawBody, xattrs, cas, err
}

// createTombstone inserts a new server tombstone with associated xattrs.  Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) createTombstone(ctx context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	supportsTombstoneCreation := c.IsSupported(sgbucket.BucketStoreFeatureCreateDeletedWithXattr)

	var docFlags memdx.SubdocDocFlag
	if supportsTombstoneCreation {
		docFlags = memdx.SubdocDocFlagCreateAsDeleted | memdx.SubdocDocFlagAccessDeleted | memdx.SubdocDocFlagAddDoc
	} else {
		docFlags = memdx.SubdocDocFlagMkDoc
	}

	mutateOps, err := getUpsertSpecsForXattrs(xattrs)
	if err != nil {
		return 0, err
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          docFlags,
		Expiry:         exp,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// insertBodyAndXattrs inserts a document and associated xattrs in a single mutateIn operation.  Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) insertBodyAndXattrs(ctx context.Context, k string, exp uint32, v any, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps, err := getUpsertSpecsForXattrs(xattrs)
	if err != nil {
		return 0, err
	}

	bodyBytes, err := JSONMarshal(v)
	if err != nil {
		return 0, err
	}
	mutateOps = append(mutateOps, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeSetDoc,
		Value: bodyBytes,
	})
	mutateOps = appendMacroExpansions(mutateOps, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          memdx.SubdocDocFlagAddDoc,
		Expiry:         exp,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// SubdocInsert performs a subdoc insert operation to the specified path in the document body.
func (c *Collection) SubdocInsert(ctx context.Context, k string, fieldPath string, cas uint64, value any) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	valueBytes, err := JSONMarshal(value)
	if err != nil {
		return err
	}

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictAdd,
			Path:  []byte(fieldPath),
			Value: valueBytes,
		},
	}

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Cas:            cas,
	})

	if errors.Is(mutateErr, memdx.ErrDocNotFound) {
		return ErrNotFound
	}

	if errors.Is(mutateErr, memdx.ErrSubDocPathExists) {
		return ErrAlreadyExists
	}

	if errors.Is(mutateErr, memdx.ErrSubDocPathNotFound) {
		return ErrPathNotFound
	}

	return mutateErr
}

// SubdocSetXattrs performs a set of the given xattr. Does a straight set with no cas.
func (c *Collection) SubdocSetXattrs(k string, xvs map[string][]byte) (casOut uint64, err error) {

	mutateOps := make([]memdx.MutateInOp, 0, len(xvs))
	for xattrKey, xv := range xvs {
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
			Value: xv,
		})
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          memdx.SubdocDocFlagMkDoc | memdx.SubdocDocFlagAccessDeleted,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}

	return result.Cas, nil
}

// UpdateXattrs updates the xattrs on an existing document. Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return c.updateXattrs(ctx, k, exp, cas, xattrs, nil, opts)
}

func (c *Collection) updateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, xattrsToDelete []string, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	if !c.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) && len(xattrs) >= 2 {
		return 0, fmt.Errorf("UpdateXattrs: more than 1 xattr %v not supported in UpdateXattrs in this version of Couchbase Server", xattrs)
	}
	if cas == 0 && len(xattrsToDelete) > 0 {
		return 0, sgbucket.ErrDeleteXattrOnDocumentInsert
	}
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps, err := getUpsertSpecsForXattrs(xattrs)
	if err != nil {
		return 0, err
	}
	for _, xattrKey := range xattrsToDelete {
		if _, ok := xattrs[xattrKey]; ok {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrUpsertAndDeleteSameXattr)
		}
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
		})
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)

	preserveExpiry := getMutateInPreserveExpiry(ctx, exp, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          memdx.SubdocDocFlagAccessDeleted,
		Expiry:         exp,
		PreserveExpiry: preserveExpiry,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// updateBodyAndXattrs updates the document body and xattrs of an existing document. Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) updateBodyAndXattrs(ctx context.Context, k string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v any, xattrs map[string][]byte, xattrsToDelete []string) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps, err := getUpsertSpecsForXattrs(xattrs)
	if err != nil {
		return 0, err
	}
	for _, xattrKey := range xattrsToDelete {
		if _, ok := xattrs[xattrKey]; ok {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrUpsertAndDeleteSameXattr)
		}
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
		})
	}

	bodyBytes, err := JSONMarshal(v)
	if err != nil {
		return 0, err
	}
	mutateOps = append(mutateOps, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeSetDoc,
		Value: bodyBytes,
	})
	mutateOps = appendMacroExpansions(mutateOps, opts)

	preserveExpiry := getMutateInPreserveExpiry(ctx, exp, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Expiry:         exp,
		PreserveExpiry: preserveExpiry,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// updateXattrDeleteBody deletes the document body and updates the xattrs of an existing document. Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) updateXattrsDeleteBody(ctx context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, xattrsToDelete []string, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps, err := getUpsertSpecsForXattrs(xattrs)
	if err != nil {
		return 0, err
	}
	if cas == 0 && len(xattrsToDelete) > 0 {
		return 0, sgbucket.ErrDeleteXattrOnDocumentInsert
	}

	for _, xattrKey := range xattrsToDelete {
		if _, ok := xattrs[xattrKey]; ok {
			return 0, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrUpsertAndDeleteSameXattr)
		}
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
		})
	}
	// Delete the body
	mutateOps = append(mutateOps, memdx.MutateInOp{
		Op: memdx.MutateInOpTypeDeleteDoc,
	})

	mutateOps = appendMacroExpansions(mutateOps, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Expiry:         exp,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// UpdateXattrDeleteBody deletes the document body and updates the xattr of an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) UpdateXattrDeleteBody(ctx context.Context, k, xattrKey string, exp uint32, cas uint64, xv any, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	xvBytes, err := JSONMarshal(xv)
	if err != nil {
		return 0, err
	}

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
			Value: xvBytes,
		},
		{
			Op: memdx.MutateInOpTypeDeleteDoc,
		},
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Expiry:         exp,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// subdocDeleteXattrs deletes xattrs of an existing document (or document tombstone)
func (c *Collection) subdocDeleteXattrs(k string, xattrKeys []string, cas uint64) (err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]memdx.MutateInOp, 0, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
		})
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          memdx.SubdocDocFlagAccessDeleted,
		Cas:            cas,
	})
	return mutateErr
}

// subdocRemovePaths will delete the supplied xattr keys from a document. Not a cas safe operation.
func (c *Collection) subdocRemovePaths(k string, xattrKeys ...string) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]memdx.MutateInOp, 0, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
		})
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
	})

	return mutateErr
}

// deleteBodyAndXattrs deletes the document body and associated xattrs of an existing document.
func (c *Collection) deleteBodyAndXattrs(ctx context.Context, k string, xattrKeys []string) (err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]memdx.MutateInOp, 0, len(xattrKeys)+1)

	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
		})
	}
	// Delete body via subdoc remove on empty path
	mutateOps = append(mutateOps, memdx.MutateInOp{
		Op:   memdx.MutateInOpTypeDelete,
		Path: []byte(""),
	})

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
	})
	if mutateErr == nil {
		return nil
	}

	// StatusKeyNotFound returned if document doesn't exist
	if errors.Is(mutateErr, memdx.ErrDocNotFound) {
		return ErrNotFound
	}

	// StatusSubDocBadMulti returned if xattr doesn't exist
	if errors.Is(mutateErr, memdx.ErrSubDocPathNotFound) {
		return ErrXattrNotFound
	}
	return mutateErr
}

// deleteBody deletes the document body of an existing document, and updates cas and crc32c in the associated xattr. Used in Couchbase Server < 6.6
func (c *Collection) deleteBody(ctx context.Context, k string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []memdx.MutateInOp{
		{
			Op: memdx.MutateInOpTypeDeleteDoc,
		},
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)

	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(opCtx, &gocbcorex.MutateInOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Expiry:         exp,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// isKVError checks if the error has a specific memdx status code.
func isKVError(err error, code memdx.Status) bool {
	if err == nil {
		return false
	}

	var serverErr *memdx.ServerError
	if errors.As(err, &serverErr) {
		return serverErr.Status == code
	}

	var serverErrCtx *memdx.ServerErrorWithContext
	if errors.As(err, &serverErrCtx) {
		return serverErrCtx.Cause.Status == code
	}

	return false
}

// appendMacroExpansions will append macro expansions defined in MutateInOptions to the provided
// memdx MutateInOp slice.
func appendMacroExpansions(mutateInSpec []memdx.MutateInOp, opts *sgbucket.MutateInOptions) []memdx.MutateInOp {

	if opts == nil {
		return mutateInSpec
	}
	for _, v := range opts.MacroExpansion {
		mutateInSpec = append(mutateInSpec, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
			Path:  []byte(v.Path),
			Value: memdxMutationMacro(v.Type),
		})
	}
	return mutateInSpec
}

func memdxMutationMacro(meType sgbucket.MacroExpansionType) []byte {
	switch meType {
	case sgbucket.MacroCas:
		return memdx.SubdocMacroNewCas
	case sgbucket.MacroCrc32c:
		return memdx.SubdocMacroNewCrc32c
	default:
		return memdx.SubdocMacroNewCas
	}
}

// getUpsertSpecsForXattrs returns a slice of memdx.MutateInOp for the given xattrs, or returns an error if any values are nil.
func getUpsertSpecsForXattrs(xattrs map[string][]byte) ([]memdx.MutateInOp, error) {
	mutateOps := make([]memdx.MutateInOp, 0, len(xattrs))
	for xattrKey, xattrVal := range xattrs {
		if xattrVal == nil {
			return nil, fmt.Errorf("%s: %w", xattrKey, sgbucket.ErrNilXattrValue)
		}
		mutateOps = append(mutateOps, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrKey),
			Value: xattrVal,
		})
	}
	return mutateOps, nil
}
