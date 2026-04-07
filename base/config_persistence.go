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
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
)

// ConfigPersistence manages the underlying storage of database config documents in the bucket.
// Implementations support using either document body or xattr for storage
type ConfigPersistence interface {
	// Operations for interacting with raw config ([]byte).
	// cas values represent document cas; cfgCas represent the cas associated with the last mutation
	loadRawConfig(ctx context.Context, c *Collection, key string) ([]byte, uint64, error)
	removeRawConfig(c *Collection, key string, cas uint64) (uint64, error)
	replaceRawConfig(c *Collection, key string, value []byte, cas uint64) (casOut uint64, err error)

	// Operations for interacting with marshalled config. cfgCas represents the cas value
	// associated with the last config mutation, and may not match document CAS
	loadConfig(ctx context.Context, c *Collection, key string, valuePtr any) (cfgCas uint64, err error)
	insertConfig(c *Collection, key string, value any) (cfgCas uint64, err error)

	// touchConfigRollback sets the specific property to the specified string value via a subdoc operation.
	// Used to change the cas value during rollback, to guard against races with slow updates
	touchConfigRollback(c *Collection, key string, property string, value string, cas uint64) (casOut uint64, err error)

	// keyExists checks whether the specified key exists in the collection
	keyExists(c *Collection, key string) (found bool, err error)
}

var _ ConfigPersistence = &XattrBootstrapPersistence{}
var _ ConfigPersistence = &DocumentBootstrapPersistence{}

// System xattr persistence
type XattrBootstrapPersistence struct {
	CommonBootstrapPersistence
}

const cfgXattrKey = "_sync"
const cfgXattrConfigPath = cfgXattrKey + ".config"
const cfgXattrBody = `{"cfgVersion": 1}`

func (xbp *XattrBootstrapPersistence) insertConfig(c *Collection, key string, value any) (cas uint64, err error) {

	valueBytes, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(cfgXattrConfigPath),
			Value: valueBytes,
		},
		{
			Op:    memdx.MutateInOpTypeSetDoc,
			Value: []byte(cfgXattrBody),
		},
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          memdx.SubdocDocFlagAddDoc,
	})
	if errors.Is(mutateErr, memdx.ErrDocExists) {
		return 0, ErrAlreadyExists
	}
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

func (xbp *XattrBootstrapPersistence) touchConfigRollback(c *Collection, key, property, value string, cas uint64) (casOut uint64, err error) {
	xattrProperty := cfgXattrKey + "." + property

	valueBytes, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(xattrProperty),
			Value: valueBytes,
		},
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// loadRawConfig returns the config and document cas.  Does not restore deleted documents,
// to avoid cas collisions with concurrent updates
func (xbp *XattrBootstrapPersistence) loadRawConfig(ctx context.Context, c *Collection, key string) ([]byte, uint64, error) {

	ops := []memdx.LookupInOp{
		{
			Op:    memdx.LookupInOpTypeGet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(cfgXattrConfigPath),
		},
	}

	opCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	res, lookupErr := c.agent().LookupIn(opCtx, &gocbcorex.LookupInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            ops,
		Flags:          memdx.SubdocDocFlagAccessDeleted,
	})
	if lookupErr != nil {
		if errors.Is(lookupErr, memdx.ErrDocNotFound) {
			DebugfCtx(ctx, KeyCRUD, "No config document found for key=%s", key)
			return nil, 0, ErrNotFound
		}
		return nil, 0, lookupErr
	}

	if len(res.Ops) > 0 && res.Ops[0].Err != nil {
		SyncGatewayStats.GlobalStats.ConfigStat.XattrFormatMismatches.Add(1)
		DebugfCtx(ctx, KeyCRUD, "Found config document but No xattr config found for key=%s, path=%s: %v", key, cfgXattrConfigPath, res.Ops[0].Err)
		return nil, 0, ErrNotFound
	}

	var rawValue []byte
	if len(res.Ops) > 0 {
		rawValue = res.Ops[0].Value
	}

	return rawValue, res.Cas, nil
}

func (xbp *XattrBootstrapPersistence) removeRawConfig(c *Collection, key string, cas uint64) (uint64, error) {

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(cfgXattrKey),
		},
		{
			Op:   memdx.MutateInOpTypeDelete,
			Path: []byte(""),
		},
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Cas:            cas,
	})
	if mutateErr == nil {
		return result.Cas, nil
	}

	// StatusKeyNotFound returned if document doesn't exist
	if errors.Is(mutateErr, memdx.ErrDocNotFound) {
		return 0, ErrNotFound
	}

	// StatusSubDocPathNotFound returned if xattr doesn't exist
	if errors.Is(mutateErr, memdx.ErrSubDocPathNotFound) {
		return 0, ErrNotFound
	}
	return 0, mutateErr
}

func (xbp *XattrBootstrapPersistence) replaceRawConfig(c *Collection, key string, value []byte, cas uint64) (uint64, error) {

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(cfgXattrConfigPath),
			Value: value,
		},
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// loadConfig returns the cas associated with the last cfg change.  If a deleted document body is
// detected, recreates the document to avoid metadata purge
func (xbp *XattrBootstrapPersistence) loadConfig(ctx context.Context, c *Collection, key string, valuePtr any) (cas uint64, err error) {

	ops := []memdx.LookupInOp{
		{
			Op:    memdx.LookupInOpTypeGet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(cfgXattrConfigPath),
		},
		{
			Op: memdx.LookupInOpTypeGetDoc,
		},
	}

	opCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	res, lookupErr := c.agent().LookupIn(opCtx, &gocbcorex.LookupInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            ops,
		Flags:          memdx.SubdocDocFlagAccessDeleted,
	})
	if lookupErr != nil {
		if errors.Is(lookupErr, memdx.ErrDocNotFound) {
			DebugfCtx(ctx, KeyCRUD, "No config document found for key=%s", key)
			return 0, ErrNotFound
		}
		return 0, lookupErr
	}

	// Check xattr result
	if len(res.Ops) > 0 && res.Ops[0].Err != nil {
		SyncGatewayStats.GlobalStats.ConfigStat.XattrFormatMismatches.Add(1)
		DebugfCtx(ctx, KeyCRUD, "Found config document but No xattr config found for key=%s, path=%s: %v", key, cfgXattrConfigPath, res.Ops[0].Err)
		return 0, ErrNotFound
	}

	// Unmarshal xattr value
	if len(res.Ops) > 0 && res.Ops[0].Value != nil {
		if unmarshalErr := JSONUnmarshal(res.Ops[0].Value, valuePtr); unmarshalErr != nil {
			return 0, unmarshalErr
		}
	}

	casOut := res.Cas

	// deleted document check - if body fetch failed (deleted), restore
	if len(res.Ops) > 1 && res.Ops[1].Err != nil {
		restoreCas, restoreErr := xbp.restoreDocumentBody(c, key, valuePtr)
		if restoreErr != nil {
			WarnfCtx(ctx, "Error attempting to restore unexpected deletion of config: %v", restoreErr)
		} else {
			casOut = restoreCas
		}
	}
	return casOut, nil
}

// Restore a deleted document's body.  Rewrites metadata
func (xbp *XattrBootstrapPersistence) restoreDocumentBody(c *Collection, key string, value any) (casOut uint64, err error) {

	valueBytes, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte(cfgXattrConfigPath),
			Value: valueBytes,
		},
		{
			Op:    memdx.MutateInOpTypeSetDoc,
			Value: []byte(cfgXattrBody),
		},
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Flags:          memdx.SubdocDocFlagAddDoc,
	})
	if errors.Is(mutateErr, memdx.ErrDocExists) {
		return 0, ErrAlreadyExists
	}
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// Document Body persistence stores config in the document body.
// cfgCas is just document cas
type DocumentBootstrapPersistence struct {
	CommonBootstrapPersistence
}

func (dbp *DocumentBootstrapPersistence) loadRawConfig(_ context.Context, c *Collection, key string) ([]byte, uint64, error) {
	rv, cas, err := c.GetRaw(key)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}

	return rv, cas, nil
}

func (dbp *DocumentBootstrapPersistence) removeRawConfig(c *Collection, key string, cas uint64) (uint64, error) {
	casOut, err := c.Remove(key, cas)
	if err != nil {
		return 0, err
	}
	return casOut, nil
}

func (dbp *DocumentBootstrapPersistence) replaceRawConfig(c *Collection, key string, value []byte, cas uint64) (uint64, error) {

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, err := c.agent().Replace(ctx, &gocbcorex.ReplaceOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Value:          value,
		Cas:            cas,
	})
	if err != nil {
		return 0, err
	}

	return result.Cas, nil
}

func (dbp *DocumentBootstrapPersistence) loadConfig(_ context.Context, c *Collection, key string, valuePtr any) (cas uint64, err error) {

	cas, err = c.Get(key, valuePtr)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return 0, ErrNotFound
		}
		return 0, err
	}

	return cas, nil
}

func (dbp *DocumentBootstrapPersistence) insertConfig(c *Collection, key string, value any) (cas uint64, err error) {
	added, err := c.Add(key, 0, value)
	if err != nil {
		return 0, err
	}
	if !added {
		return 0, ErrAlreadyExists
	}
	// Add doesn't return a CAS in our interface, need to fetch it
	_, fetchCas, fetchErr := c.GetRaw(key)
	if fetchErr != nil {
		return 0, fetchErr
	}
	return fetchCas, nil
}

func (dbp *DocumentBootstrapPersistence) touchConfigRollback(c *Collection, key, property, value string, cas uint64) (casOut uint64, err error) {
	valueBytes, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	mutateOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDictSet,
			Path:  []byte(property),
			Value: valueBytes,
		},
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, mutateErr := c.agent().MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            []byte(key),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Ops:            mutateOps,
		Cas:            cas,
	})
	if mutateErr != nil {
		return 0, mutateErr
	}
	return result.Cas, nil
}

// Common operations that don't depend on storage format
type CommonBootstrapPersistence struct {
}

// Check whether the specified key exists.  Ignores format of stored data
func (cbp *CommonBootstrapPersistence) keyExists(c *Collection, key string) (bool, error) {
	return c.Exists(key)
}
