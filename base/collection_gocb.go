/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/commonflags"
	"github.com/couchbase/gocbcorex/memdx"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// DefaultCollectionID represents _default._default collection
const DefaultCollectionID = uint32(0)

const (
	// SystemScope is the place for system collections to exist in
	SystemScope = "_system"
	// SystemCollectionMobile is the place to store Sync Gateway metadata on a system-collections-enabled Couchbase Server (7.6+)
	SystemCollectionMobile = "_mobile"

	// MetadataCollectionID is the KV collection ID for the SG Metadata store.
	// Subject to change when we move to system collections. Might not be possible to declare as const (need to retrieve from server?)
	MetadataCollectionID = DefaultCollectionID
)

type Collection struct {
	Bucket         *GocbV2Bucket
	scopeName      string
	collectionName string
	kvCollectionID uint32 // cached copy of KV's collection ID for this collection
}

// Ensure that Collection implements sgbucket.DataStore/N1QLStore
var (
	_ DataStore          = &Collection{}
	_ N1QLStore          = &Collection{}
	_ sgbucket.ViewStore = &Collection{}
)

func AsCollection(dataStore DataStore) (*Collection, error) {
	collection, ok := dataStore.(*Collection)
	if !ok {
		return nil, fmt.Errorf("dataStore is not a *Collection (got %T)", dataStore)
	}
	return collection, nil
}

// CollectionName returns the collection name
func (c *Collection) CollectionName() string {
	return c.collectionName
}

// ScopeName returns the scope name
func (c *Collection) ScopeName() string {
	return c.scopeName
}

// GetName returns a qualified name
func (c *Collection) GetName() string {
	if c.IsDefaultScopeCollection() {
		return c.Bucket.GetName()
	}
	return FullyQualifiedCollectionName(c.BucketName(), c.ScopeName(), c.CollectionName())
}

// agent returns the underlying gocbcorex agent from the bucket
func (c *Collection) agent() *gocbcorex.Agent {
	return c.Bucket.agent
}

// KV store

func (c *Collection) Get(k string, rv any) (cas uint64, err error) {

	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	getResult, err := c.agent().Get(ctx, &gocbcorex.GetOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
	})
	if err != nil {
		return 0, err
	}

	// Decode using commonflags
	err = sgTranscodeGet(getResult.Value, getResult.Flags, rv)
	return getResult.Cas, err
}

func (c *Collection) GetRaw(k string) (rv []byte, cas uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	getResult, err := c.agent().Get(ctx, &gocbcorex.GetOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
	})
	if err != nil {
		return nil, 0, err
	}

	return getResult.Value, getResult.Cas, nil
}

func (c *Collection) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, err := c.agent().GetAndTouch(ctx, &gocbcorex.GetAndTouchOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Expiry:         exp,
	})
	if err != nil {
		return nil, 0, err
	}

	return result.Value, result.Cas, nil
}

func (c *Collection) Touch(k string, exp uint32) (cas uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, err := c.agent().Touch(ctx, &gocbcorex.TouchOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Expiry:         exp,
	})
	if err != nil {
		return 0, err
	}
	return result.Cas, nil
}

func (c *Collection) Add(k string, exp uint32, v any) (added bool, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	value, flags, err := sgTranscodeSet(v)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, gocbErr := c.agent().Add(ctx, &gocbcorex.AddOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Value:          value,
		Flags:          flags,
		Expiry:         exp,
	})
	if gocbErr != nil {
		// Check key exists handling
		if errors.Is(gocbErr, memdx.ErrDocExists) {
			return false, nil
		}
		err = pkgerrors.WithStack(gocbErr)
	}
	return err == nil, err
}

func (c *Collection) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, gocbErr := c.agent().Add(ctx, &gocbcorex.AddOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Value:          v,
		Flags:          commonflags.Encode(commonflags.BinaryType, commonflags.NoCompression),
		Expiry:         exp,
	})
	if gocbErr != nil {
		// Check key exists handling
		if errors.Is(gocbErr, memdx.ErrDocExists) {
			return false, nil
		}
		err = pkgerrors.WithStack(gocbErr)
	}
	return err == nil, err
}

func (c *Collection) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v any) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	value, flags, err := sgTranscodeSet(v)
	if err != nil {
		return err
	}

	preserveExpiry := false
	if opts != nil {
		preserveExpiry = opts.PreserveExpiry
		if exp != 0 && preserveExpiry {
			preserveExpiry = false
		}
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, err = c.agent().Upsert(ctx, &gocbcorex.UpsertOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Value:          value,
		Flags:          flags,
		Expiry:         exp,
		PreserveExpiry: preserveExpiry,
	})
	return err
}

func (c *Collection) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	preserveExpiry := false
	if opts != nil {
		preserveExpiry = opts.PreserveExpiry
		if exp != 0 && preserveExpiry {
			preserveExpiry = false
		}
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, err := c.agent().Upsert(ctx, &gocbcorex.UpsertOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Value:          v,
		Flags:          commonflags.Encode(commonflags.BinaryType, commonflags.NoCompression),
		Expiry:         exp,
		PreserveExpiry: preserveExpiry,
	})
	return err
}

func (c *Collection) WriteCas(k string, exp uint32, cas uint64, v any, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	value, flags, err := sgTranscodeSet(v)
	if err != nil {
		return 0, err
	}

	if opt == sgbucket.Raw {
		flags = commonflags.Encode(commonflags.BinaryType, commonflags.NoCompression)
	}

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	if cas == 0 {
		result, err := c.agent().Add(ctx, &gocbcorex.AddOptions{
			Key:            []byte(k),
			ScopeName:      c.scopeName,
			CollectionName: c.collectionName,
			Value:          value,
			Flags:          flags,
			Expiry:         exp,
		})
		if err != nil {
			return 0, err
		}
		return result.Cas, nil
	}

	result, err := c.agent().Replace(ctx, &gocbcorex.ReplaceOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Value:          value,
		Flags:          flags,
		Cas:            cas,
		Expiry:         exp,
	})
	if err != nil {
		return 0, err
	}
	return result.Cas, nil
}

func (c *Collection) Delete(k string) error {
	_, err := c.Remove(k, 0)
	return err
}

func (c *Collection) Remove(k string, cas uint64) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, errRemove := c.agent().Delete(ctx, &gocbcorex.DeleteOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Cas:            cas,
	})
	if errRemove == nil && result != nil {
		casOut = result.Cas
	}
	return casOut, errRemove
}

func (c *Collection) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	for {
		var value []byte
		var err error
		var callbackExpiry *uint32

		// Load the existing value.
		var cas uint64

		c.Bucket.waitForAvailKvOp()
		ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
		getResult, getErr := c.agent().Get(ctx, &gocbcorex.GetOptions{
			Key:            []byte(k),
			ScopeName:      c.scopeName,
			CollectionName: c.collectionName,
		})
		cancel()
		c.Bucket.releaseKvOp()

		if getErr != nil {
			if !errors.Is(getErr, memdx.ErrDocNotFound) {
				// Unexpected error, abort
				return cas, getErr
			}
			cas = 0 // Key not found error
		} else {
			cas = getResult.Cas
			value = getResult.Value
		}

		// Invoke callback to get updated value
		var isDelete bool
		value, callbackExpiry, isDelete, err = callback(value)
		if err != nil {
			return cas, err
		}

		if callbackExpiry != nil {
			exp = *callbackExpiry
		}

		var resultCas uint64
		casRetry := false

		c.Bucket.waitForAvailKvOp()
		ctx2, cancel2 := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Add().
			result, addErr := c.agent().Add(ctx2, &gocbcorex.AddOptions{
				Key:            []byte(k),
				ScopeName:      c.scopeName,
				CollectionName: c.collectionName,
				Value:          value,
				Flags:          commonflags.Encode(commonflags.JSONType, commonflags.NoCompression),
				Expiry:         exp,
			})
			if addErr == nil {
				resultCas = result.Cas
			} else if errors.Is(addErr, memdx.ErrDocExists) {
				casRetry = true
			} else {
				err = addErr
			}
		} else {
			if value == nil && isDelete {
				result, delErr := c.agent().Delete(ctx2, &gocbcorex.DeleteOptions{
					Key:            []byte(k),
					ScopeName:      c.scopeName,
					CollectionName: c.collectionName,
					Cas:            cas,
				})
				if delErr == nil {
					resultCas = result.Cas
				} else if errors.Is(delErr, memdx.ErrCasMismatch) || errors.Is(delErr, memdx.ErrDocExists) {
					casRetry = true
				} else {
					err = delErr
				}
			} else {
				result, replErr := c.agent().Replace(ctx2, &gocbcorex.ReplaceOptions{
					Key:            []byte(k),
					ScopeName:      c.scopeName,
					CollectionName: c.collectionName,
					Value:          value,
					Flags:          commonflags.Encode(commonflags.JSONType, commonflags.NoCompression),
					Cas:            cas,
					Expiry:         exp,
				})
				if replErr == nil {
					resultCas = result.Cas
				} else if errors.Is(replErr, memdx.ErrCasMismatch) || errors.Is(replErr, memdx.ErrDocExists) {
					casRetry = true
				} else {
					err = replErr
				}
			}
		}
		cancel2()
		c.Bucket.releaseKvOp()

		if casRetry {
			// retry on cas failure
		} else {
			// err will be nil if successful
			return resultCas, err
		}
	}
}

func (c *Collection) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	incrResult, err := c.agent().Increment(ctx, &gocbcorex.IncrementOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
		Initial:        def,
		Delta:          amt,
		Expiry:         exp,
	})
	if err != nil {
		return 0, err
	}

	return incrResult.Value, nil
}

// Recoverable errors or timeouts trigger retry for read operations
func (c *Collection) isRecoverableReadError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, memdx.ErrTmpFail) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// Recoverable errors trigger retry for write operations
func (c *Collection) isRecoverableWriteError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, memdx.ErrTmpFail) {
		return true
	}
	return false
}

// GetExpiry requires a full document retrieval in order to obtain the expiry
func (c *Collection) GetExpiry(ctx context.Context, k string) (expiry uint32, getMetaError error) {
	opCtx, cancel := context.WithDeadline(ctx, c.Bucket.getBucketOpDeadline())
	defer cancel()

	result, err := c.agent().GetMeta(opCtx, &gocbcorex.GetMetaOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
	})
	if err != nil {
		return 0, err
	}

	return result.Expiry, nil
}

func (c *Collection) Exists(k string) (exists bool, err error) {
	ctx, cancel := context.WithDeadline(context.Background(), c.Bucket.getBucketOpDeadline())
	defer cancel()

	_, err = c.agent().GetMeta(ctx, &gocbcorex.GetMetaOptions{
		Key:            []byte(k),
		ScopeName:      c.scopeName,
		CollectionName: c.collectionName,
	})
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// sgTranscodeGet decodes a raw value+flags from KV into a Go value.
func sgTranscodeGet(value []byte, flags uint32, out any) error {
	valueType, compression := commonflags.Decode(flags)

	if compression != commonflags.NoCompression && compression != commonflags.UnknownCompression {
		return errors.New("unexpected value compression")
	}

	switch valueType {
	case commonflags.BinaryType:
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = value
			return nil
		case *any:
			*typedOut = value
			return nil
		case *string:
			*typedOut = string(value)
			return nil
		default:
			return errors.New("you must decode raw binary data into a byte array or string")
		}
	case commonflags.StringType:
		switch typedOut := out.(type) {
		case *string:
			*typedOut = string(value)
			return nil
		case *[]byte:
			*typedOut = value
			return nil
		default:
			return errors.New("you must decode string data into a string or byte array")
		}
	case commonflags.JSONType, commonflags.UnknownType:
		// JSON or legacy format — unmarshal
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = value
			return nil
		default:
			return JSONUnmarshal(value, typedOut)
		}
	}

	return errors.New("unexpected flags value")
}

// sgTranscodeSet encodes a Go value into raw bytes+flags for KV storage.
func sgTranscodeSet(value any) ([]byte, uint32, error) {
	switch v := value.(type) {
	case []byte:
		// Raw JSON bytes
		return v, commonflags.Encode(commonflags.JSONType, commonflags.NoCompression), nil
	case *[]byte:
		return *v, commonflags.Encode(commonflags.JSONType, commonflags.NoCompression), nil
	default:
		// Marshal to JSON
		bytes, err := JSONMarshal(value)
		if err != nil {
			return nil, 0, err
		}
		return bytes, commonflags.Encode(commonflags.JSONType, commonflags.NoCompression), nil
	}
}

// GetCollectionID returns the kv CollectionID for the current collection.
func (c *Collection) GetCollectionID() uint32 {
	return c.kvCollectionID
}

// setCollectionID sets private property of kv CollectionID.
func (c *Collection) setCollectionID() error {
	if !c.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		c.kvCollectionID = DefaultCollectionID
		return nil
	}
	// default collection has a known ID
	if c.IsDefaultScopeCollection() {
		c.kvCollectionID = DefaultCollectionID
		return nil
	}

	// Get the collection ID from the manifest
	manifest, err := c.Bucket.GetCollectionManifest()
	if err != nil {
		return err
	}

	id, ok := GetIDForCollection(manifest, c.scopeName, c.collectionName)
	if !ok {
		return fmt.Errorf("collection %s.%s not found in manifest", c.scopeName, c.collectionName)
	}

	c.kvCollectionID = id
	return nil
}
