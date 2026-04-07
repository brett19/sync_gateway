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
	"expvar"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	sgbucket "github.com/couchbase/sg-bucket"
)

//

const openStreamTimeout = 30 * time.Second
const openRetryCount = uint32(10)
const DefaultNumWorkers = 8

// DCP buffer size if we are running in serverless
const DefaultDCPBufferServerless = 1 * 1024 * 1024

const getVbSeqnoTimeout = 30 * time.Second

const infiniteOpenStreamRetries = uint32(math.MaxUint32)

type endStreamCallbackFunc func(e endStreamEvent)

var ErrVbUUIDMismatch = errors.New("VbUUID mismatch when failOnRollback set")

type GoCBDCPClient struct {
	ctx                        context.Context
	dcpStreamName              string                         // DCP stream name, must be unique
	streamSet                  *gocbcorex.DcpStreamSet         // gocbcorex DCP stream set, manages DCP connections and stream operations
	bucket                     *GocbV2Bucket                   // Bucket reference for accessing gocbcorex agent
	callback                   sgbucket.FeedEventCallbackFunc // Callback invoked on DCP mutations/deletions
	workers                    []*DCPWorker                   // Workers for concurrent processing of incoming mutations and callback.  vbuckets are partitioned across workers
	workersWg                  sync.WaitGroup                 // Active workers WG - used for signaling when the DCPClient workers have all stopped so the doneChannel can be closed
	supportsCollections        bool                           // Whether the target data store supports collections
	numVbuckets                uint16                         // number of vbuckets on target data store
	terminator                 chan bool                      // Used to close worker goroutines spawned by the DCPClient
	doneChannel                chan error                     // Returns nil on successful completion of one-shot feed or external close of feed, error otherwise
	metadata                   DCPMetadataStore               // Implementation of DCPMetadataStore for metadata persistence
	activeVbuckets             map[uint16]struct{}            // vbuckets that have an open stream
	activeVbucketLock          sync.Mutex                     // Synchronization for activeVbuckets
	oneShot                    bool                           // Whether DCP feed should be one-shot
	closing                    AtomicBool                     // Set when the client is closing (either due to internal or external request)
	closeError                 error                          // Will be set to a non-nil value for unexpected error
	closeErrorLock             sync.Mutex                     // Synchronization on close error
	failOnRollback             bool                           // When true, close when rollback detected
	checkpointPrefix           string                         // DCP checkpoint key prefix
	checkpointPersistFrequency *time.Duration                 // Used to override the default checkpoint persistence frequency
	dbStats                    *expvar.Map                    // Stats for database
	agentPriority              string                         // agentPriority specifies the priority level for a dcp stream
	collectionIDs              []uint32                       // collectionIDs used by gocbcorex, if empty, uses default collections
	feedContent                sgbucket.FeedContent           // feedContent specifies whether the DCP feed should include values, xattrs, or both
}

type DCPClientOptions struct {
	FeedID                     string // Optional description for a DCP feed
	NumWorkers                 int
	OneShot                    bool
	FailOnRollback             bool                 // When true, the DCP client will terminate on DCP rollback
	InitialMetadata            []DCPMetadata        // When set, will be used as initial metadata for the DCP feed.  Will override any persisted metadata
	CheckpointPersistFrequency *time.Duration       // Overrides metadata persistence frequency - intended for test use
	MetadataStoreType          DCPMetadataStoreType // define storage type for DCPMetadata
	DbStats                    *expvar.Map          // Optional stats
	AgentPriority              string               // agentPriority specifies the priority level for a dcp stream (e.g. "medium")
	CollectionIDs              []uint32             // CollectionIDs used by gocbcorex, if empty, uses default collections
	CheckpointPrefix           string
	FeedContent                sgbucket.FeedContent // FeedContent specifies whether the DCP feed should include values, xattrs, or both
}

// dcpOptionsForFeedContent returns the gocbcorex KvClientDcpOptions flags for the given FeedContent option.
func dcpOptionsForFeedContent(ctx context.Context, c sgbucket.FeedContent) (includeXattrs bool, excludeValues bool) {
	switch c {
	case sgbucket.FeedContentDefault:
		return true, false
	case sgbucket.FeedContentKeysOnly:
		return false, true
	case sgbucket.FeedContentBodyOnly:
		return false, false
	case sgbucket.FeedContentXattrOnly:
		return true, true
	default:
		AssertfCtx(ctx, "invalid FeedContent value: %d", c)
		return true, false
	}
}

func NewDCPClient(ctx context.Context, callback sgbucket.FeedEventCallbackFunc, options DCPClientOptions, bucket *GocbV2Bucket) (*GoCBDCPClient, error) {

	numVbuckets, err := bucket.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	return newDCPClientWithForBuckets(ctx, callback, options, bucket, numVbuckets)
}

func newDCPClientWithForBuckets(ctx context.Context, callback sgbucket.FeedEventCallbackFunc, options DCPClientOptions, bucket *GocbV2Bucket, numVbuckets uint16) (*GoCBDCPClient, error) {

	numWorkers := DefaultNumWorkers
	if options.NumWorkers > 0 {
		numWorkers = options.NumWorkers
	}
	if options.AgentPriority == "high" {
		return nil, fmt.Errorf("sync gateway should not set high priority for DCP feeds")
	}

	if options.CheckpointPrefix == "" {
		if options.MetadataStoreType == DCPMetadataStoreCS {
			return nil, fmt.Errorf("callers must specify a checkpoint prefix when persisting metadata")
		}
	}
	dcpStreamName, err := GenerateDcpStreamName(options.FeedID)
	if err != nil {
		return nil, fmt.Errorf("error generating DCP stream name: %w", err)
	}
	client := &GoCBDCPClient{
		ctx:                 ctx,
		dcpStreamName:       dcpStreamName,
		workers:             make([]*DCPWorker, numWorkers),
		numVbuckets:         numVbuckets,
		callback:            callback,
		bucket:              bucket,
		supportsCollections: bucket.IsSupported(sgbucket.BucketStoreFeatureCollections),
		terminator:          make(chan bool),
		doneChannel:         make(chan error, 1),
		failOnRollback:      options.FailOnRollback,
		checkpointPrefix:    options.CheckpointPrefix,
		dbStats:             options.DbStats,
		agentPriority:       options.AgentPriority,
		collectionIDs:       options.CollectionIDs,
		feedContent:         options.FeedContent,
	}

	// Initialize active vbuckets
	client.activeVbuckets = make(map[uint16]struct{})
	for vbNo := range numVbuckets {
		client.activeVbuckets[vbNo] = struct{}{}
	}

	switch options.MetadataStoreType {
	case DCPMetadataStoreCS:
		// TODO: Change GetSingleDataStore to a metadata Store?
		metadataStore := bucket.DefaultDataStore()
		client.metadata = NewDCPMetadataCS(ctx, metadataStore, numVbuckets, numWorkers, options.CheckpointPrefix)
	case DCPMetadataStoreInMemory:
		client.metadata = NewDCPMetadataMem(numVbuckets)
	default:
		return nil, fmt.Errorf("Unknown Metadatatype: %d", options.MetadataStoreType)
	}
	if options.InitialMetadata != nil {
		for vbID, meta := range options.InitialMetadata {
			client.metadata.SetMeta(uint16(vbID), meta)
		}
	}
	if len(client.collectionIDs) == 0 {
		client.collectionIDs = []uint32{DefaultCollectionID}
	}

	client.oneShot = options.OneShot

	return client, nil
}

// getCollectionHighSeqNos returns the highSeqNo for a given KV collection ID.
func (dc *GoCBDCPClient) getCollectionHighSeqNos(collectionID uint32) ([]uint64, error) {
	_, highSeqNos, err := dc.bucket.GetStatsVbSeqno(dc.numVbuckets, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get vbucket seqnos: %w", err)
	}

	result := make([]uint64, dc.numVbuckets)
	for vbID := uint16(0); vbID < dc.numVbuckets; vbID++ {
		result[vbID] = highSeqNos[vbID]
	}
	return result, nil
}

// getHighSeqNos returns the maximum sequence number for every collection configured by the DCP agent.
func (dc *GoCBDCPClient) getHighSeqNos() ([]uint64, error) {
	highSeqNos := make([]uint64, dc.numVbuckets)
	// Initialize highSeqNo to the current metadata's StartSeqNo - we don't want to use a value lower than what
	// we've already processed
	for vbNo := uint16(0); vbNo < dc.numVbuckets; vbNo++ {
		highSeqNos[vbNo] = dc.metadata.GetMeta(vbNo).StartSeqNo
	}
	for _, collectionID := range dc.collectionIDs {
		colHighSeqNos, err := dc.getCollectionHighSeqNos(collectionID)
		if err != nil {
			return nil, err
		}
		for i, colHighSeqNo := range colHighSeqNos {
			if colHighSeqNo > highSeqNos[i] {
				highSeqNos[i] = colHighSeqNo
			}
		}
	}
	return highSeqNos, nil
}

// configureOneShot sets highSeqnos for a one shot feed.
func (dc *GoCBDCPClient) configureOneShot() error {
	highSeqNos, err := dc.getHighSeqNos()
	if err != nil {
		return err
	}

	// Set endSeqNos on client metadata for use when opening streams
	endSeqNos := make(map[uint16]uint64, dc.numVbuckets)
	for vbNo, highSeqNo := range highSeqNos {
		endSeqNos[uint16(vbNo)] = highSeqNo
	}
	dc.metadata.SetEndSeqNos(endSeqNos)
	return nil
}

// Start returns an error and a channel to indicate when the DCPClient is done. If Start returns an error, DCPClient.Close() needs to be called.
func (dc *GoCBDCPClient) Start() (doneChan chan error, err error) {
	err = dc.initStreamSet()
	if err != nil {
		return dc.doneChannel, err
	}
	if dc.oneShot {
		err = dc.configureOneShot()
		if err != nil {
			return dc.doneChannel, err
		}
	}
	dc.startWorkers(dc.ctx)

	for i := uint16(0); i < dc.numVbuckets; i++ {
		openErr := dc.openStream(i, openRetryCount)
		if openErr != nil {
			return dc.doneChannel, fmt.Errorf("Unable to start DCP client, error opening stream for vb %d: %w", i, openErr)
		}
	}
	return dc.doneChannel, nil
}

// Close is used externally to stop the DCP client. If the client was already closed due to error, returns that error
func (dc *GoCBDCPClient) Close() error {
	dc.close()
	return dc.getCloseError()
}

// GetMetadata returns metadata for all vbuckets
func (dc *GoCBDCPClient) GetMetadata() []DCPMetadata {
	metadata := make([]DCPMetadata, dc.numVbuckets)
	for i := uint16(0); i < dc.numVbuckets; i++ {
		metadata[i] = dc.metadata.GetMeta(i)
	}
	return metadata
}

// close is used internally to stop the DCP client.  Sends any fatal errors to the client's done channel, and
// closes that channel.
func (dc *GoCBDCPClient) close() {

	// set dc.closing to true, avoid re-triggering close if it's already in progress
	if !dc.closing.CompareAndSwap(false, true) {
		InfofCtx(dc.ctx, KeyDCP, "DCP Client close called - client is already closing")
		return
	}

	// Stop workers
	close(dc.terminator)
	if dc.streamSet != nil {
		streamSetErr := dc.streamSet.Close()
		if streamSetErr != nil {
			WarnfCtx(dc.ctx, "Error closing DCP stream set in client close: %v", streamSetErr)
		}
	}

	// Wait for all workers to finish before closing doneChannel
	go func() {
		dc.workersWg.Wait()
		dc.doneChannel <- dc.getCloseError()
		close(dc.doneChannel)
	}()
}

// initStreamSet creates a gocbcorex DcpStreamSet via the bucket's Agent
func (dc *GoCBDCPClient) initStreamSet() error {
	agent := dc.bucket.GetAgent()

	includeXattrs, excludeValues := dcpOptionsForFeedContent(dc.ctx, dc.feedContent)

	dcpOpts := gocbcorex.KvClientDcpOptions{
		ConnectionName:     dc.dcpStreamName,
		IncludeXattrs:      includeXattrs,
		ExcludeValues:      excludeValues,
		NoopInterval:       120 * time.Second,
		EnableExpiryEvents: false,
	}

	if dc.agentPriority != "" {
		dcpOpts.Priority = dc.agentPriority
	}

	streamSet, err := agent.NewStreamSet(gocbcorex.NewStreamSetOptions{
		DcpOpts:  dcpOpts,
		Handlers: dc.buildDcpEventHandlers(),
	})
	if err != nil {
		return fmt.Errorf("Unable to start DCP client - error creating stream set: %w", err)
	}

	dc.streamSet = streamSet
	return nil
}

// buildDcpEventHandlers creates the gocbcorex.DcpEventsHandlers for the DCP client.
// This replaces the gocbcore.StreamObserver interface.
func (dc *GoCBDCPClient) buildDcpEventHandlers() gocbcorex.DcpEventsHandlers {
	return gocbcorex.DcpEventsHandlers{
		StreamOpen: func(req *memdx.DcpStreamReqResponse) {
			// Failover log is handled via the OpenVbucket return value
		},
		StreamEnd: func(req *memdx.DcpStreamEndEvent) {
			e := endStreamEvent{
				streamEventCommon: streamEventCommon{
					vbID:     req.VbucketId,
					streamID: req.StreamId,
				},
				flags: req.StreamEndFlags,
			}
			dc.workerForVbno(req.VbucketId).Send(dc.ctx, e)
		},
		SnapshotMarker: func(req *memdx.DcpSnapshotMarkerEvent) {
			e := snapshotEvent{
				streamEventCommon: streamEventCommon{
					vbID:     req.VbucketId,
					streamID: req.StreamId,
				},
				startSeq:     req.StartSeqNo,
				endSeq:       req.EndSeqNo,
				snapshotType: req.SnapshotType,
			}
			dc.workerForVbno(req.VbucketId).Send(dc.ctx, e)
		},
		Mutation: func(req *memdx.DcpMutationEvent) {
			if dc.filteredKey(req.Key) {
				return
			}

			e := mutationEvent{
				streamEventCommon: streamEventCommon{
					vbID:     req.VbucketId,
					streamID: req.StreamId,
				},
				seq:        req.SeqNo,
				revNo:      req.RevNo,
				flags:      req.Flags,
				expiry:     req.Expiry,
				cas:        req.Cas,
				datatype:   req.Datatype,
				collection: req.CollectionId,

				// The byte slices must be copied to ensure that memory associated with the underlying memd mutationEvent and Packet are independent and can be released or reused by gocbcorex as needed.
				key:   EfficientBytesClone(req.Key),
				value: EfficientBytesClone(req.Value),
			}
			dc.workerForVbno(req.VbucketId).Send(dc.ctx, e)
		},
		Deletion: func(req *memdx.DcpDeletionEvent) {
			if dc.filteredKey(req.Key) {
				return
			}

			e := deletionEvent{
				streamEventCommon: streamEventCommon{
					vbID:     req.VbucketId,
					streamID: req.StreamId,
				},
				seq:        req.SeqNo,
				cas:        req.Cas,
				revNo:      req.RevNo,
				datatype:   req.Datatype,
				collection: req.CollectionId,

				// The byte slices must be copied to ensure that memory associated with the underlying memd mutationEvent and Packet are independent and can be released or reused by gocbcorex as needed.
				key:   EfficientBytesClone(req.Key),
				value: EfficientBytesClone(req.Key), // Deletions don't carry a value body, but match original behavior
			}
			dc.workerForVbno(req.VbucketId).Send(dc.ctx, e)
		},
		Expiration: func(req *memdx.DcpExpirationEvent) {
			// SG doesn't opt in to expirations, so they'll come through as deletion events
			// (cf.https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/expiry-opcode-output.md)
			WarnfCtx(dc.ctx, "Unexpected DCP expiration event (vb:%d) for key %v", req.VbucketId, UD(string(req.Key)))
		},
		CollectionCreation: func(req *memdx.DcpCollectionCreationEvent) {
			// Not used by SG at this time
		},
		CollectionDeletion: func(req *memdx.DcpCollectionDeletionEvent) {
			// Not used by SG at this time
		},
		CollectionFlush: func(req *memdx.DcpCollectionFlushEvent) {
			// Not used by SG at this time
		},
		ScopeCreation: func(req *memdx.DcpScopeCreationEvent) {
			// Not used by SG at this time
		},
		ScopeDeletion: func(req *memdx.DcpScopeDeletionEvent) {
			// Not used by SG at this time
		},
		CollectionChanged: func(req *memdx.DcpCollectionModificationEvent) {
			// Not used by SG at this time
		},
		OSOSnapshot: func(req *memdx.DcpOSOSnapshotEvent) {
			// Not used by SG at this time
		},
		SeqNoAdvanced: func(req *memdx.DcpSeqNoAdvancedEvent) {
			dc.workerForVbno(req.VbucketId).Send(dc.ctx, seqnoAdvancedEvent{
				streamEventCommon: streamEventCommon{
					vbID:     req.VbucketId,
					streamID: req.StreamId,
				},
				seq: req.SeqNo,
			})
		},
	}
}

func (dc *GoCBDCPClient) workerForVbno(vbNo uint16) *DCPWorker {
	workerIndex := int(vbNo % uint16(len(dc.workers)))
	return dc.workers[workerIndex]
}

// startWorkers initializes the DCP workers to receive stream events from eventFeed
func (dc *GoCBDCPClient) startWorkers(ctx context.Context) {

	// vbuckets are assigned to workers as vbNo % NumWorkers.  Create set of assigned vbuckets
	assignedVbs := make(map[int][]uint16)
	for workerIndex, _ := range dc.workers {
		assignedVbs[workerIndex] = make([]uint16, 0)
	}

	for vbNo := uint16(0); vbNo < dc.numVbuckets; vbNo++ {
		workerIndex := int(vbNo % uint16(len(dc.workers)))
		assignedVbs[workerIndex] = append(assignedVbs[workerIndex], vbNo)
	}

	//
	for index, _ := range dc.workers {
		options := &DCPWorkerOptions{
			metaPersistFrequency: dc.checkpointPersistFrequency,
		}
		dc.workers[index] = NewDCPWorker(index, dc.metadata, dc.callback, dc.onStreamEnd, dc.terminator, nil, dc.checkpointPrefix, assignedVbs[index], options)
		dc.workers[index].Start(ctx, &dc.workersWg)
	}
}

func (dc *GoCBDCPClient) openStream(vbID uint16, maxRetries uint32) error {

	var openStreamErr error
	var attempts uint32
	for {
		// Cancel open for stopped client
		select {

		case <-dc.terminator:
			return nil
		default:
		}

		openStreamErr = dc.openStreamRequest(vbID)
		if openStreamErr == nil {
			return nil
		}

		var rollbackErr *memdx.DcpRollbackError
		switch {
		case errors.As(openStreamErr, &rollbackErr):
			if dc.failOnRollback {
				InfofCtx(dc.ctx, KeyDCP, "Open stream for vbID %d failed due to rollback or range error, closing client based on failOnRollback=true", vbID)
				return fmt.Errorf("%w, failOnRollback requested", openStreamErr)
			}
			InfofCtx(dc.ctx, KeyDCP, "Open stream for vbID %d failed due to rollback or range error, will roll back metadata and retry: %v", vbID, openStreamErr)

			dc.rollback(dc.ctx, vbID, rollbackErr.RollbackSeqNo)
		case errors.Is(openStreamErr, ErrVbUUIDMismatch):
			WarnfCtx(dc.ctx, "Closing Stream for vbID: %d, %s", vbID, openStreamErr)
			return openStreamErr
		case errors.Is(openStreamErr, ErrTimeout):
			InfofCtx(dc.ctx, KeyDCP, "Timeout attempting to open stream for vb %d, will retry", vbID)
		default:
			WarnfCtx(dc.ctx, "Unknown error opening stream for vbID %d: %v", vbID, openStreamErr)
		}
		if maxRetries == infiniteOpenStreamRetries {
			continue
		} else if attempts > maxRetries {
			break
		}
		attempts++
	}

	return fmt.Errorf("openStream failed to complete after %d attempts, last error: %w", attempts, openStreamErr)
}

func (dc *GoCBDCPClient) rollback(ctx context.Context, vbID uint16, seqNo uint64) {
	if dc.dbStats != nil {
		dc.dbStats.Add("dcp_rollback_count", 1)
	}
	dc.metadata.Rollback(ctx, vbID, seqNo)
}

// openStreamRequest issues the OpenVbucket request via the gocbcorex DcpStreamSet
func (dc *GoCBDCPClient) openStreamRequest(vbID uint16) error {

	vbMeta := dc.metadata.GetMeta(vbID)

	openOpts := &gocbcorex.OpenVbucketOptions{
		VbucketId:      vbID,
		Flags:          0, // DcpStreamAddFlagActiveOnly is implicit in gocbcorex
		StartSeqNo:     vbMeta.StartSeqNo,
		EndSeqNo:       vbMeta.EndSeqNo,
		VbUuid:         vbMeta.VbUUID,
		SnapStartSeqNo: vbMeta.SnapStartSeqNo,
		SnapEndSeqNo:   vbMeta.SnapEndSeqNo,
	}

	// Always use a collection-aware feed if supported
	if dc.supportsCollections {
		openOpts.CollectionIds = dc.collectionIDs
	}

	ctx, cancel := context.WithTimeout(dc.ctx, openStreamTimeout)
	defer cancel()

	resp, err := dc.streamSet.OpenVbucket(ctx, openOpts)
	if err != nil {
		return err
	}

	// Verify the failover log and update metadata
	if resp != nil {
		verifyErr := dc.verifyFailoverLog(vbID, resp.FailoverLog)
		if verifyErr != nil {
			return verifyErr
		}
		dc.metadata.SetFailoverEntries(vbID, resp.FailoverLog)
	}

	return nil
}

// verifyFailoverLog checks for VbUUID changes when failOnRollback is set, and
// writes the failover log to the client metadata store.  If previous VbUUID is zero, it's
// not considered a rollback - it's not required to initialize vbUUIDs into meta.
func (dc *GoCBDCPClient) verifyFailoverLog(vbID uint16, f []memdx.DcpFailoverEntry) error {

	if dc.failOnRollback {
		previousMeta := dc.metadata.GetMeta(vbID)
		// Cases where VbUUID and StartSeqNo aren't set aren't considered rollback
		if previousMeta.VbUUID == 0 && previousMeta.StartSeqNo == 0 {
			return nil
		}

		currentVbUUID := getLatestVbUUID(f)
		// if previousVbUUID hasn't been set yet (is zero), don't treat as rollback.
		if previousMeta.VbUUID != currentVbUUID {
			return ErrVbUUIDMismatch
		}
	}
	return nil
}

func (dc *GoCBDCPClient) deactivateVbucket(vbID uint16) {
	dc.activeVbucketLock.Lock()
	delete(dc.activeVbuckets, vbID)
	activeCount := len(dc.activeVbuckets)
	dc.activeVbucketLock.Unlock()
	if activeCount == 0 {
		dc.close()
		// On successful one-shot feed completion, purge persisted checkpoints
		if dc.oneShot {
			dc.metadata.Purge(dc.ctx, len(dc.workers))
		}
	}
}

func (dc *GoCBDCPClient) onStreamEnd(e endStreamEvent) {
	if e.flags == memdx.DcpStreamEndFlagOk {
		DebugfCtx(dc.ctx, KeyDCP, "Stream (vb:%d) closed, all items streamed", e.vbID)
		dc.deactivateVbucket(e.vbID)
		return
	}

	if e.flags == memdx.DcpStreamEndFlagClosed {
		DebugfCtx(dc.ctx, KeyDCP, "Stream (vb:%d) closed by DCPClient", e.vbID)
		dc.fatalError(fmt.Errorf("Stream (vb:%d) closed by DCPClient", e.vbID))
		return
	}

	if e.flags == memdx.DcpStreamEndFlagStateChanged || e.flags == memdx.DcpStreamEndFlagTooSlow || e.flags == memdx.DcpStreamEndFlagDisconnected {
		DebugfCtx(dc.ctx, KeyDCP, "Stream (vb:%d) ended with a known flag (%d), will reconnect", e.vbID, e.flags)
	} else {
		InfofCtx(dc.ctx, KeyDCP, "Stream (vb:%d) ended with an unknown flag (%d), will reconnect", e.vbID, e.flags)
	}
	retries := infiniteOpenStreamRetries
	if dc.oneShot {
		retries = openRetryCount
	}

	// Re-opening the stream needs to be asynchronous to avoid deadlocks
	go func(vb uint16, maxRetries uint32) {
		err := dc.openStream(vb, maxRetries)
		if err != nil {
			dc.fatalError(fmt.Errorf("Stream (vb:%d) failed to reopen: %w", vb, err))
		}
	}(e.vbID, retries)
}

func (dc *GoCBDCPClient) fatalError(err error) {
	dc.setCloseError(err)
	dc.close()
}

func (dc *GoCBDCPClient) setCloseError(err error) {
	dc.closeErrorLock.Lock()
	defer dc.closeErrorLock.Unlock()
	// If the DCPClient is already closing, don't update the error.  If an initial error triggered the close,
	// then closeError will already be set.  In the event of a requested close, we want to ignore EOF errors associated
	// with stream close
	if dc.closing.IsTrue() {
		return
	}
	if dc.closeError == nil {
		dc.closeError = err
	}
}

func (dc *GoCBDCPClient) getCloseError() error {
	dc.closeErrorLock.Lock()
	defer dc.closeErrorLock.Unlock()
	return dc.closeError
}

// getVbUUID returns the VbUUID for the given sequence in the failover log. (the most
// recent failover log entry where log.SeqNo is less than the given sequence)
func getVbUUID(failoverLog []memdx.DcpFailoverEntry, seq uint64) (vbUUID uint64) {
	for i := len(failoverLog) - 1; i >= 0; i-- {
		if failoverLog[i].SeqNo <= seq {
			return failoverLog[i].VbUuid
		}
	}
	return 0
}

// getLatestVbUUID returns the VbUUID associated with the highest sequence in the failover log
func getLatestVbUUID(failoverLog []memdx.DcpFailoverEntry) (vbUUID uint64) {
	if len(failoverLog) == 0 {
		return 0
	}
	entry := failoverLog[len(failoverLog)-1]
	return entry.VbUuid
}

func (dc *GoCBDCPClient) GetMetadataKeyPrefix() string {
	return dc.metadata.GetKeyPrefix()
}

// StartWorkersForTest will iterate through dcp workers to start them, to be used for caching testing purposes only.
func (dc *GoCBDCPClient) StartWorkersForTest(t *testing.T) {
	dc.startWorkers(dc.ctx)
}

// NewDCPClientForTest is a test-only function to create a DCP client with a specific number of vbuckets.
func NewDCPClientForTest(ctx context.Context, t *testing.T, callback sgbucket.FeedEventCallbackFunc, options DCPClientOptions, bucket *GocbV2Bucket, numVbuckets uint16) (*GoCBDCPClient, error) {
	return newDCPClientWithForBuckets(ctx, callback, options, bucket, numVbuckets)
}

func (dc *GoCBDCPClient) filteredKey(key []byte) bool {
	return false
}

// SendMutationForTest creates a mutationEvent and sends it directly to the appropriate worker.
// This is used by performance testing tools that generate synthetic DCP events.
func (dc *GoCBDCPClient) SendMutationForTest(vbID uint16, seq uint64, key []byte, value []byte, cas uint64, datatype uint8) {
	e := mutationEvent{
		streamEventCommon: streamEventCommon{
			vbID:     vbID,
			streamID: vbID,
		},
		seq:      seq,
		revNo:    1,
		flags:    0,
		expiry:   0,
		cas:      cas,
		datatype: datatype,
		key:      key,
		value:    value,
	}
	dc.workerForVbno(vbID).Send(dc.ctx, e)
}
