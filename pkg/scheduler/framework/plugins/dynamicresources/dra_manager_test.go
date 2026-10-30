/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicresources

import (
	"maps"
	"testing"

	"github.com/onsi/gomega"
	resourceapi "k8s.io/api/resource/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/dynamic-resource-allocation/structured/schedulerapi"
	"k8s.io/kubernetes/pkg/features"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	claimUID      = types.UID("claim-uid-1")
	otherClaimUID = types.UID("claim-uid-2")
)

// testInformer implements [assumecache.Informer] and can be used to feed changes into an assume
// cache during unit testing. Only a single event handler is supported, which is
// sufficient for one assume cache.
type testInformer struct {
	handler cache.ResourceEventHandler
}

func (i *testInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	i.handler = handler
	return nil, nil
}

func (i *testInformer) add(obj any) {
	if i.handler == nil {
		return
	}
	i.handler.OnAdd(obj, false)
}

func TestSignalClaimPendingAllocation(t *testing.T) {
	testSignalClaimPendingAllocation(ktesting.Init(t))
}
func testSignalClaimPendingAllocation(tCtx ktesting.TContext) {
	specialAllocatedClaim := allocatedClaim.DeepCopy()
	specialAllocatedClaim.Name = specialClaimInMemName

	tests := map[string]struct {
		inFlightAllocations         map[types.UID]inFlightAllocation
		cachedClaims                []*resourceapi.ResourceClaim
		claimUID                    types.UID
		allocatedClaim              *resourceapi.ResourceClaim
		expectedInFlightAllocations map[types.UID]inFlightAllocation
	}{
		"empty": {
			cachedClaims: []*resourceapi.ResourceClaim{
				st.FromResourceClaim(pendingClaim).UID(string(claimUID)).Obj(),
			},
			claimUID:       claimUID,
			allocatedClaim: allocatedClaim,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID: {claim: allocatedClaim, sharers: 1},
			},
		},
		"claim-already-allocated": {
			claimUID: claimUID,
			cachedClaims: []*resourceapi.ResourceClaim{
				st.FromResourceClaim(allocatedClaim).UID(string(claimUID)).Obj(),
			},
			allocatedClaim:              allocatedClaim,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{},
		},
		"extended-resource-claim-not-in-cache": {
			claimUID:       claimUID,
			allocatedClaim: specialAllocatedClaim,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID: {claim: specialAllocatedClaim, sharers: 1},
			},
		},
		"already-exists": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 1},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
			claimUID:       claimUID,
			allocatedClaim: allocatedClaim,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 2},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
		},
		"already-exists-ignores-different-claim-argument": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID: {claim: allocatedClaim, sharers: 1},
			},
			claimUID:       claimUID,
			allocatedClaim: allocatedClaim2,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID: {claim: allocatedClaim, sharers: 2},
			},
		},
	}

	for name, test := range tests {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			c := &claimTracker{
				logger:              tCtx.Logger(),
				inFlightAllocations: make(map[types.UID]inFlightAllocation),
			}
			maps.Copy(c.inFlightAllocations, test.inFlightAllocations)

			informer := &testInformer{}
			c.cache = assumecache.NewAssumeCache(tCtx.Logger(), informer, "", "", nil)
			for _, cached := range test.cachedClaims {
				informer.add(cached)
			}

			err := c.SignalClaimPendingAllocation(test.claimUID, test.allocatedClaim)
			tCtx.ExpectNoError(err)
			tCtx.Expect(c.inFlightAllocations).To(gomega.Equal(test.expectedInFlightAllocations))
		})
	}
}

func TestGetPendingAllocation(t *testing.T) {
	testGetPendingAllocation(ktesting.Init(t))
}
func testGetPendingAllocation(tCtx ktesting.TContext) {
	tests := map[string]struct {
		inFlightAllocations map[types.UID]inFlightAllocation
		claimUID            types.UID
		expected            *resourceapi.AllocationResult
	}{
		"empty": {
			claimUID: claimUID,
			expected: nil,
		},
		"nil-claim": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID: {claim: nil},
			},
			claimUID: claimUID,
			expected: nil,
		},
		"claim": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 1},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
			claimUID: claimUID,
			expected: allocationResult,
		},
	}

	for name, test := range tests {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			c := &claimTracker{
				logger:              tCtx.Logger(),
				inFlightAllocations: make(map[types.UID]inFlightAllocation),
			}
			maps.Copy(c.inFlightAllocations, test.inFlightAllocations)
			beforeInFlight := maps.Clone(c.inFlightAllocations)

			actual := c.GetPendingAllocation(test.claimUID)
			tCtx.Expect(actual).To(gomega.Equal(test.expected))
			// Get is strictly read-only
			tCtx.Expect(c.inFlightAllocations).To(gomega.Equal(beforeInFlight))
		})
	}
}

func TestMaybeRemoveClaimPendingAllocation(t *testing.T) {
	testMaybeRemoveClaimPendingAllocation(ktesting.Init(t))
}
func testMaybeRemoveClaimPendingAllocation(tCtx ktesting.TContext) {
	tests := map[string]struct {
		inFlightAllocations         map[types.UID]inFlightAllocation
		claimUID                    types.UID
		forceRemove                 bool
		expected                    bool
		expectedInFlightAllocations map[types.UID]inFlightAllocation
	}{
		"empty": {
			claimUID:                    claimUID,
			forceRemove:                 true,
			expected:                    false,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{},
		},
		"delete-last-sharer": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 1},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
			claimUID: claimUID,
			expected: true,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
		},
		"force-delete-last-sharer": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 1},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
			claimUID:    claimUID,
			forceRemove: true,
			expected:    true,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
		},
		"decrement-remaining-sharers": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 2},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
			claimUID: claimUID,
			expected: false,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 1},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
		},
		"force-delete-remaining-sharers": {
			inFlightAllocations: map[types.UID]inFlightAllocation{
				claimUID:      {claim: allocatedClaim, sharers: 2},
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
			claimUID:    claimUID,
			forceRemove: true,
			expected:    true,
			expectedInFlightAllocations: map[types.UID]inFlightAllocation{
				otherClaimUID: {claim: allocatedClaim2, sharers: 10},
			},
		},
	}

	for name, test := range tests {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			c := &claimTracker{
				logger:              tCtx.Logger(),
				inFlightAllocations: make(map[types.UID]inFlightAllocation),
			}
			maps.Copy(c.inFlightAllocations, test.inFlightAllocations)

			actual := c.MaybeRemoveClaimPendingAllocation(test.claimUID, test.forceRemove)
			tCtx.Expect(actual).To(gomega.Equal(test.expected), "wrong value for deletion indicator")
			tCtx.Expect(c.inFlightAllocations).To(gomega.Equal(test.expectedInFlightAllocations))
		})
	}
}

// TestGatherAllocatedStateWhileWaitingForBindingConditions follows a claim
// with consumable capacity through the claim tracker the way Reserve,
// PreBind (bindClaim) and the claim informer update it. While PreBind waits
// for binding conditions, the allocation is both stored in the assume cache
// and in flight. The capacity which it consumes must be counted once,
// otherwise the device looks fuller than it is and other claims which
// would fit are rejected until PreBind is done.
func TestGatherAllocatedStateWhileWaitingForBindingConditions(t *testing.T) {
	testGatherAllocatedStateWhileWaitingForBindingConditions(ktesting.Init(t))
}
func testGatherAllocatedStateWhileWaitingForBindingConditions(tCtx ktesting.TContext) {
	featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, true)

	informer := &testInformer{}
	c := &claimTracker{
		cache:               assumecache.NewAssumeCache(tCtx.Logger(), informer, "ResourceClaim", "", nil),
		inFlightAllocations: make(map[types.UID]inFlightAllocation),
		allocatedDevices:    newAllocatedDevices(tCtx.Logger()),
		logger:              tCtx.Logger(),
	}
	c.cache.AddEventHandler(c.allocatedDevices.handlers())

	pending := st.FromResourceClaim(pendingClaim).UID(string(claimUID)).Obj()
	pending.ResourceVersion = "1"
	// What Reserve signals as pending allocation: the claim with the
	// allocation, not stored yet.
	reserved := st.FromResourceClaim(pending).Allocation(allocationResultWithConsumedCapacity).Obj()
	// What bindClaim gets back from the apiserver and assumes.
	allocated := reserved.DeepCopy()
	allocated.ResourceVersion = "2"
	deallocated := pending.DeepCopy()
	deallocated.ResourceVersion = "3"

	deviceID := schedulerapi.MakeDeviceID(driver, nodeName, sharedDeviceName)
	expectConsumed := func(phase string, expected *apiresource.Quantity) {
		tCtx.Helper()
		state, err := c.GatherAllocatedState()
		tCtx.ExpectNoError(err, phase)
		capacity, found := state.AggregatedCapacity[deviceID]
		if expected == nil {
			tCtx.Expect(found).To(gomega.BeFalseBecause("%s: no capacity of %s should be consumed, got %v", phase, deviceID, capacity))
			return
		}
		tCtx.Expect(found).To(gomega.BeTrueBecause("%s: capacity of %s should be consumed", phase, deviceID))
		tCtx.Expect(capacity[capacityName]).ToNot(gomega.BeNil(), phase)
		tCtx.Expect(capacity[capacityName].Cmp(*expected)).To(gomega.Equal(0), "%s: consumed %s: got %s, want %s", phase, capacityName, capacity[capacityName], expected)
	}
	one := apiresource.MustParse("1")

	informer.add(pending)
	expectConsumed("pending", nil)

	// Reserve.
	tCtx.ExpectNoError(c.SignalClaimPendingAllocation(claimUID, reserved))
	expectConsumed("in flight", &one)

	// bindClaim stored the allocation, PreBind waits for binding conditions.
	tCtx.ExpectNoError(c.AssumeClaimAfterAPICall(allocated))
	expectConsumed("in flight and allocated", &one)
	informer.add(allocated)
	expectConsumed("in flight and allocated, informer caught up", &one)

	// PreBind succeeded.
	tCtx.Expect(c.MaybeRemoveClaimPendingAllocation(claimUID, true)).To(gomega.BeTrueBecause("the in-flight allocation should get removed"))
	expectConsumed("allocated", &one)

	informer.add(deallocated)
	expectConsumed("deallocated", nil)
}
