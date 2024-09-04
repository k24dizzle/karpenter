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

package disruption

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/samber/lo"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

const (
	NodePoolDrifted     cloudprovider.DriftReason = "NodePoolDrifted"
	RequirementsDrifted cloudprovider.DriftReason = "RequirementsDrifted"
)

// Drift is a nodeclaim sub-controller that adds or removes status conditions on drifted nodeclaims
type Drift struct {
	cloudProvider cloudprovider.CloudProvider
}

func (d *Drift) Reconcile(ctx context.Context, nodePool *v1.NodePool, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	hasDriftedCondition := nodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted) != nil

	// From here there are three scenarios to handle:
	// 1. If NodeClaim is not launched, remove the drift status condition
	if !nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched).IsTrue() {
		_ = nodeClaim.StatusConditions().Clear(v1.ConditionTypeDrifted)
		if hasDriftedCondition {
			log.FromContext(ctx).V(1).Info("removing drift status condition, isn't launched")
		}
		return reconcile.Result{}, nil
	}
	driftedReason, err := d.isDrifted(ctx, nodePool, nodeClaim)
	if err != nil {
		return reconcile.Result{}, cloudprovider.IgnoreNodeClaimNotFoundError(fmt.Errorf("getting drift, %w", err))
	}
	// 2. Otherwise, if the NodeClaim isn't drifted, but has the status condition, remove it.
	if driftedReason == "" {
		if hasDriftedCondition {
			_ = nodeClaim.StatusConditions().Clear(v1.ConditionTypeDrifted)
			log.FromContext(ctx).V(1).Info("removing drifted status condition, not drifted")
		}
		return reconcile.Result{RequeueAfter: 5 * time.Minute}, nil
	}
	// 3. Finally, if the NodeClaim is drifted, but doesn't have status condition, add it.
	log.FromContext(ctx).Info(fmt.Sprintf("[kevin] [nodeclaim disruption] nodeClaim %s is drifted (%t) from nodePool %s", nodeClaim.Name, driftedReason, nodePool.Name))
	nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDrifted, string(driftedReason), string(driftedReason))
	if !hasDriftedCondition {
		log.FromContext(ctx).V(1).WithValues("reason", string(driftedReason)).Info("marking drifted")
	}
	// Requeue after 5 minutes for the cache TTL
	return reconcile.Result{RequeueAfter: 5 * time.Minute}, nil
}

// isDrifted will check if a NodeClaim is drifted from the fields in the NodePool Spec and the CloudProvider
func (d *Drift) isDrifted(ctx context.Context, nodePool *v1.NodePool, nodeClaim *v1.NodeClaim) (cloudprovider.DriftReason, error) {
	// First check for static drift or node requirements have drifted to save on API calls.
	if reason := lo.FindOrElse([]cloudprovider.DriftReason{areStaticFieldsDrifted(nodePool, nodeClaim), areRequirementsDrifted(nodePool, nodeClaim)}, "", func(i cloudprovider.DriftReason) bool {
		return i != ""
	}); reason != "" {
		return reason, nil
	}
	driftedReason, err := d.cloudProvider.IsDrifted(ctx, nodeClaim)
	if err != nil {
		return "", err
	}
	return driftedReason, nil
}

// Eligible fields for drift are described in the docs
// https://karpenter.sh/docs/concepts/deprovisioning/#drift
func areStaticFieldsDrifted(nodePool *v1.NodePool, nodeClaim *v1.NodeClaim) cloudprovider.DriftReason {
	nodePoolHash, foundNodePoolHash := nodePool.Annotations[v1.NodePoolHashAnnotationKey]
	nodePoolHashVersion, foundNodePoolHashVersion := nodePool.Annotations[v1.NodePoolHashVersionAnnotationKey]
	nodeClaimHash, foundNodeClaimHash := nodeClaim.Annotations[v1.NodePoolHashAnnotationKey]
	nodeClaimHashVersion, foundNodeClaimHashVersion := nodeClaim.Annotations[v1.NodePoolHashVersionAnnotationKey]

	if !foundNodePoolHash || !foundNodePoolHashVersion || !foundNodeClaimHash || !foundNodeClaimHashVersion {
		return ""
	}
	// validate that the hash version on the NodePool is the same as the NodeClaim before evaluating for static drift
	if nodePoolHashVersion != nodeClaimHashVersion {
		return ""
	}
	return lo.Ternary(nodePoolHash != nodeClaimHash, NodePoolDrifted, "")
}

func areRequirementsDrifted(nodePool *v1.NodePool, nodeClaim *v1.NodeClaim) cloudprovider.DriftReason {
	nodepoolReq := scheduling.NewNodeSelectorRequirementsWithMinValues(nodePool.Spec.Template.Spec.Requirements...)
	nodeClaimReq := scheduling.NewLabelRequirements(nodeClaim.Labels)

	// Every nodepool requirement is compatible with the NodeClaim label set
	if nodeClaimReq.Compatible(nodepoolReq) != nil {
		return RequirementsDrifted
	}

	return ""
}
