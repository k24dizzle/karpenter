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

package scheduling

import (
	"fmt"

	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// debugLogEnabled controls whether debug logging is enabled for TSC troubleshooting
var debugLogEnabled = true

func debugLog(format string, args ...interface{}) {
	if debugLogEnabled {
		fmt.Printf("[DEBUG-TSC] "+format+"\n", args...)
	}
}

// TopologyNodeFilter is used to determine if a given actual node or scheduling node matches the pod's node selectors
// and required node affinity terms.  This is used with topology spread constraints to determine if the node should be
// included for topology counting purposes. This is only used with topology spread constraints as affinities/anti-affinities
// always count across all nodes. A nil or zero-value TopologyNodeFilter behaves well and the filter returns true for
// all nodes.
type TopologyNodeFilter struct {
	Requirements   []scheduling.Requirements
	TaintPolicy    corev1.NodeInclusionPolicy
	AffinityPolicy corev1.NodeInclusionPolicy
	Tolerations    []corev1.Toleration
}

// MakeTopologyNodeFilter creates a filter for TSC counting based on pod's NodeSelector and NodeAffinity.
//
// Since volume requirements are no longer injected into the pod's NodeAffinity, this function
// now simply processes the pod's ORIGINAL affinity. No truncation or workaround needed.
func MakeTopologyNodeFilter(p *corev1.Pod, taintPolicy corev1.NodeInclusionPolicy, affinityPolicy corev1.NodeInclusionPolicy) TopologyNodeFilter {
	debugLog("=== MakeTopologyNodeFilter for pod %s/%s ===", p.Namespace, p.Name)
	debugLog("  TaintPolicy: %s, AffinityPolicy: %s", taintPolicy, affinityPolicy)

	nodeSelectorRequirements := scheduling.NewLabelRequirements(p.Spec.NodeSelector)
	debugLog("  NodeSelector requirements: %v", nodeSelectorRequirements)

	// If no NodeAffinity exists, just use NodeSelector
	if p.Spec.Affinity == nil || p.Spec.Affinity.NodeAffinity == nil || p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		debugLog("  No NodeAffinity.Required found, using only NodeSelector")
		return TopologyNodeFilter{
			Requirements:   []scheduling.Requirements{nodeSelectorRequirements},
			TaintPolicy:    taintPolicy,
			AffinityPolicy: affinityPolicy,
			Tolerations:    p.Spec.Tolerations,
		}
	}

	filter := TopologyNodeFilter{
		TaintPolicy:    taintPolicy,
		AffinityPolicy: affinityPolicy,
		Tolerations:    p.Spec.Tolerations,
	}

	numTerms := len(p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms)
	debugLog("  NodeAffinity.Required has %d terms", numTerms)

	for i, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		debugLog("  Term %d: %d MatchExpressions", i, len(term.MatchExpressions))
		for j, expr := range term.MatchExpressions {
			debugLog("    MatchExpression[%d]: key=%s, op=%s, values=%v", j, expr.Key, expr.Operator, expr.Values)
		}

		requirements := scheduling.NewRequirements()
		requirements.Add(nodeSelectorRequirements.Values()...)
		requirements.Add(scheduling.NewNodeSelectorRequirements(term.MatchExpressions...).Values()...)
		filter.Requirements = append(filter.Requirements, requirements)
	}

	debugLog("  Final filter has %d requirement sets", len(filter.Requirements))
	return filter
}

// Matches returns true if the TopologyNodeFilter doesn't prohibit node from the participating in the topology
func (t TopologyNodeFilter) Matches(taints []corev1.Taint, requirements scheduling.Requirements, compatibilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	matchesAffinity := true
	if t.AffinityPolicy == corev1.NodeInclusionPolicyHonor {
		matchesAffinity = t.matchesRequirements(requirements)
	}
	matchesTaints := true
	if t.TaintPolicy == corev1.NodeInclusionPolicyHonor {
		if err := scheduling.Taints(taints).Tolerates(t.Tolerations); err != nil {
			matchesTaints = false
		}
	}
	result := matchesAffinity && matchesTaints
	if !result {
		debugLog("TopologyNodeFilter.Matches: REJECTED - matchesAffinity=%v, matchesTaints=%v", matchesAffinity, matchesTaints)
	}
	return result
}

// MatchesRequirements returns true if the TopologyNodeFilter doesn't prohibit a node with the requirements from
// participating in the topology. This method allows checking the requirements from a scheduling.NodeClaim to see if the
// node we will soon create participates in this topology.
func (t TopologyNodeFilter) matchesRequirements(requirements scheduling.Requirements, compatabilityOptions ...option.Function[scheduling.CompatibilityOptions]) bool {
	// no requirements, so it always matches
	if len(t.Requirements) == 0 || t.AffinityPolicy == corev1.NodeInclusionPolicyIgnore {
		return true
	}
	// these are an OR, so if any passes the filter passes
	for _, req := range t.Requirements {
		if err := requirements.Compatible(req, compatabilityOptions...); err == nil {
			return true
		}
	}
	return false
}
