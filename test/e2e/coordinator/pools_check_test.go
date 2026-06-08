package coordinate2e

import (
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	inferenceapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
)

// expectedPools enumerates the three phase-specific InferencePools the
// e-p-d-pools topology brings up. Their existence is the single hard signal
// that the env wired correctly: every other route in the pipeline depends on
// them. Names derive from poolNameBase (e.g. qwen3-vl-2b-instruct-inference-pool).
func expectedPools() []string {
	return []string{
		poolNameBase + "-encode",
		poolNameBase + "-prefill",
		poolNameBase + "-decode",
	}
}

// expectAllPoolsExist asserts that the encode, prefill, and decode
// InferencePools exist in the test namespace.
func expectAllPoolsExist() {
	for _, name := range expectedPools() {
		pool := &inferenceapi.InferencePool{}
		key := types.NamespacedName{Namespace: testConfig.NsName, Name: name}
		gomega.Eventually(func() error {
			return testConfig.K8sClient.Get(testConfig.Context, key, pool)
		}, readyTimeout, defaultInterval).Should(
			gomega.Succeed(),
			"InferencePool %q not found in namespace %q", name, testConfig.NsName,
		)
	}
}
