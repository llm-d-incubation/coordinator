// Package e2eutil holds helpers shared across coordinator e2e test suites.
package e2eutil

import (
	"k8s.io/apimachinery/pkg/runtime"
	inferenceapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"
)

// AddInferenceScheme registers gateway-api-inference-extension types (e.g.
// InferencePool) into the supplied scheme so that e2e tests can create and
// inspect those objects via the dynamic client.
func AddInferenceScheme(scheme *runtime.Scheme) error {
	return inferenceapi.Install(scheme)
}
