/*
Copyright 2026 The llm-d Authors.

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

package agentgateway

import (
	"os/exec"
	"strings"
	"testing"
)

func TestAgentgatewayOverlayRoutesPhasesAndCoordinator(t *testing.T) {
	rendered := renderOverlay(t)

	for _, want := range []string{
		"kind: Gateway",
		"name: ${GATEWAY_NAME}",
		"gatewayClassName: agentgateway",
		"kind: ConfigMap",
		"name: llm-d-coordinator-config",
		`address: "http://inference-gateway:80"`,
		"kind: HTTPRoute",
		"name: ${GATEWAY_NAME}-coordinator",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered overlay missing %q:\n%s", want, rendered)
		}
	}

	route := renderedRoute(t, rendered)
	if count := strings.Count(route, "name: EPP-Phase"); count != 3 {
		t.Fatalf("route has %d EPP-Phase matches, want 3:\n%s", count, route)
	}
	for phase, backend := range map[string]string{
		"encode":  "${POOL_NAME}-encode",
		"prefill": "${POOL_NAME}-prefill",
		"decode":  "${POOL_NAME}-decode",
	} {
		assertRuleContains(t, route, "value: "+phase, "kind: InferencePool", "name: "+backend)
	}

	assertRuleContains(t, route, "kind: Service", "name: llm-d-coordinator", "port: 8080")
}

func renderOverlay(t *testing.T) string {
	t.Helper()

	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("kubectl not installed")
	}
	out, err := exec.Command(kubectl, "kustomize", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("rendering kustomize overlay: %v\n%s", err, string(out))
	}
	return string(out)
}

func renderedRoute(t *testing.T, rendered string) string {
	t.Helper()
	marker := "kind: HTTPRoute"
	start := strings.Index(rendered, marker)
	if start == -1 {
		t.Fatalf("rendered overlay missing %q:\n%s", marker, rendered)
	}
	return rendered[start:]
}

func assertRuleContains(t *testing.T, route string, snippets ...string) {
	t.Helper()
	for _, rule := range strings.Split(route, "\n  - backendRefs:") {
		matched := true
		for _, snippet := range snippets {
			if !strings.Contains(rule, snippet) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	t.Fatalf("no HTTPRoute rule contains %q:\n%s", strings.Join(snippets, ", "), route)
}
