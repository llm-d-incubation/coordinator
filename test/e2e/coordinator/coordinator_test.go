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

package coordinate2e

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	testutils "github.com/llm-d/llm-d-router/test/utils"
)

const requestTimeout = 60 * time.Second

var _ = ginkgo.Describe("Coordinator pipeline", func() {
	ginkgo.It("routes a chat completion end-to-end", func() {
		// Pools first so each EPP can resolve its --pool-name.
		encodePool := createInferencePool("encode", true)
		prefillPool := createInferencePool("prefill", true)
		decodePool := createInferencePool("decode", true)
		expectAllPoolsExist()

		encodeEPP := createEndPointPicker("encode", encodeEPPConfig)
		prefillEPP := createEndPointPicker("prefill", prefillEPPConfig)
		decodeEPP := createEndPointPicker("decode", decodeEPPConfig)

		encodeReplicas, prefillReplicas, decodeReplicas := 1, 1, 1
		modelServers := createModelServers(encodeReplicas, prefillReplicas, decodeReplicas)

		encodePods := getPodNames(encodeSelector)
		prefillPods := getPodNames(prefillSelector)
		decodePods := getPodNames(decodeSelector)
		gomega.Expect(encodePods).Should(gomega.HaveLen(encodeReplicas))
		gomega.Expect(prefillPods).Should(gomega.HaveLen(prefillReplicas))
		gomega.Expect(decodePods).Should(gomega.HaveLen(decodeReplicas))

		coordinator := createCoordinator(simpleConfig)

		body := []byte(fmt.Sprintf(
			`{"model":%q,"messages":[{"role":"user","content":"hello"}]}`,
			modelName,
		))

		req, err := http.NewRequest(http.MethodPost,
			coordinatorBaseURL+"/v1/chat/completions",
			bytes.NewReader(body))
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: requestTimeout}
		resp, err := client.Do(req)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		defer resp.Body.Close()

		raw, err := io.ReadAll(resp.Body)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK),
			"coordinator returned non-200: body=%s", string(raw))
		gomega.Expect(raw).NotTo(gomega.BeEmpty(), "coordinator returned empty body")

		testutils.DeleteObjects(testConfig, coordinator)
		testutils.DeleteObjects(testConfig, modelServers)
		testutils.DeleteObjects(testConfig, decodeEPP)
		testutils.DeleteObjects(testConfig, prefillEPP)
		testutils.DeleteObjects(testConfig, encodeEPP)
		testutils.DeleteObjects(testConfig, decodePool)
		testutils.DeleteObjects(testConfig, prefillPool)
		testutils.DeleteObjects(testConfig, encodePool)
	})
})
