package gateway

import "strings"

const (
	DefaultGeneratePath = "/inference/v1/generate"

	EPPPhaseHeader = "EPP-Phase"

	PhaseEncode  = "encode"
	PhasePrefill = "prefill"
	PhaseDecode  = "decode"
)

type RequestFormat int

const (
	FormatGenerate RequestFormat = iota
	FormatCompletions
	FormatChatCompletions
)

func DetectFormat(path string) RequestFormat {
	if strings.Contains(path, "/v1/chat/completions") {
		return FormatChatCompletions
	}
	if strings.Contains(path, "/v1/completions") {
		return FormatCompletions
	}
	return FormatGenerate
}

func PathForFormat(format RequestFormat) string {
	switch format {
	case FormatChatCompletions:
		return "/v1/chat/completions"
	case FormatCompletions:
		return "/v1/completions"
	default:
		return DefaultGeneratePath
	}
}
