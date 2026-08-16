package blockhash

import (
	"crypto/sha256"
	"encoding/binary"
	"unicode"
)

const ApproxTokenizationAlgorithm = "approx-tokenizer-sha256-units-v1"
const ApproxTokenizerRevision = "approx-tokenizer-v1:config-sha256=bae1e25ad3aa298a"
const ApproxTokenizerScope = "router_runtime_approximation"

func ApproxTokenize(prompt string) []uint64 {
	return ApproxTokenizeWithRevision(prompt, ApproxTokenizerRevision)
}

func ApproxTokenizeWithRevision(prompt string, tokenizerRevision string) []uint64 {
	units := ApproxTokenUnits(prompt)
	tokens := make([]uint64, 0, len(units))
	for _, unit := range units {
		tokens = append(tokens, ApproxTokenID(unit, tokenizerRevision))
	}
	return tokens
}

func ApproxTokenUnits(prompt string) []string {
	runes := []rune(prompt)
	units := make([]string, 0, len(runes))
	for i := 0; i < len(runes); {
		ch := runes[i]
		if unicode.IsSpace(ch) {
			i++
			continue
		}
		if isASCIIWord(ch) {
			start := i
			i++
			for i < len(runes) && isASCIIWord(runes[i]) {
				i++
			}
			units = append(units, string(runes[start:i]))
			continue
		}
		units = append(units, string(ch))
		i++
	}
	return units
}

func ApproxTokenID(unit string, tokenizerRevision string) uint64 {
	payload := []byte(tokenizerRevision)
	payload = append(payload, 0)
	payload = append(payload, []byte(unit)...)
	digest := sha256.Sum256(payload)
	value := binary.BigEndian.Uint64(digest[:8])
	return value%1000000 + 1
}

func ApproxTokenizerConfigHash() string {
	return "bae1e25ad3aa298a"
}

func isASCIIWord(ch rune) bool {
	return ch == '_' || ('0' <= ch && ch <= '9') || ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z')
}
