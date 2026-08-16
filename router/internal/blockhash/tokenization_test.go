package blockhash

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

type tokenizationVectorFile struct {
	Algorithm         string               `json:"algorithm"`
	ConfigHash        string               `json:"config_hash"`
	TokenizerRevision string               `json:"tokenizer_revision"`
	Vectors           []tokenizationVector `json:"vectors"`
}

type tokenizationVector struct {
	Name              string       `json:"name"`
	Prompt            string       `json:"prompt"`
	TokenizerRevision string       `json:"tokenizer_revision"`
	ExpectedUnits     []string     `json:"expected_units"`
	ExpectedTokens    []uint64     `json:"expected_tokens"`
	BlockSizeTokens   int          `json:"block_size_tokens"`
	IsolationKey      IsolationKey `json:"isolation_key"`
	ExpectedHashes    []string     `json:"expected_hashes"`
}

func TestTokenizationContractVectors(t *testing.T) {
	vectors := loadTokenizationVectors(t)
	if vectors.Algorithm != ApproxTokenizationAlgorithm {
		t.Fatalf("algorithm mismatch: got %s want %s", vectors.Algorithm, ApproxTokenizationAlgorithm)
	}
	if vectors.ConfigHash != ApproxTokenizerConfigHash() {
		t.Fatalf("config hash mismatch: got %s want %s", vectors.ConfigHash, ApproxTokenizerConfigHash())
	}
	if vectors.TokenizerRevision != ApproxTokenizerRevision {
		t.Fatalf("tokenizer revision mismatch: got %s want %s", vectors.TokenizerRevision, ApproxTokenizerRevision)
	}
	for _, vector := range vectors.Vectors {
		t.Run(vector.Name, func(t *testing.T) {
			units := ApproxTokenUnits(vector.Prompt)
			if !reflect.DeepEqual(units, vector.ExpectedUnits) {
				t.Fatalf("units mismatch: got %#v want %#v", units, vector.ExpectedUnits)
			}
			tokens := ApproxTokenizeWithRevision(vector.Prompt, vector.TokenizerRevision)
			if !reflect.DeepEqual(tokens, vector.ExpectedTokens) {
				t.Fatalf("tokens mismatch: got %#v want %#v", tokens, vector.ExpectedTokens)
			}
			hashes := New(vector.BlockSizeTokens).ComputeBlocks(tokens, vector.IsolationKey)
			if !reflect.DeepEqual(hashes, vector.ExpectedHashes) {
				t.Fatalf("hashes mismatch: got %#v want %#v", hashes, vector.ExpectedHashes)
			}
		})
	}
}

func TestTokenizerRevisionChangesBlockHashNamespace(t *testing.T) {
	vectors := loadTokenizationVectors(t)
	var base *tokenizationVector
	var other *tokenizationVector
	for i := range vectors.Vectors {
		switch vectors.Vectors[i].Name {
		case "basic_ascii":
			base = &vectors.Vectors[i]
		case "different_tokenizer_revision":
			other = &vectors.Vectors[i]
		}
	}
	if base == nil || other == nil {
		t.Fatal("missing tokenizer revision vectors")
	}
	if base.Prompt != other.Prompt {
		t.Fatal("revision guard vectors must use the same prompt")
	}
	if reflect.DeepEqual(base.ExpectedTokens, other.ExpectedTokens) {
		t.Fatal("token ids did not change when tokenizer revision changed")
	}
	if base.ExpectedHashes[0] == other.ExpectedHashes[0] {
		t.Fatal("block hash namespace did not change when tokenizer revision changed")
	}
}

func loadTokenizationVectors(t *testing.T) tokenizationVectorFile {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		candidate := filepath.Join(dir, "..", "..", "..", "shared", "fixtures", "tokenization_vectors.json")
		if _, err := os.Stat(candidate); err == nil {
			raw, readErr := os.ReadFile(candidate)
			if readErr != nil {
				t.Fatal(readErr)
			}
			var vectors tokenizationVectorFile
			if err := json.Unmarshal(raw, &vectors); err != nil {
				t.Fatal(err)
			}
			return vectors
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate shared/fixtures/tokenization_vectors.json")
		}
		dir = parent
	}
}
