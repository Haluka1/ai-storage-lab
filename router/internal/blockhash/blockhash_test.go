package blockhash

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type vectorFile struct {
	Algorithm string       `json:"algorithm"`
	Vectors   []testVector `json:"vectors"`
}

type testVector struct {
	Name            string       `json:"name"`
	BlockSizeTokens int          `json:"block_size_tokens"`
	Tokens          []uint64     `json:"tokens"`
	IsolationKey    IsolationKey `json:"isolation_key"`
	ExpectedHashes  []string     `json:"expected_hashes"`
}

func TestBlockHashVectors(t *testing.T) {
	vectors := loadVectors(t)
	for _, vector := range vectors.Vectors {
		t.Run(vector.Name, func(t *testing.T) {
			got := New(vector.BlockSizeTokens).ComputeBlocks(vector.Tokens, vector.IsolationKey)
			if len(got) != len(vector.ExpectedHashes) {
				t.Fatalf("hash count mismatch: got %d want %d", len(got), len(vector.ExpectedHashes))
			}
			for i := range got {
				if got[i] != vector.ExpectedHashes[i] {
					t.Fatalf("hash[%d] mismatch: got %s want %s", i, got[i], vector.ExpectedHashes[i])
				}
			}
		})
	}
}

func TestIsolationChangesHash(t *testing.T) {
	vectors := loadVectors(t)
	base := vectors.Vectors[0].ExpectedHashes[0]
	for _, name := range []string{"different_tenant", "different_model_revision", "different_tokenizer", "different_lora", "different_modality"} {
		var found *testVector
		for i := range vectors.Vectors {
			if vectors.Vectors[i].Name == name {
				found = &vectors.Vectors[i]
				break
			}
		}
		if found == nil {
			t.Fatalf("missing vector %s", name)
		}
		if found.ExpectedHashes[0] == base {
			t.Fatalf("%s did not change first block hash", name)
		}
	}
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		candidate := filepath.Join(dir, "..", "..", "..", "shared", "fixtures", "blockhash_vectors.json")
		if _, err := os.Stat(candidate); err == nil {
			raw, readErr := os.ReadFile(candidate)
			if readErr != nil {
				t.Fatal(readErr)
			}
			var vectors vectorFile
			if err := json.Unmarshal(raw, &vectors); err != nil {
				t.Fatal(err)
			}
			return vectors
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate shared/fixtures/blockhash_vectors.json")
		}
		dir = parent
	}
}
