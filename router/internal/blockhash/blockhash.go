package blockhash

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
)

const EmptyParent = "0000000000000000000000000000000000000000000000000000000000000000"

type IsolationKey struct {
	TenantID          string `json:"tenant_id"`
	TenantSalt        string `json:"tenant_salt"`
	ModelID           string `json:"model_id"`
	ModelRevision     string `json:"model_revision"`
	TokenizerRevision string `json:"tokenizer_revision"`
	LoRAID            string `json:"lora_id"`
	ModalityKey       string `json:"modality_key"`
	CacheSalt         string `json:"cache_salt"`
}

type Hasher struct {
	BlockSizeTokens int
}

func New(blockSizeTokens int) *Hasher {
	if blockSizeTokens <= 0 {
		blockSizeTokens = 16
	}
	return &Hasher{BlockSizeTokens: blockSizeTokens}
}

func (h *Hasher) ComputeBlocks(tokens []uint64, key IsolationKey) []string {
	if len(tokens) == 0 {
		return nil
	}
	parent := EmptyParent
	out := make([]string, 0, (len(tokens)+h.BlockSizeTokens-1)/h.BlockSizeTokens)
	for start := 0; start < len(tokens); start += h.BlockSizeTokens {
		end := start + h.BlockSizeTokens
		if end > len(tokens) {
			end = len(tokens)
		}
		hashHex := computeOne(parent, tokens[start:end], key)
		out = append(out, hashHex)
		parent = hashHex
	}
	return out
}

func computeOne(parent string, blockTokens []uint64, key IsolationKey) string {
	h := sha256.New()
	writeString(h, parent)
	writeString(h, key.TenantID)
	writeString(h, key.TenantSalt)
	writeString(h, key.ModelID)
	writeString(h, key.ModelRevision)
	writeString(h, key.TokenizerRevision)
	writeString(h, key.LoRAID)
	writeString(h, key.ModalityKey)
	writeString(h, key.CacheSalt)
	writeUint64(h, uint64(len(blockTokens)))
	for _, token := range blockTokens {
		writeUint64(h, token)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeString(h hash.Hash, value string) {
	writeUint64(h, uint64(len(value)))
	_, _ = h.Write([]byte(value))
}

func writeUint64(h hash.Hash, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, _ = h.Write(buf[:])
}
