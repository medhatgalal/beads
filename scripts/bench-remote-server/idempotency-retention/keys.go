package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
)

const signatureDomain = "beads-idempotency-envelope-v1"

var ErrVerification = errors.New("request signature is not accepted by the bounded verification ring")

// VerificationRing retains a fixed number of HMAC verification epochs. The
// prototype keeps key bytes in memory; production should supply equivalent
// signing and verification through a secret-manager/KMS adapter.
type VerificationRing struct {
	mu      sync.RWMutex
	maxKeys int
	active  uint64
	keys    map[uint64][32]byte
	ordered []uint64
}

func NewVerificationRing(maxKeys int, initialEpoch uint64, secret []byte) (*VerificationRing, error) {
	if maxKeys <= 0 {
		return nil, fmt.Errorf("verification ring size must be positive")
	}
	if initialEpoch == 0 || initialEpoch > math.MaxInt64 {
		return nil, fmt.Errorf("initial key epoch must be positive")
	}
	key, err := normalizeKey(secret)
	if err != nil {
		return nil, err
	}
	return &VerificationRing{
		maxKeys: maxKeys,
		active:  initialEpoch,
		keys:    map[uint64][32]byte{initialEpoch: key},
		ordered: []uint64{initialEpoch},
	}, nil
}

func normalizeKey(secret []byte) ([32]byte, error) {
	if len(secret) < 32 {
		return [32]byte{}, fmt.Errorf("verification secret must contain at least 32 bytes")
	}
	return sha256.Sum256(secret), nil
}

func (r *VerificationRing) Rotate(nextEpoch uint64, secret []byte) error {
	key, err := normalizeKey(secret)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active >= math.MaxInt64 {
		return fmt.Errorf("active key epoch is exhausted")
	}
	if nextEpoch != r.active+1 {
		return fmt.Errorf("key epoch must advance exactly once: active=%d requested=%d", r.active, nextEpoch)
	}
	r.active = nextEpoch
	r.keys[nextEpoch] = key
	r.ordered = append(r.ordered, nextEpoch)
	for len(r.ordered) > r.maxKeys {
		retired := r.ordered[0]
		delete(r.keys, retired)
		r.ordered = r.ordered[1:]
	}
	return nil
}

func (r *VerificationRing) Sign(in Request) (Request, error) {
	r.mu.RLock()
	epoch := r.active
	key := r.keys[epoch]
	r.mu.RUnlock()
	in.KeyEpoch = epoch
	in.Signature = signRequest(key, in)
	return in, nil
}

func (r *VerificationRing) Verify(in Request) error {
	r.mu.RLock()
	key, ok := r.keys[in.KeyEpoch]
	r.mu.RUnlock()
	if !ok {
		return ErrVerification
	}
	want := signRequest(key, in)
	if !hmac.Equal(want[:], in.Signature[:]) {
		return ErrVerification
	}
	return nil
}

func (r *VerificationRing) Bounds() (active uint64, oldest uint64, count int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ordered) == 0 {
		return r.active, 0, 0
	}
	return r.active, r.ordered[0], len(r.ordered)
}

func signRequest(key [32]byte, in Request) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	writeSignatureField(mac, []byte(signatureDomain))
	writeSignatureField(mac, []byte(in.ProjectID))
	writeSignatureField(mac, []byte(in.ProducerID))
	writeSignatureField(mac, in.SubjectHash[:])
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], in.LedgerEpoch)
	writeSignatureField(mac, number[:])
	binary.BigEndian.PutUint64(number[:], in.ProducerEpoch)
	writeSignatureField(mac, number[:])
	binary.BigEndian.PutUint64(number[:], in.Sequence)
	writeSignatureField(mac, number[:])
	writeSignatureField(mac, in.RequestHash[:])
	binary.BigEndian.PutUint64(number[:], in.KeyEpoch)
	writeSignatureField(mac, number[:])
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func writeSignatureField(dst interface{ Write([]byte) (int, error) }, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = dst.Write(size[:])
	_, _ = dst.Write(value)
}
