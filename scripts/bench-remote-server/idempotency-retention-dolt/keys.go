package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

var errVerification = errors.New("synthetic request key epoch or signature is not accepted")

type verificationRing struct {
	active  uint64
	maxKeys int
	keys    map[uint64][32]byte
	order   []uint64
}

func newVerificationRing(maxKeys int) *verificationRing {
	return &verificationRing{maxKeys: maxKeys, keys: make(map[uint64][32]byte)}
}

func (r *verificationRing) rotate(epoch uint64) error {
	if epoch == 0 || (r.active != 0 && epoch != r.active+1) {
		return fmt.Errorf("key epoch must advance exactly once")
	}
	r.active = epoch
	r.keys[epoch] = sha256.Sum256([]byte(fmt.Sprintf("beads-perf-lab-retention-key-epoch-%d", epoch)))
	r.order = append(r.order, epoch)
	for len(r.order) > r.maxKeys {
		delete(r.keys, r.order[0])
		r.order = r.order[1:]
	}
	return nil
}

func (r *verificationRing) sign(in request) (request, error) {
	key, ok := r.keys[r.active]
	if !ok {
		return request{}, fmt.Errorf("verification ring has no active key")
	}
	in.KeyEpoch = r.active
	in.Signature = requestSignature(key, in)
	return in, nil
}

func (r *verificationRing) verify(in request) error {
	key, ok := r.keys[in.KeyEpoch]
	if !ok {
		return errVerification
	}
	want := requestSignature(key, in)
	if !hmac.Equal(want[:], in.Signature[:]) {
		return errVerification
	}
	return nil
}

func requestSignature(key [32]byte, in request) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	writeField(mac, []byte("beads-perf-lab-retention-dolt-v2"))
	writeField(mac, []byte(labDatabase))
	writeField(mac, []byte(in.RunID))
	writeField(mac, []byte(in.ProducerID))
	writeField(mac, []byte(in.SubjectHash))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], in.ExpectedRepositoryEpoch)
	writeField(mac, number[:])
	binary.BigEndian.PutUint64(number[:], in.ProducerEpoch)
	writeField(mac, number[:])
	binary.BigEndian.PutUint64(number[:], in.Sequence)
	writeField(mac, number[:])
	writeField(mac, []byte(in.RequestHash))
	writeField(mac, []byte(in.Payload))
	writeField(mac, []byte(in.OperationID))
	binary.BigEndian.PutUint64(number[:], in.KeyEpoch)
	writeField(mac, number[:])
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func writeField(dst interface{ Write([]byte) (int, error) }, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = dst.Write(size[:])
	_, _ = dst.Write(value)
}
