package raftkv

import "bytes"
import "encoding/gob"

type Recorder struct {
	data map[string]int64
}

func NewRecorder() *Recorder {
	return &Recorder{
		data: make(map[string]int64),
	}
}

func (r *Recorder) GobEncode() ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(r.data)
	return buffer.Bytes(), nil
}

func (r *Recorder) GobDecode(data []byte) error {
	if data == nil || len(data) < 1 {
		return nil
	}
	d := make(map[string]int64)
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&d); err != nil {
		return err
	}
	r.data = d
	return nil
}

func (r *Recorder) Record(cid string, seq int64) {
	if seq > r.data[cid] {
		r.data[cid] = seq
	}
}

func (r *Recorder) Recorded(cid string, seq int64) bool {
	s, ok := r.data[cid]
	return ok && (seq <= s)
}
