package raftkv

import "bytes"
import "encoding/gob"

type KV struct {
	data map[string]string
}

func NewKV() *KV {
	return &KV{
		data: make(map[string]string),
	}
}

func (kv *KV) GobEncode() ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(kv.data)
	return buffer.Bytes(), nil
}

func (kv *KV) GobDecode(data []byte) error {
	if data == nil || len(data) < 1 {
		return nil
	}
	d := make(map[string]string)
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&d); err != nil {
		return err
	}
	kv.data = d
	return nil
}

func (kv *KV) Get(key string) string {
	value, ok := kv.data[key]
	if !ok {
		return ""
	}
	return value
}

func (kv *KV) Set(key string, value string) {
	kv.data[key] = value
}
