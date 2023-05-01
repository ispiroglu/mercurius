package serialize

import (
	"bytes"
	"encoding/gob"
	"log"
)

type ISerializer interface {
	Encode(toEncode any) ([]byte, error)
	Decode(byteArr []byte, a any) error
}

type Serializer struct {
	Buf     *bytes.Buffer
	Encoder *gob.Encoder
	Decoder *gob.Decoder
}

func NewSerializer() *Serializer {
	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	decoder := gob.NewDecoder(buf)

	return &Serializer{
		Buf:     buf,
		Encoder: encoder,
		Decoder: decoder,
	}
}

func (s *Serializer) Encode(toEncode any) ([]byte, error) {
	s.Buf.Reset()
	defer s.Buf.Reset()

	err := s.Encoder.Encode(toEncode)
	if err != nil {
		log.Println("Error on encode, ", err)
		return nil, err
	}
	return s.Buf.Bytes(), nil
}

func (s *Serializer) Decode(byteArr []byte, toReturn any) error {
	s.Buf.Write(byteArr)
	defer s.Buf.Reset()

	err := s.Decoder.Decode(toReturn)
	if err != nil {
		log.Println("Error on decode, ", err)
		return err
	}

	//fmt.Println("Decoded Successfully, ", toReturn)
	return nil
}
