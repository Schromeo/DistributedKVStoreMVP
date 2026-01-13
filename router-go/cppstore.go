package main

/*
#cgo windows LDFLAGS: -L${SRCDIR}/../storage-cpp/build -lkv
#include "../storage-cpp/kv.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

type CppStore struct {
	h unsafe.Pointer
}

func NewCppStore() *CppStore {
	return &CppStore{h: C.kv_new()}
}

func (s *CppStore) Close() {
	if s.h != nil {
		C.kv_free(s.h)
		s.h = nil
	}
}

func (s *CppStore) Put(k, v string) error {
	ck := C.CString(k)
	cv := C.CString(v)
	defer C.free(unsafe.Pointer(ck))
	defer C.free(unsafe.Pointer(cv))

	C.kv_put(s.h, ck, cv)
	return nil
}

func (s *CppStore) Get(k string) (string, bool, error) {
	ck := C.CString(k)
	defer C.free(unsafe.Pointer(ck))

	p := C.kv_get(s.h, ck)
	if p == nil {
		return "", false, nil
	}
	defer C.kv_free_str(p)

	return C.GoString(p), true, nil
}
