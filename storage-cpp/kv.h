#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// opaque handle
void* kv_new();
void  kv_free(void* h);

void  kv_put(void* h, const char* key, const char* val);

// Returns a malloc-allocated C string. Caller must call kv_free_str().
char* kv_get(void* h, const char* key);
void  kv_free_str(char* p);

#ifdef __cplusplus
}
#endif
