#include "kv.h"
#include <unordered_map>
#include <string>
#include <mutex>
#include <cstring>
#include <cstdlib>

struct KV {
    std::unordered_map<std::string, std::string> m;
    std::mutex mu;
};

void* kv_new() {
    return new KV();
}

void kv_free(void* h) {
    delete static_cast<KV*>(h);
}

void kv_put(void* h, const char* key, const char* val) {
    auto* kv = static_cast<KV*>(h);
    std::lock_guard<std::mutex> lk(kv->mu);
    kv->m[std::string(key)] = std::string(val);
}

char* kv_get(void* h, const char* key) {
    auto* kv = static_cast<KV*>(h);
    std::lock_guard<std::mutex> lk(kv->mu);
    auto it = kv->m.find(std::string(key));
    if (it == kv->m.end()) return nullptr;

    const std::string& s = it->second;
    char* out = (char*)std::malloc(s.size() + 1);
    if (!out) return nullptr;
    std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

void kv_free_str(char* p) {
    std::free(p);
}
