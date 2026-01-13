#include "kv.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <mutex>
#include <string>
#include <cstring>
#include <cstdlib>
#include <filesystem>

struct KV {
    std::unique_ptr<rocksdb::DB> db;
    std::mutex mu;
};

static std::string get_db_path() {
    const char* env = std::getenv("KV_DB_PATH");
    if (env && env[0] != '\0') {
        return std::string(env);
    }
    // ASCII-only default path to avoid encoding issues on Windows toolchain
    return std::string("C:\\tmp\\dkv\\rocksdb");
}

void* kv_new() {
    auto* kv = new KV();

    std::string path = get_db_path();
    try {
        std::filesystem::create_directories(path);
    } catch (...) {
        // ignore; rocksdb open will report error if path invalid
    }

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw = nullptr;
    rocksdb::Status s = rocksdb::DB::Open(options, path, &raw);
    if (!s.ok()) {
        // If open fails, keep db=nullptr; caller operations become no-op / not found.
        // (For MVP, we avoid throwing across C boundary.)
        delete kv;
        return nullptr;
    }

    kv->db.reset(raw);
    return kv;
}

void kv_free(void* h) {
    auto* kv = static_cast<KV*>(h);
    delete kv;
}

void kv_put(void* h, const char* key, const char* val) {
    auto* kv = static_cast<KV*>(h);
    if (!kv || !kv->db) return;

    std::lock_guard<std::mutex> lk(kv->mu);

    rocksdb::WriteOptions wopt;
    // For MVP, default is fine. (You can tune sync/WAL later)
    kv->db->Put(wopt, rocksdb::Slice(key), rocksdb::Slice(val));
}

char* kv_get(void* h, const char* key) {
    auto* kv = static_cast<KV*>(h);
    if (!kv || !kv->db) return nullptr;

    std::lock_guard<std::mutex> lk(kv->mu);

    std::string value;
    rocksdb::ReadOptions ropt;
    rocksdb::Status s = kv->db->Get(ropt, rocksdb::Slice(key), &value);
    if (!s.ok()) return nullptr;

    char* out = (char*)std::malloc(value.size() + 1);
    if (!out) return nullptr;
    std::memcpy(out, value.c_str(), value.size());
    out[value.size()] = '\0';
    return out;
}

void kv_free_str(char* p) {
    std::free(p);
}
