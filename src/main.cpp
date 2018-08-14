#include <napi.h>
#include "lib_kafka.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  return lib_kafka::Init(env, exports);
}

NODE_API_MODULE(lib_kafka, InitAll)