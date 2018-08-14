#include <napi.h>
namespace lib_kafka {
  int createTopic(std::string topic, int numPartition, int replicationFactor, std::string kafka_addr, int PORT);
  Napi::Number createTopicWrapper(const Napi::CallbackInfo& info);
  Napi::Object Init(Napi::Env env, Napi::Object exports);
}