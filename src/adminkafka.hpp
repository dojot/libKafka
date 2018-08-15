#ifndef ADMIN_KAFKA_HPP_
#define ADMIN_KAFKA_HPP_

#include <string>

namespace kafka {
  int createTopic(std::string topic, int numPartition, int replicationFactor, std::string kafka_addr, int PORT);
}

#endif // ADMIN_KAFKA_HPP_