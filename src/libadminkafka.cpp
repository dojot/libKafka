#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <vector>

#include "adminkafka.h"

struct partition_t {
    int32_t partition;
    std::vector <int32_t> replicas;
};

struct config_entries_t {
    std::string config_name;
    std::string config_value;
};


struct KafkaMessage {
    int32_t size;
    KafkaMessage() { 
        size = 0;
    }
};



struct RequestMessage {
    KafkaMessage header;
    int16_t api_key;
    int16_t api_version;
    int32_t correlation_id;
    std::string client_id;
};


struct CreateTopicItem {
    std::string topic;
    int32_t num_partitions;
    int16_t replication_factor;
    std::vector<partition_t> replica_assignment;
    std::vector<config_entries_t> config_entries;
    int32_t timeout;
};


struct CreateTopicMessage {
    RequestMessage header;
    std::vector<CreateTopicItem> items;
};

struct CreateTopicResponse {
    int32_t length;
    int32_t correlation_ID;
    int32_t array_count;
    int16_t string_length;
    std::string topic_name;
    int16_t error_code;
};

void marshall(uint8_t *& output, uint32_t & offset, std::string data);
void marshall(uint8_t *& output, uint32_t & offset, int32_t data);
void marshall(uint8_t *& output, uint32_t & offset, int16_t data);
void marshall(uint8_t *& output, uint32_t & offset, partition_t & data);
void marshall(uint8_t *& output, uint32_t & offset, config_entries_t & data);
void marshall(uint8_t *& output, uint32_t & offset, KafkaMessage & data);
void marshall(uint8_t *& output, uint32_t & offset, RequestMessage & data);
void marshall(uint8_t *& output, uint32_t & offset, CreateTopicItem & data);
void marshall(uint8_t *& output, uint32_t & offset, CreateTopicMessage & data);

template <class T>
void marshall(uint8_t *& output, uint32_t & offset, std::vector<T> data) {
    int32_t size = data.size();
    ::marshall(output, offset, size);
    for (T value : data) {
        ::marshall(output, offset, value);
    }
}

void marshall(uint8_t *& output, uint32_t & offset, std::string data) {
    int16_t temp = ntohs(data.length());
    memcpy(output, &temp, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    output += sizeof(uint16_t);;

    const char * str = data.c_str();

    memcpy(output, str, data.length());
    offset += data.length();    
    output += data.length();
}

void marshall(uint8_t *& output, uint32_t & offset, int32_t data) {
    int32_t temp = ntohl(data);
    memcpy(output, &temp, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    output += sizeof(uint32_t);
}

void marshall(uint8_t *& output, uint32_t & offset, int16_t data) {
    int16_t temp = ntohs(data);
    memcpy(output, &temp, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    output += sizeof(uint16_t);
}


void marshall(uint8_t *& output, uint32_t & offset, partition_t & data) {
    ::marshall(output, offset, data.partition);
    ::marshall(output, offset, data.replicas);
}

void marshall(uint8_t *& output, uint32_t & offset, config_entries_t & data) {
    ::marshall(output, offset, data.config_name);
    ::marshall(output, offset, data.config_value);
}
void marshall(uint8_t *& output, uint32_t & offset, KafkaMessage & data) {
    ::marshall(output, offset, data.size);
}
void marshall(uint8_t *& output, uint32_t & offset, RequestMessage & data) {
    ::marshall(output, offset, data.header);
    ::marshall(output, offset, data.api_key);
    ::marshall(output, offset, data.api_version);
    ::marshall(output, offset, data.correlation_id);
    ::marshall(output, offset, data.client_id);
}

void marshall(uint8_t *& output, uint32_t & offset, CreateTopicItem & data) {
    ::marshall(output, offset, data.topic);
    ::marshall(output, offset, data.num_partitions);
    ::marshall(output, offset, data.replication_factor);
    ::marshall(output, offset, data.replica_assignment);
    ::marshall(output, offset, data.config_entries);
    ::marshall(output, offset, data.timeout);
}

void marshall(uint8_t *& output, uint32_t & offset, CreateTopicMessage & data) {
    ::marshall(output, offset, data.header);
    ::marshall(output, offset, data.items);
}

CreateTopicResponse getCreateTopicResponse(char *buffer) {
    CreateTopicResponse response;
    uint32_t temp_32;
    uint16_t temp_16;
    uint8_t * cursor = (uint8_t *) buffer;
    char temp_string[256] = { 0 };

    cursor = (uint8_t *) &buffer [0];
    memcpy(&temp_32, cursor, sizeof(uint32_t));
    response.length = ntohl(temp_32);
    cursor += sizeof(uint32_t);

    memcpy(&temp_32, cursor, sizeof(uint32_t));
    response.correlation_ID = ntohl(temp_32);
    cursor += sizeof(uint32_t);

    memcpy(&temp_32, cursor, sizeof(uint32_t));
    response.array_count = ntohl(temp_32);
    cursor += sizeof(uint32_t);

    memcpy(&temp_16, cursor, sizeof(uint16_t));
    response.string_length = ntohs(temp_16);
    cursor += sizeof(uint16_t);

    
    strncpy(temp_string, (char *) cursor, response.string_length);
    temp_string[255] = 0;
    response.topic_name.assign(temp_string);
    cursor += response.string_length;

    memcpy(&temp_16, cursor, sizeof(uint16_t));
    response.error_code = ntohs(temp_16);
    cursor += sizeof(uint16_t);

    return response;
};

int KafkaSend(uint8_t *data, uint32_t dataSize, char *buffer, std::string kafka_ip, int PORT) {    
    int sock = 0;
    struct sockaddr_in serv_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        return -1;
    }
  
    memset(&serv_addr, '0', sizeof(serv_addr));
  
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
      
    inet_pton(AF_INET, kafka_ip.c_str(), &serv_addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        return -1;
    }

    int ret = send(sock, data, dataSize, 0);
    
    if (ret > 0) {
        int r = read(sock , buffer, 1024);
        return r;
    }

    return -1;
}

int kafka::createTopic(std::string topic, int numPartition, int replicationFactor, std::string kafka_addr, int PORT) {
    char buffer[1024] = { 0 };
    CreateTopicMessage message;
    message.header.api_key = 19;
    message.header.api_version = 0;
    message.header.correlation_id = 98765;
    message.header.client_id = "rdkakfa";

    CreateTopicItem item;
    item.topic = topic;
    item.num_partitions = numPartition;
    item.replication_factor = replicationFactor;
    item.timeout = 400;
    message.items.push_back(item);

    uint32_t offset = 0;
    uint32_t totalSize = 0;
    uint8_t data[256] = { 0 };
    uint8_t * cursor = & data [0];
    ::marshall(cursor, offset, message);
    message.header.header.size = offset;
    totalSize = offset + 4;
    cursor = & data [0];
    offset = 0;
    ::marshall(cursor, offset, message.header);

    
    if (KafkaSend(data, totalSize, buffer, kafka_addr, PORT) > 0) {
        CreateTopicResponse response;
        
        response = getCreateTopicResponse(buffer);

        return response.error_code;   
    }

    return -1;
};

int APIRequest(std::string kafka_addr, int PORT) {
    RequestMessage message;

    char buffer[1024] = { 0 };
    message.api_key = 18;
    message.api_version = 0;
    message.client_id = "rdkafka";
    message.correlation_id = 98765;
    message.header.size = sizeof(message.api_key) + sizeof(message.api_version) + message.client_id.length() + sizeof(message.correlation_id) + 2;
    uint32_t offset = 0;
    uint8_t data[256] = { 0 };
    uint8_t * cursor = & data [0];
    ::marshall(cursor, offset, message);

    
    if (KafkaSend(data, offset, buffer, kafka_addr, PORT) == 0) {
        return 0;
    }

    return -1;
}

