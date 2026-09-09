package ru.wolfram.playground

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tech.ydb.core.grpc.GrpcTransport
import tech.ydb.table.TableClient
import tech.ydb.topic.TopicClient

@Configuration
class YdbConfig {
    @Bean(destroyMethod = "close")
    fun grpcTransport(): GrpcTransport {
        return GrpcTransport.forConnectionString("grpc://localhost:2136/local")
            .build()
    }

    @Bean(destroyMethod = "close")
    fun tableClient(transport: GrpcTransport): TableClient {
        return TableClient.newClient(transport).build()
    }

    @Bean(destroyMethod = "close")
    fun topicClient(transport: GrpcTransport): TopicClient {
        return TopicClient.newClient(transport).build()
    }
}