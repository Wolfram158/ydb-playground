package ru.wolfram.playground

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tech.ydb.common.transaction.TxMode
import tech.ydb.table.TableClient
import tech.ydb.table.query.Params
import tech.ydb.table.values.ListValue
import tech.ydb.table.values.PrimitiveValue
import tech.ydb.topic.TopicClient
import tech.ydb.topic.settings.SendSettings
import tech.ydb.topic.settings.WriterSettings
import tech.ydb.topic.write.AsyncWriter
import tech.ydb.topic.write.Message
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@Component
@OptIn(ExperimentalAtomicApi::class)
class OutboxWorker(
    private val tableClient: TableClient,
    private val topicClient: TopicClient
) {
    private val log = LoggerFactory.getLogger(OutboxWorker::class.java)

    private val currentWriter = AtomicReference<AsyncWriter?>(null)

    @Scheduled(fixedRate = 30000)
    fun processPendingMessages() {
        log.info("Starting outbox processing...")

        val session = tableClient
            .createSession(Duration.of(5, ChronoUnit.SECONDS))
            .join().value

        val tx = session
            .beginTransaction(TxMode.SERIALIZABLE_RW)
            .join()
            .value

        var writer: AsyncWriter? = null

        try {
            val rs = tx.executeDataQuery("select * from outbox where status = 'pending' limit 10")
                .join().value.getResultSet(0)
            val messages = mutableListOf<OutboxMessage>()
            while (rs.next()) {
                messages.add(
                    OutboxMessage(
                        id = rs.getColumn("id").int64,
                        msg = rs.getColumn("msg").text,
                        status = rs.getColumn("status").text
                    )
                )
            }

            writer = getOrCreateWriter()

            CompletableFuture.allOf(*messages.map { message ->
                writer.send(
                    Message.newBuilder()
                        .setData(message.msg.toByteArray(Charsets.UTF_8))
                        .setSeqNo(message.id)
                        .build(),
                    SendSettings.newBuilder()
                        .setTransaction(tx)
                        .build()
                )
            }.toTypedArray()).join()

            val params = Params.of(
                $$"$ids",
                ListValue.of(*messages.map { PrimitiveValue.newInt64(it.id) }.toTypedArray())
            )

            tx.executeDataQuery(
                $$"update outbox set status = 'processed' where id in $ids",
                params
            )
                .join()

            tx.commit().join()

            log.info("Outbox processing completed. Processed ${messages.size} messages")
        } catch (e: Exception) {
            log.error("Outbox processing failed, rolling back", e)
            writer?.let { invalidateWriter(it) }
            try {
                tx.rollback().join()
            } catch (e: Exception) {
                log.error("Rollback failed", e)
            }
        } finally {
            session.close()
        }
    }

    @Synchronized
    private fun getOrCreateWriter(): AsyncWriter {
        val existing = currentWriter.load()
        if (existing != null) {
            return existing
        }

        log.info("Creating new SyncWriter...")

        val settings = WriterSettings.newBuilder()
            .setTopicPath("/local/outbox-events")
            .setProducerId("outbox-worker-producer")
            .setMessageGroupId("outbox-worker-producer")
            .build()

        val writer = topicClient.createAsyncWriter(settings)
        writer.init().join()

        currentWriter.store(writer)
        log.info("SyncWriter created and initialized")

        return writer
    }

    @Synchronized
    private fun invalidateWriter(writer: AsyncWriter) {
        currentWriter.compareAndSet(writer, null)

        try {
            log.info("Shutting down invalidated writer...")
            writer.shutdown()
            log.info("Writer shut down successfully")
        } catch (e: Exception) {
            log.warn("Failed to shutdown writer: ${e.message}")
        }
    }
}

data class OutboxMessage(
    val id: Long,
    val msg: String,
    val status: String
)