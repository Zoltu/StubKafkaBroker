package com.zoltu

import com.zoltu.extensions.toByteBuffer
import com.zoltu.extensions.toScalaSeq
import kafka.api.PartitionMetadata
import kafka.api.TopicMetadata
import kafka.api.TopicMetadataResponse
import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.ControlledShutdownRequest
import org.apache.kafka.common.requests.DescribeGroupsRequest
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.GroupCoordinatorRequest
import org.apache.kafka.common.requests.HeartbeatRequest
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.ListGroupsRequest
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.StopReplicaRequest
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.common.requests.UpdateMetadataRequest
import scala.Option
import java.nio.ByteBuffer

class StubKafkaServer {
	val thisBroker: BrokerEndPoint get() = BrokerEndPoint(0, lengthPrefixedMessageServer.host, lengthPrefixedMessageServer.port)

	var metadataRequestHandler: (RequestHeader, MetadataRequest) -> TopicMetadataResponse = { requestHeader, metadataRequest ->
		TopicMetadataResponse(brokers.toScalaSeq(), topics.map { it.toTopicMetadata() }.toScalaSeq(), requestHeader.correlationId())
	}

	private val lengthPrefixedMessageServer = LengthPrefixedMessageServer { processRequest(it) }
	private var brokers: Set<BrokerEndPoint> = setOf(thisBroker)
	private var topics: Set<Topic> = emptySet()

	fun reset() {
		clearBrokers()
		clearTopics()
	}

	fun addBroker(broker: BrokerEndPoint) = { brokers = brokers.plus(broker) }
	fun clearBrokers() {
		brokers = setOf(thisBroker)
	}

	fun addTopic(topic: Topic) {
		topics = topics.plus(topic)
	}

	fun clearTopics() {
		topics = emptySet()
	}

	private fun processRequest(byteBuffer: ByteBuffer): ByteBuffer {
		val requestHeader = RequestHeader.parse(byteBuffer)
		val requestBody = AbstractRequest.getRequest(requestHeader.apiKey().toInt(), requestHeader.apiVersion().toInt(), byteBuffer)
		when (requestBody ) {
			is MetadataRequest -> return metadataRequestHandler(requestHeader, requestBody).toByteBuffer()
			is ProduceRequest, is FetchRequest, is ListOffsetRequest, is LeaderAndIsrRequest, is StopReplicaRequest, is ControlledShutdownRequest, is UpdateMetadataRequest, is OffsetCommitRequest, is OffsetFetchRequest, is GroupCoordinatorRequest, is JoinGroupRequest, is HeartbeatRequest, is LeaveGroupRequest, is SyncGroupRequest, is DescribeGroupsRequest, is ListGroupsRequest -> {
				throw UnsupportedOperationException("Unhandled request type.")
			}
			else -> throw UnsupportedOperationException("Unhandled request type.")
		}
	}

	data class Topic(val topicName: String, val partitions: Array<Partition>) {
		fun toTopicMetadata(): TopicMetadata = TopicMetadata(topicName, partitions.map { it.toPartitionMetadata() }.toScalaSeq(), 0)

		companion object Factory {
			fun createSimple(topicName: String, partitionLeader: BrokerEndPoint) = Topic(topicName, arrayOf(Partition.createSimple(0, partitionLeader)))
		}
	}

	data class Partition(val id: Int, val leader: BrokerEndPoint?, val replicas: Array<BrokerEndPoint>, val inSyncReplicas: Array<BrokerEndPoint>) {
		fun toPartitionMetadata(): PartitionMetadata = PartitionMetadata(id, Option.apply(leader), replicas.toScalaSeq(), inSyncReplicas.toScalaSeq(), 0)

		companion object Factory {
			fun createSimple(id: Int, leader: BrokerEndPoint) = Partition(id, leader, emptyArray(), emptyArray())
		}
	}
}
