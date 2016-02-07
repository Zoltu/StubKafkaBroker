package com.zoltu

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.Properties

fun getDefaultConsumerProperties(port: Int): Properties {
	val properties = Properties()
	properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:$port")
	properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my group")
	// we want our tests to fail fast
	properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100)
	properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500)
	properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 500)
	properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000)
	return properties
}

fun getDefaultKafkaConsumer(port: Int): KafkaConsumer<ByteArray, ByteArray> {
	val properties = getDefaultConsumerProperties(port)
	val keyDeserializer = ByteArrayDeserializer()
	val messageDeserializer = ByteArrayDeserializer()
	return KafkaConsumer<ByteArray, ByteArray>(properties, keyDeserializer, messageDeserializer)
}

fun getDefaultProducerProperties(port: Int): Properties {
	val properties = Properties()
	properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:$port")
	return properties
}

fun getDefaultKafkaProducer(port: Int): KafkaProducer<ByteArray, ByteArray> {
	val properties = getDefaultProducerProperties(port)
	val keySerializer = ByteArraySerializer()
	val messageSerializer = ByteArraySerializer()
	return KafkaProducer(properties, keySerializer, messageSerializer)
}
