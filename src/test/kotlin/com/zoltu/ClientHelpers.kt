package com.zoltu

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.Properties

fun getDefaultConsumerProperties(port: Int): Properties {
	val properties = Properties()
	properties.put("bootstrap.servers", "localhost:$port")
	// we want our tests to fail fast
	properties.put("heartbeat.interval.ms", 100)
	properties.put("fetch.max.wait.ms", 500)
	properties.put("session.timeout.ms", 500)
	properties.put("request.timeout.ms", 10000)
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
	properties.put("bootstrap.servers", "localhost:$port")
	return properties
}

fun getDefaultKafkaProducer(port: Int): KafkaProducer<ByteArray, ByteArray> {
	val properties = getDefaultProducerProperties(port)
	val keySerializer = ByteArraySerializer()
	val messageSerializer = ByteArraySerializer()
	return KafkaProducer(properties, keySerializer, messageSerializer)
}
