[![Download](https://api.bintray.com/packages/zoltu/maven/StubKafkaBroker/images/download.svg)](https://bintray.com/zoltu/maven/StubKafkaBroker/_latestVersion)
[![Build Status](https://ci.appveyor.com/api/projects/status/github/Zoltu/StubKafkaBroker?svg=true)](https://ci.appveyor.com/project/Zoltu/stubkafkabroker)

# StubKafkaBroker
A stub Kafka Broker that speaks the Kafka wire protocol.

### What Works (using stub defaults)
The following requests: Metadata, Produce, Fetch, OffsetFetch, OffsetCommit, GroupCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup.

This is enough to support simple (poll topic + partition) production/consumption as well as basic single-consumer group management (subscribe topic and poll).

### What Doesn't Work (using stub defaults)
The following requests: ListOffsetRequest, LeaderAndIsrRequest, StopReplicaRequest, ControlledShutdownRequest, UpdateMetadataRequest, DescribeGroupsRequest, ListGroupsRequest.  These are all administrative methods and generally aren't necessary for your average producer/consumer.

Multi-consumer group management.  The default behavior is to have the broker ack (or similar) everything.  It doesn't actually track group state at the moment so if two consumers connect they won't actually be in a consumer group together.

## Usage
### Gradle
```groovy
repositories {
	jcenter()
}
dependencies {
	compile(group: 'com.zoltu', name: 'StubKafkaBroker', version: '3.0.5')
}
```
*Note: You probably want to replace the version listed here with the latest version shown in the Build Status badge at the top of this readme.*

### Maven
```xml
<repositories>
	<repository>
		<snapshots>
			<enabled>false</enabled>
		</snapshots>
		<id>central</id>
		<name>bintray</name>
		<url>http://jcenter.bintray.com</url>
	</repository>
</repositories>
<dependency>
	<groupId>com.zoltu</groupId>
	<artifactId>StubKafkaBroker</artifactId>
	<version>1.1.17</version>
</dependency>
```
*Note: You probably want to replace the version listed here with the latest version shown in the Build Status badge at the top of this readme.*

### Java
TODO: It is basically the same as Kotlin but more verbose

### Kotlin
See the [tests](https://github.com/Zoltu/StubKafkaBroker/tree/master/src/test/kotlin/com/zoltu) for more examples of how to assert on various actions (like production and consumption).

```kotlin
// instatiate the server, this will start up the server and cause it to start listening on a random open port
val stubKafkaBroker = StubKafkaBroker()

// you can find out what port the server is listening on once it is created
val port = stubKafkaBroker.thisBroker.port()

// adding a topic will cause the default request handlers that respond with topic information to have this topic (like MetadataRequest)
stubKafkaBroker.addTopic("my topic")

// create a new Kafka Consumer (using off-the-shelf Kafka client library)
val properties = Properties()
properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:$port")
val kafkaConsumer = KafkaConsumer<ByteArray, ByteArray>(properties, ByteArrayDeserializer(), ByteArrayDeserializer())

// our kafka consumer can now make requests of the broker that we stubbed out
val topics = kafkaConsumer.listTopics()
assertEquals("my topic", topics.entries.single().key)

// more interesting would be seeing what happens if the broker takes a long time to respond
val defaultMetadataRequestHandler = stubKafkaBroker.metadataRequestHandler
stubKafkaBroker.metadataRequestHandler = { requestHeader, metadataRequest ->
	Thread.sleep(Duration.ofMinutes(1).toMillis())
	defaultMetadataRequestHandler(requestHeader, metadataRequest)
}
// the following causes the 0.9.0 Kafka Client to block forever since it refuses to do anything until it gets metadata and every metadata request will timeout (default timeout is 40 seconds)
// topics = kafkaConsumer.listTopics()

// try again with multiple brokers
val stubKafkaBrokerFast = StubKafkaBroker()
stubKafkaBrokerFast.addTopic("my topic")
properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:${stubKafkaBroker.thisBroker.port()};localhost:${stubKafkaBrokerFast.thisBroker.port()}")
kafkaConsumer = KafkaConsumer<ByteArray, ByteArray>(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
// does not block, since the fast broker will respond rather quickly
val topics = kafkaConsumer.listTopics()
assertEquals("my topic", topics.entries.single().key)
```
