package com.zoltu.extensions

import org.apache.kafka.common.requests.AbstractRequestResponse
import org.apache.kafka.common.requests.ResponseHeader
import java.nio.ByteBuffer

fun AbstractRequestResponse.toResponseByteBuffer(correlationId: Int): ByteBuffer {
	val responseHeader = ResponseHeader(correlationId).toStruct()!!
	val responseBody = this.toStruct()!!
	val length = responseHeader.sizeOf() + responseBody.sizeOf()
	val byteBuffer = ByteBuffer.wrap(ByteArray(length))
	responseHeader.writeTo(byteBuffer)
	responseBody.writeTo(byteBuffer)
	return byteBuffer
}
