package com.zoltu

import com.zoltu.extensions.listen
import com.zoltu.extensions.readBytes
import com.zoltu.extensions.readInt
import com.zoltu.extensions.writeBytes
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel

public class LengthPrefixedMessageServer(private val process: (ByteBuffer) -> ByteBuffer) {
	public val host: String get() = inetSocketAddress.hostString
	public val port: Int get() = inetSocketAddress.port

	private val inetSocketAddress: InetSocketAddress get() {
		val localAddress = serverSocketChannel.localAddress
		when (localAddress) {
			is InetSocketAddress -> return localAddress
			else -> throw Exception("Server is not bound to an InetSocketAddress.  This should be impossible.")
		}
	}

	private val socketAddress = InetSocketAddress(0)
	private val serverSocketChannel = AsynchronousServerSocketChannel.open().bind(socketAddress)

	init {
		acceptOneConnection()
	}

	private fun acceptOneConnection() {
		serverSocketChannel.listen()
				.thenApply { acceptOneConnection(); it }
				.thenAccept { readPayload(it) }
	}

	private fun readPayload(socketChannel: AsynchronousSocketChannel) {
		socketChannel.readInt()
				.thenCompose { socketChannel.readBytes(it) }
				.thenApply { getBuffersForWritingFromResponse(process(it)) }
				.thenCompose { socketChannel.writeBytes(it) }
				.thenAccept { readPayload(socketChannel) }
				.exceptionally { socketChannel.close(); null }
	}

	private fun getBuffersForWritingFromResponse(responseByteBuffer: ByteBuffer): Array<ByteBuffer> {
		responseByteBuffer.flip()
		val lengthByteBuffer = ByteBuffer.allocate(4).putInt(responseByteBuffer.capacity())
		lengthByteBuffer.flip()
		return arrayOf(lengthByteBuffer, responseByteBuffer)
	}
}
