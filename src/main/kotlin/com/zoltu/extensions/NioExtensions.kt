package com.zoltu.extensions

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

fun AsynchronousServerSocketChannel.listen(): CompletableFuture<AsynchronousSocketChannel> {
	val completionHandler = object : CompletionHandler<AsynchronousSocketChannel, CompletableFuture<AsynchronousSocketChannel>> {
		override fun completed(socketChannel: AsynchronousSocketChannel, attachment: CompletableFuture<AsynchronousSocketChannel>) {
			attachment.complete(socketChannel)
		}

		override fun failed(throwable: Throwable, attachment: CompletableFuture<AsynchronousSocketChannel>) {
			attachment.completeExceptionally(throwable)
		}
	}

	val completableFuture = CompletableFuture<AsynchronousSocketChannel>()
	this.accept<CompletableFuture<AsynchronousSocketChannel>>(completableFuture, completionHandler)
	return completableFuture
}

fun AsynchronousSocketChannel.readInt(): CompletableFuture<Int> {
	return this.readBytes(4).thenApply { it.int }
}

fun AsynchronousSocketChannel.readBytes(length: Int): CompletableFuture<ByteBuffer> {
	data class Attachment(val future: CompletableFuture<ByteBuffer>, val byteBuffer: ByteBuffer)

	val completionHandler = object : CompletionHandler<Int, Attachment> {
		override fun completed(result: Int, attachment: Attachment) {
			if (result == -1) {
				attachment.future.completeExceptionally(Exception("Reached the end of the TCP stream."))
				return
			}
			attachment.byteBuffer.flip()
			attachment.future.complete(attachment.byteBuffer)
		}

		override fun failed(throwable: Throwable, attachment: Attachment) {
			attachment.future.completeExceptionally(throwable)
		}
	}

	val completableFuture = CompletableFuture<ByteBuffer>()
	val byteBuffer = ByteBuffer.allocate(length);
	val attachment = Attachment(completableFuture, byteBuffer)
	this.read<Attachment>(byteBuffer, attachment, completionHandler)
	return completableFuture
}

fun AsynchronousSocketChannel.writeBytes(byteBuffers: Array<ByteBuffer>): CompletableFuture<Nothing> {
	val completionHandler = object : CompletionHandler<Long, CompletableFuture<Nothing>> {
		override fun completed(result: Long, attachment: CompletableFuture<Nothing>) {
			attachment.complete(null)
		}

		override fun failed(throwable: Throwable, attachment: CompletableFuture<Nothing>) {
			attachment.completeExceptionally(throwable)
		}
	}

	val completableFuture = CompletableFuture<Nothing>()
	this.write(byteBuffers, 0, byteBuffers.size, 0, TimeUnit.MILLISECONDS, completableFuture, completionHandler)
	return completableFuture
}
