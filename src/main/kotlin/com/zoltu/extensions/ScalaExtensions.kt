package com.zoltu.extensions

import scala.Predef
import scala.Tuple2
import scala.`Predef$`.`MODULE$`
import scala.collection.JavaConversions

fun <T> Array<T>.toScalaSeq(): scala.collection.Seq<T> = JavaConversions.asScalaIterator(this.iterator()).toSeq()
fun <T> Sequence<T>.toScalaSeq(): scala.collection.Seq<T> = JavaConversions.asScalaIterator(this.iterator()).toSeq()
fun <T> Iterable<T>.toScalaSeq(): scala.collection.Seq<T> = JavaConversions.asScalaIterator(this.iterator()).toSeq()
fun <TKey, TValue> Map<TKey, TValue>.toScalaMap(): scala.collection.mutable.Map<TKey, TValue> = JavaConversions.mapAsScalaMap(this)
fun <TKey, TValue> Map<TKey, TValue>.toScalaImmutableMap(): scala.collection.immutable.Map<TKey, TValue> {
	val wrappedArray = Predef.wrapRefArray(this.entries.map { Tuple2(it.key, it.value) }.toTypedArray())
	@Suppress("UNCHECKED_CAST")
	return `MODULE$`.Map().apply(wrappedArray) as scala.collection.immutable.Map<TKey, TValue>
}
