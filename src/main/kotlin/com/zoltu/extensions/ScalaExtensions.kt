package com.zoltu.extensions

import scala.collection.JavaConversions
import scala.collection.Seq

fun <T> Array<T>.toScalaSeq(): Seq<T> = JavaConversions.asScalaIterator(this.iterator()).toSeq()
fun <T> Sequence<T>.toScalaSeq(): Seq<T> = JavaConversions.asScalaIterator(this.iterator()).toSeq()
fun <T> Iterable<T>.toScalaSeq(): Seq<T> = JavaConversions.asScalaIterator(this.iterator()).toSeq()
fun <TKey, TValue> Map<TKey, TValue>.toScalaMap(): scala.collection.mutable.Map<TKey, TValue> = JavaConversions.mapAsScalaMap(this)
