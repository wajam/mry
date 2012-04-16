package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import com.wajam.mry.api.protobuf.Transaction.{PTransactionCollectionValue, PTransactionCollection, PTransactionValue}
import scala.collection.JavaConversions._
import com.wajam.mry.execution.{ListValue, MapValue, IntValue, NullValue, StringValue, Value, Transaction => MryTransaction}

/**
 * Protocol buffers translator
 */
class ProtobufTranslator extends ProtocolTranslator {
  private def toProtoValue(value: Value): PTransactionValue = {
    value.serializableValue match {
      case strValue: StringValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.STRING)
          .setStringValue(strValue.strValue)
          .build()

      case intValue: IntValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.INT)
          .setIntValue(intValue.intValue)
          .build()

      case listValue: ListValue =>
        val collection = PTransactionCollection.newBuilder()

        for (v <- listValue.listValue) {
          val protoVal = this.toProtoValue(v)
          collection.addValues(PTransactionCollectionValue.newBuilder().setValue(protoVal).build())
        }

        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.ARRAY)
          .setMap(collection)
          .build()

      case mapValue: MapValue =>
        val collection = PTransactionCollection.newBuilder()

        for ((k, v) <- mapValue.mapValue) {
          val protoVal = this.toProtoValue(v)
          collection.addValues(PTransactionCollectionValue.newBuilder().setKey(k).setValue(protoVal).build())
        }

        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.MAP)
          .setMap(collection)
          .build()

      case n: NullValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.NULL)
          .build()

      case _ =>
        throw new TranslationException("Can't convert value '%s' (type %s) to protobuf".format(value, value.getClass))
    }
  }

  def encodeValue(value: Value): Array[Byte] = this.toProtoValue(value).toByteArray

  def fromProtoValue(protoVal: PTransactionValue) : Value = {
    protoVal.getType match {
      case PTransactionValue.Type.STRING =>
        new StringValue(protoVal.getStringValue)

      case PTransactionValue.Type.INT =>
        new IntValue(protoVal.getIntValue)

      case PTransactionValue.Type.MAP =>
        var map = Map[String, Value]()
        val protoMap = protoVal.getMap
        for (protoVal <- protoMap.getValuesList) {
          map += (protoVal.getKey -> this.fromProtoValue(protoVal.getValue))
        }
        new MapValue(map)

      case PTransactionValue.Type.ARRAY =>
        var list = List[Value]()
        val protoMap = protoVal.getMap
        for (protoVal <- protoMap.getValuesList) {
          list :+= this.fromProtoValue(protoVal.getValue)
        }
        new ListValue(list)

      case PTransactionValue.Type.NULL =>
        new NullValue

      case _ =>
        throw new TranslationException("Unsupported protobuf value type '%s'".format(protoVal.getType))
    }
  }

  def decodeValue(data: Array[Byte]): Value = this.fromProtoValue(PTransactionValue.parseFrom(data))

  def encodeTransaction(transaction: MryTransaction): Array[Byte] = {
    return null
  }

  def decodeTransaction(data: Array[Byte]): MryTransaction = {
    new MryTransaction()
  }
}
