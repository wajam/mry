package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import com.wajam.mry.api.protobuf.Transaction.{PTransactionCollectionValue, PTransactionCollection, PTransactionValue}
import scala.collection.JavaConversions._
import com.wajam.mry.execution.{Transaction => MryTransaction, _}

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

      case doubleValue: DoubleValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.DOUBLE)
          .setDoubleValue(doubleValue.doubleValue)
          .build()

      case boolValue: BoolValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.BOOL)
          .setBoolValue(boolValue.boolValue)
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

  def fromProtoValue(protoVal: PTransactionValue): Value = {
    protoVal.getType match {
      case PTransactionValue.Type.STRING =>
        new StringValue(protoVal.getStringValue)

      case PTransactionValue.Type.INT =>
        new IntValue(protoVal.getIntValue)

      case PTransactionValue.Type.BOOL =>
        new BoolValue(protoVal.getBoolValue)

      case PTransactionValue.Type.DOUBLE =>
        new DoubleValue(protoVal.getDoubleValue)

      case PTransactionValue.Type.MAP =>
        val protoMap = protoVal.getMap
        val map: Map[String, Value] = (for {
          protoVal <- protoMap.getValuesList
        } yield (protoVal.getKey -> this.fromProtoValue(protoVal.getValue))).toMap // TODO HotSpot hates that toMap

        new MapValue(map)

      case PTransactionValue.Type.ARRAY =>
        val protoMap = protoVal.getMap
        val list = for {
          protoVal <- protoMap.getValuesList
        } yield this.fromProtoValue(protoVal.getValue)

        new ListValue(list)
      case PTransactionValue.Type.NULL =>
        new NullValue

      case _ =>
        throw new TranslationException("Unsupported protobuf value type '%s'".format(protoVal.getType))
    }
  }

  def decodeValue(data: Array[Byte]): Value = this.fromProtoValue(PTransactionValue.parseFrom(data))

  def encodeTransaction(transaction: MryTransaction): Array[Byte] = {
    null
  }

  def decodeTransaction(data: Array[Byte]): MryTransaction = {
    new MryTransaction()
  }
}
