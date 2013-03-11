package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import com.wajam.mry.api.protobuf.MryProtobuf._
import scala.collection.JavaConversions._
import com.wajam.mry.execution.{Transaction => MryTransaction, _}
import com.wajam.mry.execution.MapValue
import com.wajam.mry.execution.IntValue
import com.wajam.mry.api.Transport
import com.wajam.mry.execution.BoolValue
import com.wajam.mry.execution.ListValue
import com.wajam.mry.execution.StringValue
import com.wajam.mry.execution.DoubleValue
import com.wajam.mry.execution.Transaction


/**
 * Protocol buffers translator
 */
class ProtobufTranslator extends ProtocolTranslator {

  def encodeTransaction(transaction: MryTransaction): Array[Byte] = {
    encodePTransaction(transaction).toByteArray
  }

  def decodeTransaction(data: Array[Byte]): MryTransaction = {
    decodePTransaction(PTransaction.parseFrom(data))
  }

  def encodeValue(value: Value): Array[Byte] = this.encodePValue(value).toByteArray

  def decodeValue(data: Array[Byte]): Value = this.decodePValue(PTransactionValue.parseFrom(data))

  def encodeAll(transport: Transport): Array[Byte] = {
    encodePTransport(transport).toByteArray
  }

  def decodeAll(data: Array[Byte]): Transport = {
    decodePTransport(PTransport.parseFrom(data))
  }

  private def encodePTransport(transport: Transport): PTransport =  {
    val Transport(request, response) = transport

    val pTransport = PTransport.newBuilder()

    for (r <- request)
      pTransport.setRequest(encodePTransaction(r))

    for (or <- response; v <- or)
      pTransport.addResponse(encodePValue(v))

    pTransport.build()
  }

  private def decodePTransport(transaction: PTransport): Transport =  {
    null
  }

  private def encodePTransaction(transaction: Transaction): PTransaction =  {
    null
  }

  private def decodePTransaction(transaction: PTransaction): Transaction =  {
    null
  }


  private def encodePValue(value: Value): PTransactionValue = {
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
          val protoVal = this.encodePValue(v)
          collection.addValues(PTransactionCollectionValue.newBuilder().setValue(protoVal).build())
        }

        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.ARRAY)
          .setMap(collection)
          .build()

      case mapValue: MapValue =>
        val collection = PTransactionCollection.newBuilder()

        for ((k, v) <- mapValue.mapValue) {
          val protoVal = this.encodePValue(v)
          collection.addValues(PTransactionCollectionValue.newBuilder().setKey(k).setValue(protoVal).build())
        }

        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.MAP)
          .setMap(collection)
          .build()

      case _: NullValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.NULL)
          .build()

      case _ =>
        throw new TranslationException("Can't convert value '%s' (type %s) to protobuf".format(value, value.getClass))
    }
  }

  private def decodePValue(protoVal: PTransactionValue): Value = {
    protoVal.getType match {
      case PTransactionValue.Type.STRING =>
        StringValue(protoVal.getStringValue)

      case PTransactionValue.Type.INT =>
        IntValue(protoVal.getIntValue)

      case PTransactionValue.Type.BOOL =>
        BoolValue(protoVal.getBoolValue)

      case PTransactionValue.Type.DOUBLE =>
        DoubleValue(protoVal.getDoubleValue)

      case PTransactionValue.Type.MAP =>
        var map = Map[String, Value]()
        val protoMap = protoVal.getMap
        for (protoVal <- protoMap.getValuesList) {
          map += (protoVal.getKey -> this.decodePValue(protoVal.getValue))
        }
        new MapValue(map)

      case PTransactionValue.Type.ARRAY =>
        val protoMap = protoVal.getMap
        val list = for {
          protoVal <- protoMap.getValuesList
        } yield this.decodePValue(protoVal.getValue)

        new ListValue(list)
      case PTransactionValue.Type.NULL =>
        NullValue

      case _ =>
        throw new TranslationException("Unsupported protobuf value type '%s'".format(protoVal.getType))
    }
  }

}
