package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import com.wajam.mry.api.protobuf.MryProtobuf._
import scala.collection.JavaConverters._
import com.wajam.mry.execution._
import com.wajam.mry.api.Transport
import com.wajam.mry.execution.MapValue
import com.wajam.mry.execution.IntValue
import com.wajam.mry.api.Transport
import com.wajam.mry.execution.BoolValue
import com.wajam.mry.execution.ListValue
import com.wajam.mry.execution.StringValue
import com.wajam.mry.execution.DoubleValue

/**
 * Protocol buffers translator
 */
class ProtobufTranslator extends ProtocolTranslator {

  import ProtobufTranslator._

  def encodeTransaction(transaction: Transaction): Array[Byte] = {
    encodePTransaction(transaction).build().toByteArray
  }

  def decodeTransaction(data: Array[Byte]): Transaction = {
    decodePTransaction(PTransaction.parseFrom(data))
  }

  def encodeValue(value: Value): Array[Byte] = encodePValue(value).toByteArray

  def decodeValue(data: Array[Byte]): Value = decodePValue(PTransactionValue.parseFrom(data))

  def encodeAll(transport: Transport): Array[Byte] = {
    encodePTransport(transport).build().toByteArray
  }

  def decodeAll(data: Array[Byte]): Transport = {
    decodePTransport(PTransport.parseFrom(data))
  }
}

object ProtobufTranslator {

  private val pb2operation = Map(
    POperation.Type.GET ->  classOf[Operation.Get],
    POperation.Type.RETURN -> classOf[Operation.Return],
    POperation.Type.FROM -> classOf[Operation.From],
    POperation.Type.LIMIT -> classOf[Operation.Limit],
    POperation.Type.PROJECTION -> classOf[Operation.Projection],
    POperation.Type.SET -> classOf[Operation.Set],
    POperation.Type.DELETE -> classOf[Operation.Delete])

  private val operation2pb = pb2operation.map(_ swap)

  private def encodePTransport(transport: Transport): PTransport.Builder =  {
    val Transport(request, response) = transport

    val pTransport = PTransport.newBuilder()

    for (r <- request)
      pTransport.setRequest(encodePTransaction(r))

    for (v <- response)
      pTransport.addResponse(encodePValue(v))

    pTransport
  }

  private def decodePTransport(transport: PTransport): Transport =  {

    val request: Option[Transaction] =
      if (transport.hasRequest)
        Some(decodePTransaction(transport.getRequest))
      else
        None

    val response: Seq[Value] = transport.getResponseList.asScala.map(decodePValue(_)).toSeq

    Transport(request, response)
  }

  private def encodePTransaction(transaction: Transaction): PTransaction.Builder =  {
    val pTransaction = PTransaction.newBuilder()

    pTransaction.setId(transaction.id)
    pTransaction.setInnerBlock(encodePBlock(transaction))

    pTransaction
  }

  private def decodePTransaction(pTransaction: PTransaction): Transaction =  {

    val transaction = new Transaction()
    transaction.id = pTransaction.getId

    decodePBlock(transaction, pTransaction.getInnerBlock)

    transaction
  }


  private def encodePBlock(block: Block): PBlock =  {
    val pBlock = PBlock.newBuilder()

    pBlock.addAllVariables(block.variables.map(encodePVariable(_).build()).asJava)
    pBlock.addAllOperations(block.operations.map(encodePOperation(_).build).asJava)
    pBlock.setVarSeq(block.varSeq)

    pBlock.build()
  }

  private def decodePBlock(block: Block, pBlock: PBlock) {
    null
  }

  private def encodePVariable(variable: Variable): PVariable.Builder =  {
    null
  }

  private def decodePVariable(variable: PVariable): PVariable.Builder =  {
    null
  }


//  private def hackyWayToGetOperationValues(operation: Operation): OperationData = {
//    //    operation match {
//    //      case op: Operation.Return => new OperationData(op.from, null, null)
//    //      case op: Operation.From => new OperationData(op.from, null, null)
//    //      case op: Operation.Get => new OperationData(op.from, null, null)
//    //      case op: Operation.Set => new OperationData(op.from, null, null)
//    //      case op: Operation.Delete => new OperationData(op.from, null, null)
//    //      case op: Operation.Limit => new OperationData(op.from, null, null)
//    //      case op: Operation.Projection => new OperationData(op.from, null, null)
//    //    }
//
//    null
//  }

  private def encodePObject(obj: Object): PObject.Builder = {
    null
  }

  private def encodePOperationWithIntoAndObjects(pOperation: POperation.Builder, into: Variable, objects: Seq[Object]):  POperation.Builder = {
    val inner = POperationWithIntoAndObjects.newBuilder()
    pOperation.setOperationWithIntoAndObjects(inner)

    inner.setInto(encodePVariable(into))
    inner.addAllObjects(objects.map(encodePObject(_).build).asJava)

    pOperation
  }

  private def encodeAnIntoPOperationWithFrom(pOperation: POperation.Builder, operation: Operation with Operation.WithFrom): POperation.Builder =  {
    val inner = POperationWithFrom.newBuilder()
    pOperation.setOperationWithFrom(inner)

    inner.addAllFrom(operation.from.map(encodePVariable(_).build).asJava)

    pOperation
  }

  private def getPOperationType(operation: Operation) = {
    operation2pb.find(_.getClass.getCanonicalName == operation.getClass.getCanonicalName) match {
      case pType: POperation.Type => pType
      case None => throw new RuntimeException("Unmapped operation, please verify ProtobufTranslator mappings.")
    }
  }

  private def encodePOperation(operation: Operation): POperation.Builder =  {

    val pOperation = POperation.newBuilder()

    pOperation.setType(getPOperationType(operation))

    operation match {
      case op: Operation.WithIntoAndData => encodePOperationWithIntoAndObjects(pOperation, op.into, op.data)
      case op: Operation.WithIntoAndKeys => encodePOperationWithIntoAndObjects(pOperation, op.into, op.keys)
      case op: Operation.WithFrom => encodeAnIntoPOperationWithFrom(pOperation, op)
    }
  }

  private def decodePOperation(operation: POperation): Operation = {
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
        for (protoVal <- protoMap.getValuesList.asScala) {
          map += (protoVal.getKey -> this.decodePValue(protoVal.getValue))
        }
        new MapValue(map)

      case PTransactionValue.Type.ARRAY =>
        val protoMap = protoVal.getMap
        val list = for {
          protoVal <- protoMap.getValuesList.asScala
        } yield this.decodePValue(protoVal.getValue)

        new ListValue(list)
      case PTransactionValue.Type.NULL =>
        NullValue

      case _ =>
        throw new TranslationException("Unsupported protobuf value type '%s'".format(protoVal.getType))
    }
  }

}
