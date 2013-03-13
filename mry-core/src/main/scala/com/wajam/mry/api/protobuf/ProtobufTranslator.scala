package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import scala.collection.JavaConversions._
import com.wajam.mry.execution._
import com.wajam.mry.execution.Transaction
import com.wajam.mry.api.protobuf.MryProtobuf._
import com.wajam.mry.api.Transport

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

  def encodeValue(value: Value): Array[Byte] = encodePValue(value).build.toByteArray

  def decodeValue(data: Array[Byte]): Value = decodePValue(PTransactionValue.parseFrom(data))

  def encodeAll(transport: Transport): Array[Byte] = {
    encodePTransport(transport).build().toByteArray
  }

  def decodeAll(data: Array[Byte]): Transport = {
    decodePTransport(PTransport.parseFrom(data))
  }
}

object ProtobufTranslator {

  private def encodePTransport(transport: Transport): PTransport.Builder =  {
    val Transport(request, response) = transport

    val pTransport = PTransport.newBuilder()

    for (r <- request)
      pTransport.setRequest(encodePTransaction(r))

    for (r <- response)
      pTransport.addResponse(encodePValue(r))

    pTransport
  }

  private def decodePTransport(transport: PTransport): Transport =  {

    val request: Option[Transaction] =
      if (transport.hasRequest)
        Some(decodePTransaction(transport.getRequest))
      else
        None

    val response: Seq[Value] = transport.getResponseList.map(decodePValue(_)).toSeq

    Transport(request, response)
  }

//  private def encodePValue(value: Value): PTransactionValue = {
//    null
//  }
//
//  private def decodePValue(protoVal: PTransactionValue): Value = {
//    null
//  }

  private def encodePObject(obj: Object): PObject.Builder = {

    val pObj = PObject.newBuilder()

    obj match {
      case variable: Variable => {
        pObj.setExtension(PVariable.variable, encodePVariable(variable).build)
      }
      case value: Value => {
        pObj.setExtension(PTransactionValue.value, encodePValue(value).build)
      }
    }

    pObj
  }

  private def decodePObject(pObj: PObject): Object = {
    null
  }

  private def encodePTransaction(transaction: Transaction): PTransaction.Builder =  {
    val pTransaction = PTransaction.newBuilder()

    pTransaction.setId(transaction.id)

    val pBlock = encodePBlock(transaction)

    pBlock.setExtension(PTransaction.transaction, pTransaction.build)

    pTransaction
  }

  private def decodePTransaction(pTransaction: PTransaction): Transaction =  {

    val transaction = new Transaction()
    transaction.id = pTransaction.getId

    transaction
  }

  private def encodePBlock(block: Block): PBlock.Builder =  {
    val pBlock = PBlock.newBuilder()

    pBlock.addAllVariables(block.variables.map(encodePVariable(_).build()))
    pBlock.addAllOperations(block.operations.map(encodePOperation(_).build))
    pBlock.setVarSeq(block.varSeq)

    pBlock
  }

  private def decodePBlock(block: Block, pBlock: PBlock) {
    null
  }

  private def encodePVariable(variable: Variable):PVariable.Builder =  {
    val pVariable = PVariable.newBuilder()

    // DO NOT serialize sourceBlock, it will be available at reconstruction
    // has parent of this variable

    pVariable.setId(variable.id)
    pVariable.setValue(encodePValue(variable.value))
  }

  private def decodePVariable(pVariable: PVariable): Variable =  {
    null
  }

  private def encodePOperation(operation: Operation): POperation.Builder =  {

    import Operation._

    val pOperation = POperation.newBuilder()

    val withFrom = (op: WithFrom) =>
      pOperation.addAllVariables(op.from.map(encodePVariable(_).build))

    val WithIntoAndKeys = (op: WithIntoAndKeys) =>
      pOperation.addVariables(encodePVariable(op.into)).addAllObjects(op.keys.map(encodePObject(_).build))

    val WithIntoAndData = (op: WithIntoAndData) =>
      pOperation.addVariables(encodePVariable(op.into)).addAllObjects(op.data.map(encodePObject(_).build))

    operation match {
      case op: Return with WithFrom => withFrom(op); pOperation.setExtension(PReturn.ret, PReturn.getDefaultInstance)
      case op: From with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setExtension(PFrom.from, PFrom.getDefaultInstance)
      case op: Get with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setExtension(PGet.get, PGet.getDefaultInstance)
      case op: Set with WithIntoAndData => WithIntoAndData(op); pOperation.setExtension(PSet.set, PSet.getDefaultInstance)
      case op: Delete with WithIntoAndData => WithIntoAndData(op); pOperation.setExtension(PDelete.delete, PDelete.getDefaultInstance)
      case op: Limit with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setExtension(PLimit.limit, PLimit.getDefaultInstance)
      case op: Projection with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setExtension(PProjection.projection, PProjection.getDefaultInstance)
    }
  }

  private def decodePOperation(pOperation: POperation): Operation = {

    val os: OperationSource = null

    val operationExtensions = Seq(
      PReturn.ret,
      PFrom.from,
      PGet.get,
      PSet.set,
      PDelete.delete,
      PLimit.limit,
      PProjection.projection
    )

    val msg = pOperation
    val concreteOperation = operationExtensions.find(msg.hasExtension(_)).map(msg.getExtension(_)).head

    val variables = pOperation.getVariablesList.map(decodePVariable(_))
    val objects = pOperation.getObjectsList.map(decodePObject(_)).toSeq

    concreteOperation match {
      case op: PReturn => new Operation.Return(os, variables)
      case op: PFrom => new Operation.From(os, variables(0), objects:_*)
      case op: PGet => new Operation.Get(os, variables(0), objects:_*)
      case op: PSet => new Operation.Set(os, variables(0), objects:_*)
      case op: PDelete => new Operation.Delete(os, variables(0), objects:_*)
      case op: PLimit => new Operation.Limit(os, variables(0), objects:_*)
      case op: PProjection => new Operation.Projection(os, variables(0), objects:_*)
    }
  }

  private def encodePValue(value: Value): PTransactionValue.Builder = {
    value.serializableValue match {
      case strValue: StringValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.STRING)
          .setStringValue(strValue.strValue)

      case intValue: IntValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.INT)
          .setIntValue(intValue.intValue)

      case doubleValue: DoubleValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.DOUBLE)
          .setDoubleValue(doubleValue.doubleValue)

      case boolValue: BoolValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.BOOL)
          .setBoolValue(boolValue.boolValue)

      case listValue: ListValue =>
        val collection = PTransactionCollection.newBuilder()

        for (v <- listValue.listValue) {
          val protoVal = this.encodePValue(v)
          collection.addValues(PTransactionCollectionValue.newBuilder().setValue(protoVal).build())
        }

        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.ARRAY)
          .setMap(collection)

      case mapValue: MapValue =>
        val collection = PTransactionCollection.newBuilder()

        for ((k, v) <- mapValue.mapValue) {
          val protoVal = this.encodePValue(v)
          collection.addValues(PTransactionCollectionValue.newBuilder().setKey(k).setValue(protoVal).build())
        }

        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.MAP)
          .setMap(collection)

      case _: NullValue =>
        PTransactionValue.newBuilder()
          .setType(PTransactionValue.Type.NULL)

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

