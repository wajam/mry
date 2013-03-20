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

  def encodeTransaction(transaction: Transaction): Array[Byte] = {

    val tra = new InternalProtobufTranslator()

    tra.encodePTransaction(transaction).build().toByteArray
  }

  def decodeTransaction(data: Array[Byte]): Transaction = {

    val tra = new InternalProtobufTranslator()

    tra.decodePTransaction(PTransaction.parseFrom(data))
  }

  /* EncodeValue and DecodeValue doesn't used PHead, it's for legacy reason (all data in db use that format)
     and because they don't need it*/

  def encodeValue(value: Value): Array[Byte] = {

    val tra = new InternalProtobufTranslator()

    tra.encodePValue(value).build.toByteArray
  }

  def decodeValue(data: Array[Byte]): Value = {

    val tra = new InternalProtobufTranslator()

    tra.decodePValue(PTransactionValue.parseFrom(data))
  }

  def encodeAll(transport: Transport): Array[Byte] = {

    val tra = new InternalProtobufTranslator()

     tra.encodePTransport(transport).build().toByteArray
  }

  def decodeAll(data: Array[Byte]): Transport = {

    val tra = new InternalProtobufTranslator()

    tra.decodePTransport(PTransport.parseFrom(data))
  }
}

private class InternalProtobufTranslator {

  val pHeap = PHeap.newBuilder()
  var currentAddress = 0 // I don't think will have more than 2^31 object

  private def addToHeap(mryData: AnyRef): Int = {

    val pMryData = PMryData.newBuilder()

    mryData match {
      case value: PTransaction => pMryData.setTransaction(value)
      case value: PBlock => pMryData.setBlock(value)
      case value: PVariable => pMryData.setVariable(value)
      case value: POperation => pMryData.setOperation(value)
      case value: PTransactionValue => pMryData.setValue(value)
    }

    pHeap.addValues(pMryData)

    currentAddress += 1
    currentAddress
  }

  def encodePTransport(transport: Transport): PTransport.Builder =  {
    val Transport(request, response) = transport

    val pTransport = PTransport.newBuilder()

    pTransport.setHeap(pHeap)

    for (r <- request) {
      val trx = encodePTransaction(r)
      pTransport.setRequestAddress(addToHeap(trx))
    }

    for (r <- response) {
      val value = encodePValue(r)
      pTransport.addResponseAddress(addToHeap(value))
    }

    pTransport
  }

  def decodePTransport(transport: PTransport): Transport = {

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

  def encodePObject(obj: Object): PObject.Builder = {

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

  def decodePObject(pObj: PObject): Object = {
    throw new RuntimeException("Not implemented")
  }

  def encodePTransaction(transaction: Transaction): PTransaction.Builder =  {
    val pTransaction = PTransaction.newBuilder()

    pTransaction.setId(transaction.id)

    val pBlock = encodePBlock(transaction)

    pBlock.setExtension(PTransaction.transaction, pTransaction.build)

    pTransaction
  }

  def decodePTransaction(pTransaction: PTransaction): Transaction =  {

    val transaction = new Transaction()
    transaction.id = pTransaction.getId
    val block =  pTransaction.getExtensiosn
    decodePBlock(transaction,)

    transaction
  }

  def encodePBlock(block: Block): PBlock.Builder =  {
    val pBlock = PBlock.newBuilder()

    pBlock.addAllVariables(block.variables.map(encodePVariable(_).build()))
    pBlock.addAllOperations(block.operations.map(encodePOperation(_).build))
    pBlock.setVarSeq(block.varSeq)

    pBlock
  }

  def decodePBlock(block: Block, pBlock: PBlock) {
    block.operations = pBlock.getOperationsList().map(decodePOperation(_)).toList
    block.variables = pBlock.getVariablesList().map(decodePVariable(_)).toList

    block.varSeq = pBlock.getVarSeq
  }

  def encodePVariable(variable: Variable):PVariable.Builder =  {
    val pVariable = PVariable.newBuilder()

    // DO NOT serialize sourceBlock, it will be available at reconstruction
    // has parent of this variable

    pVariable.setId(variable.id)
    pVariable.setValue(encodePValue(variable.value))
  }

  def decodePVariable(pVariable: PVariable): Variable =  {
    throw new RuntimeException("Not implemented")
  }

  def encodePOperation(operation: Operation): POperation.Builder =  {

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

  def decodePOperation(pOperation: POperation): Operation = {

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

  def encodePValue(value: Value): PTransactionValue.Builder = {
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

  def decodePValue(protoVal: PTransactionValue): Value = {
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

