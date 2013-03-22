package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import scala.collection.JavaConversions._
import com.wajam.mry.execution._
import com.wajam.mry.execution.Transaction
import com.wajam.mry.api.protobuf.MryProtobuf._
import com.wajam.mry.api.Transport
import collection.immutable.HashMap

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

  val pb2obj = new collection.mutable.HashMap[Int, AnyRef] // Encoded address to decoded instance mapping
  val obj2pb = new collection.mutable.HashMap[AnyRef, Int] // Live instance to encoded address mapping

  private def registerEncodedData(pbAddress: Int, instance: AnyRef) = {
    obj2pb += instance -> pbAddress
  }

  private def registerDecodedData(pbAddress: Int, instance: AnyRef) = {
    pb2obj += pbAddress -> instance
  }

  private def getEncodedId(instance: AnyRef): Int = {
    // Compare by memory address
    obj2pb.find(instance eq _._1).get._2
  }

  private def getDecodedData[T](pbAddress: Int): T = {
    pb2obj(pbAddress).asInstanceOf[T]
  }

  private def addToHeap(mryData: AnyRef): java.lang.Integer = {

    val pMryData = PMryData.newBuilder()

    mryData match {
      case value: PTransaction.Builder => pMryData.setTransaction(value)
      case value: PBlock.Builder => pMryData.setBlock(value)
      case value: PVariable.Builder => pMryData.setVariable(value)
      case value: POperation.Builder => pMryData.setOperation(value)
      case value: PTransactionValue.Builder => pMryData.setValue(value)
    }

    pHeap.addValues(pMryData)

    val instanceAddress = currentAddress
    currentAddress += 1
    instanceAddress
  }

  private def getFromHeap[T](address: Int): T = {

    val pMryData = pHeap.getValues(address)

    pMryData match {
      case pMryData if pMryData.hasTransaction => pMryData.getTransaction
      case pMryData if pMryData.hasBlock => pMryData.getBlock
      case pMryData if pMryData.hasVariable => pMryData.getVariable
      case pMryData if pMryData.hasOperation => pMryData.getOperation
      case pMryData if pMryData.hasValue => pMryData.getValue
    }

    pMryData.asInstanceOf[T]
  }

  def decodeHeapToPool(heap: PHeap) = {

     null
  }

  def decodeBlockBoundsFromHeapToPool(block: Block, heap: PHeap) = {

    // Yield map with address
    val mappedData = heap.getValuesList.zipWithIndex

    // Decode values and register them to pool
    val values = mappedData.filter(_._1.hasValue).map((v) => v._2 -> decodePValue(v._1.getValue))
    values.foreach((v) => registerDecodedData(v._1, v._2))

    // Decode variables and register them to pool
    val variables = mappedData.filter(_._1.hasVariable).map((v) => v._2 -> decodePVariable(block, v._1.getVariable))
    variables.foreach((v) => registerDecodedData(v._1, v._2))

    // Decode variables and register them to pool
    val operations = mappedData.filter(_._1.hasOperation).map((v) => v._2 -> decodePOperation(block, v._1.getOperation))
    operations.foreach((v) => registerDecodedData(v._1, v._2))
  }

  def encodePTransport(transport: Transport): PTransport.Builder =  {
    val Transport(request, response) = transport

    val pTransport = PTransport.newBuilder()

    pTransport.setHeap(pHeap)

    for (r <- request) {
      // ReserveId here
      val trx = encodePTransaction(r)
      pTransport.setRequestAddress(addToHeap(trx))
    }

    for (r <- response) {
      val value = encodePValue(r)
      pTransport.addResponseAddresses(addToHeap(value))
    }

    pTransport
  }

  def decodePTransport(transport: PTransport): Transport = {

    val request: Option[Transaction] =
      if (transport.hasRequestAddress)
        Some(decodePTransaction(getFromHeap[PTransaction](transport.getRequestAddress)))
      else
        None

    val response: Seq[Value] =
      transport.getResponseAddressesList
        .map(getFromHeap[PTransactionValue](_))
        .map(decodePValue(_)).toSeq

    Transport(request, response)
  }

  def encodePObject(obj: Object): java.lang.Integer = {

    obj match {
      case variable: Variable => {
        addToHeap(encodePVariable(variable))
      }
      case value: Value => {
        addToHeap(encodePValue(value))
      }
    }
  }

  def decodePObject(block: Block, objAddress: Int): Object = {
    val pObj = getFromHeap[AnyRef](objAddress)

    pObj match {
      case pVar: PVariable => decodePVariable(block, pVar)
      case pValue: PTransactionValue => decodePValue(pValue)
    }
  }

  def encodePTransaction(transaction: Transaction): PTransaction.Builder =  {
    val pTransaction = PTransaction.newBuilder()

    pTransaction.setId(transaction.id)

    val pBlock = addToHeap(encodePBlock(transaction))

    pTransaction.setBlockAddress(pBlock)

    pTransaction
  }

  def decodePTransaction(pTransaction: PTransaction): Transaction =  {

    val transaction = new Transaction()

    transaction.id = pTransaction.getId

    val pBlock = getFromHeap[PBlock](pTransaction.getBlockAddress)

    decodePBlock(transaction, pBlock)

    transaction
  }

  def encodePBlock(block: Block): PBlock.Builder =  {
    val pBlock = PBlock.newBuilder()

    pBlock.addAllVariableAddresses(block.variables.map(encodePVariable(_)).map(addToHeap(_)))
    pBlock.addAllOperationAddresses(block.operations.map(encodePOperation(_)).map(addToHeap(_)))
    pBlock.setVarSeq(block.varSeq)

    pBlock
  }

  def decodePBlock(block: Block, pBlock: PBlock) {

    block.varSeq = pBlock.getVarSeq

    block.operations = pBlock.getOperationAddressesList().map(getFromHeap(_)).map(decodePOperation(block, _)).toList
    block.variables = pBlock.getVariableAddressesList().map(getFromHeap(_)).map(decodePVariable(block, _)).toList
  }

  def encodePVariable(variable: Variable):PVariable.Builder =  {
    val pVariable = PVariable.newBuilder()

    // DO NOT serialize sourceBlock, it will be available at reconstruction
    // has parent of this variable

    pVariable.setId(variable.id)
    pVariable.setValueAddress(addToHeap(encodePValue(variable.value)))
  }

  def decodePVariable(parentBlock: Block, pVariable: PVariable): Variable =  {

    new Variable(parentBlock, pVariable.getId, decodePValue(getFromHeap[PTransactionValue](pVariable.getValueAddress)))
  }

  def encodePOperation(operation: Operation): POperation.Builder =  {

    import Operation._

    val pOperation = POperation.newBuilder()

    val withFrom = (op: WithFrom) =>
      pOperation.addAllVariableAddresses(op.from.map(encodePVariable(_)).map(addToHeap(_)))

    val WithIntoAndKeys = (op: WithIntoAndKeys) =>
      pOperation
        .addVariableAddresses(addToHeap(encodePVariable(op.into)))
        .addAllObjectAddresses(op.keys.map(encodePObject(_)))

    val WithIntoAndData = (op: WithIntoAndData) =>
      pOperation
        .addVariableAddresses(addToHeap(encodePVariable(op.into)))
        .addAllObjectAddresses(op.data.map(encodePObject(_)))

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

  def decodePOperation(block: Block, pOperation: POperation): Operation = {

    val os: OperationSource = getDecodedData[OperationSource](pOperation.getSourceAddress)

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

    val variables = pOperation.getVariableAddressesList.map(getFromHeap[PVariable](_)).map(decodePVariable(block, _))
    val objects = pOperation.getObjectAddressesList.map(decodePObject(block, _)).toSeq

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

