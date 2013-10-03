package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import scala.collection.JavaConversions._
import com.wajam.mry.execution._
import com.wajam.mry.api.protobuf.MryProtobuf._
import com.wajam.mry.api.Transport

/**
 * Protocol buffers translator
 */
class ProtobufTranslator extends ProtocolTranslator {

  def encodeTransaction(transaction: Transaction): Array[Byte] = {

    val transport = new Transport(Some(transaction), None)

    encodeAll(transport)
  }

  def decodeTransaction(data: Array[Byte]): Transaction = {

    val transport = decodeAll(data)
    transport.transaction.get
  }

  /* EncodeValue and DecodeValue doesn't use PHeap, it's for legacy reason (all data in db use that format)
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

/**
 * Allow stateful and threadsafe operation for ProtobufTranslator by instancing this class at every call.
 * It also keep ProtobufTranslator class more readable and more relevant to it's parent interface.
 *
 */
private class InternalProtobufTranslator {

  private var currentHeapId = 1 // I don't think will have more than 2^31 object, skip 0, since 0==unassigned

  private val pb2obj = new collection.mutable.HashMap[Int, AnyRef] // Encoded HeapId to decoded instance mapping
  private val obj2pb = new collection.mutable.HashMap[AnyRef, Int] // Live instance to encoded HeapId mapping

  // Used by encoding and decoding as temporary storage for encoded/decoded object
  private var tempHeap = new collection.mutable.ListBuffer[AnyRef]

  private def registerEncodedData(pbHeapId: Int, instance: AnyRef) = {
    obj2pb += instance -> pbHeapId
  }

  private def registerDecodedData(pbHeapId: Int, instance: AnyRef) = {
    pb2obj += pbHeapId -> instance
  }

  private def getHeapIpForEncodedData(instance: AnyRef): Int = {

    // Compare by memory HeapId
    obj2pb.find(instance eq _._1).get._2
  }

  private def getEntityFromDecodedData[T](HeapId: Int): T = {

    checkHeapId(HeapId)

    pb2obj(HeapId).asInstanceOf[T]
  }

  private def checkHeapId(HeapId: Int) {
    if (HeapId == 0)
      throw new IndexOutOfBoundsException("Invalid HeapId, it's unassigned")
  }

  private def buildMryData(mryData: AnyRef): PMryData.Builder = {
    val pMryData = PMryData.newBuilder()

    mryData match {
      case value: PTransaction.Builder => pMryData.setTransaction(value)
      case value: PBlock.Builder => pMryData.setBlock(value)
      case value: PVariable.Builder => pMryData.setVariable(value)
      case value: POperation.Builder => pMryData.setOperation(value)
      case value: PTransactionValue.Builder => pMryData.setValue(value)
    }

    pMryData
  }

  private def addToHeap(mryData: AnyRef): Int = {

    val instanceHeapId = currentHeapId

    tempHeap += mryData

    currentHeapId += 1
    instanceHeapId
  }

  private def encodeHeap(): PHeap.Builder = {

    val pEncodingHeap = PHeap.newBuilder()
    tempHeap.foreach((v) => pEncodingHeap.addValues(buildMryData(v)))
    pEncodingHeap
  }

  private def getFromHeap[T](HeapId: Int): T = {

    checkHeapId(HeapId)

    tempHeap(HeapId - 1).asInstanceOf[T]
  }

  private def loadHeap(pDecodingHeap: PHeap) = {

      var addr = 1

      for (pMryData <- pDecodingHeap.getValuesList) {

        val value = pMryData match {
          case pMryData if pMryData.hasTransaction => pMryData.getTransaction
          case pMryData if pMryData.hasBlock => pMryData.getBlock
          case pMryData if pMryData.hasVariable => pMryData.getVariable
          case pMryData if pMryData.hasOperation => pMryData.getOperation
          case pMryData if pMryData.hasValue => pMryData.getValue
        }

        tempHeap += value
        addr += 1
      }
  }

  private def decodeBlockFromHeapToPool(block: Block) = {

    // Yield map with HeapId
    val mappedData = Range(1, tempHeap.size + 1).zip(tempHeap)

    /* Order here is import. DO ALL values, then ALL variable, then ALL operation */

    // Decode values and register them to pool
    mappedData.collect {
      case (key, value: PTransactionValue) => registerDecodedData(key, decodePValue(value))
    }

    // Decode variables and register them to pool
    mappedData.collect {
      case (key, value: PVariable) => registerDecodedData(key, decodePVariable(block, value))
    }

    // Decode variables and register them to pool
    mappedData.collect {
      case (key, value: POperation) => registerDecodedData(key, decodePOperation(block, value))
    }
  }

  def encodePTransport(transport: Transport): PTransport.Builder =  {

    import PTransport.Type

    val pTransport = PTransport.newBuilder()

    transport match {
      case Transport(None, None) => {
        pTransport.setType(Type.Empty)
      }

      case Transport(Some(transaction), None) => {
        pTransport.setType(Type.Transaction)

        val pTrans = encodePTransaction(transaction)
        pTransport.setRequestHeapId(pTrans)

      }

      case Transport(None, Some(values)) => {
        pTransport.setType(Type.Values)

        for (v <- values) {
          val value = encodePValue(v)
          pTransport.addResponseHeapIds(addToHeap(value))
        }
      }

      case _ =>
        throw new RuntimeException("Too many different data types in transport, only one at the time is supported.")
    }

    pTransport.setHeap(encodeHeap())

    pTransport
  }

  def decodePTransport(transport: PTransport): Transport = {

    import PTransport.Type

    loadHeap(transport.getHeap)

    val transaction =
      if (transport.getType == Type.Transaction)
        Some(decodePTransaction(transport))
      else
        None

    val values =
      if (transport.getType == Type.Values)
        Some(
          transport.getResponseHeapIdsList
          .map(value => (getFromHeap[PTransactionValue] _ andThen decodePValue _)(value)).toSeq)
      else
        None

    Transport(transaction, values)
  }

  private def encodePObject(obj: Object): Int = {

    obj match {
      case variable: Variable => {

        // Variable are already listed in the parent block, so don't duplicate
        getHeapIpForEncodedData(variable)
      }
      case value: Value => {
        addToHeap(encodePValue(value))
      }
    }
  }

  private def encodePTransaction(transaction: Transaction): Int =  {

    val pTransaction = PTransaction.newBuilder()
    val HeapId = addToHeap(pTransaction)
    registerEncodedData(HeapId, transaction)

    pTransaction.setId(transaction.id)

    val pBlock = addToHeap(encodePBlock(transaction))

    pTransaction.setBlockHeapId(pBlock)

    HeapId
  }

  private def decodePTransaction(transport: PTransport): Transaction =  {

    // Create an transaction instance, and register it in the pool
    val trx = new Transaction()
    registerDecodedData(transport.getRequestHeapId, trx)

    // Decode and create object into pool (to get them by reference later)
    decodeBlockFromHeapToPool(trx)

    // Finish decoding the transaction since now, all dependant objects are live in memory
    val pTransaction = getFromHeap[PTransaction](transport.getRequestHeapId)
    fillPTransaction(trx, pTransaction)
    trx
  }

  private def fillPTransaction(transaction: Transaction, pTransaction: PTransaction) {

    transaction.id = pTransaction.getId

    val pBlock = getFromHeap[PBlock](pTransaction.getBlockHeapId)

    decodePBlock(transaction, pBlock)
  }

  private def encodePBlock(block: Block): PBlock.Builder =  {
    val pBlock = PBlock.newBuilder()

    val varId = block.variables.map(encodePVariable(_))

    varId.zipWithIndex.foreach((v) => registerEncodedData(v._1, block.variables(v._2)))
    pBlock.addAllVariableHeapIds(varId.map(_.asInstanceOf[java.lang.Integer]))

    pBlock.addAllOperationHeapIds(block.operations.map(encodePOperation(_)).map(addToHeap(_)).map(_.asInstanceOf[java.lang.Integer]))
    pBlock.setVarSeq(block.varSeq)

    pBlock
  }

  private def decodePBlock(block: Block, pBlock: PBlock) {

    block.varSeq = pBlock.getVarSeq

    block.operations ++= pBlock.getOperationHeapIdsList().map(getEntityFromDecodedData[Operation](_))
    block.variables ++= pBlock.getVariableHeapIdsList().map(getEntityFromDecodedData[Variable](_))
  }

  private def encodePVariable(variable: Variable): Int =  {
    val pVariable = PVariable.newBuilder()

    // DO NOT serialize sourceBlock, it will be available at reconstruction
    // has parent of this variable

    pVariable.setId(variable.id)
    pVariable.setValueHeapId(addToHeap(encodePValue(variable.value)))

    val addr = addToHeap(pVariable)
    registerEncodedData(addr, variable)
    addr
  }

  private def decodePVariable(parentBlock: Block, pVariable: PVariable): Variable =  {
    new Variable(parentBlock, pVariable.getId, getEntityFromDecodedData[Value](pVariable.getValueHeapId))

  }

  private def encodePOperation(operation: Operation): POperation.Builder =  {

    import POperation.Type

    import Operation._

    val pOperation = POperation.newBuilder()

    pOperation.setSourceHeapId(getHeapIpForEncodedData(operation.source))

    val withFrom = (from: Seq[Variable]) =>
      // Variable are already listed in the parent block, so don't duplicate
      pOperation.addAllVariableHeapIds(from.map(getHeapIpForEncodedData(_).asInstanceOf[java.lang.Integer]))

    val withIntoAndSeqObject = (into: Variable, objects: Seq[Object]) =>
      pOperation
        .addVariableHeapIds(getHeapIpForEncodedData(into)) // Variable are already listed in the parent block, so don't duplicate
        .addAllObjectHeapIds(objects.map(encodePObject(_).asInstanceOf[java.lang.Integer]))

    val withFilter = (op: Filter) => {

      val objs: List[Object] = List(op.key, StringValue(op.filter.toString), op.value)

      pOperation
        .addVariableHeapIds(getHeapIpForEncodedData(op.into)) // Variable are already listed in the parent block, so don't duplicate
        .addAllObjectHeapIds(objs.map(encodePObject(_).asInstanceOf[java.lang.Integer]))
    }

    operation match {
      case op: Return => withFrom(op.from); pOperation.setType(Type.Return)
      case op: From => withIntoAndSeqObject(op.into, op.keys); pOperation.setType(Type.From)
      case op: Get => withIntoAndSeqObject(op.into, op.keys); pOperation.setType(Type.Get)
      case op: Set => withIntoAndSeqObject(op.into, op.data); pOperation.setType(Type.Set)
      case op: Delete => withIntoAndSeqObject(op.into, op.data);; pOperation.setType(Type.Delete)
      case op: Limit => withIntoAndSeqObject(op.into, op.keys); pOperation.setType(Type.Limit)
      case op: Projection => withIntoAndSeqObject(op.into, op.keys); pOperation.setType(Type.Projection)
      case op: Filter => withFilter(op); pOperation.setType(Type.Filter)
    }

    pOperation
  }

  private def decodePOperation(block: Block, pOperation: POperation): Operation = {

    import POperation.Type

    val os: OperationSource = getEntityFromDecodedData[OperationSource](pOperation.getSourceHeapId)

    val variables = pOperation.getVariableHeapIdsList.map(getEntityFromDecodedData[Variable](_))
    val objects = pOperation.getObjectHeapIdsList.map(getEntityFromDecodedData[Object](_)).toSeq

    val buildFilter = () => {
      val StringValue(filter) = objects(1)
      new Operation.Filter(os, variables(0), objects(0), MryFilters.withName(filter), objects(2))
    }

    pOperation.getType match {
      case Type.Return => new Operation.Return(os, variables)
      case Type.From => new Operation.From(os, variables(0), objects:_*)
      case Type.Get => new Operation.Get(os, variables(0), objects:_*)
      case Type.Set => new Operation.Set(os, variables(0), objects:_*)
      case Type.Delete => new Operation.Delete(os, variables(0), objects:_*)
      case Type.Limit => new Operation.Limit(os, variables(0), objects:_*)
      case Type.Projection => new Operation.Projection(os, variables(0), objects:_*)
      case Type.Filter => buildFilter()
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

