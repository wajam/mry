package com.wajam.mry.api.protobuf

import com.wajam.mry.api.{TranslationException, ProtocolTranslator}
import scala.collection.JavaConversions._
import com.wajam.mry.execution._
import com.wajam.mry.execution.Transaction
import com.wajam.mry.api.protobuf.MryProtobuf._
import com.wajam.mry.api.Transport
import scala.collection.JavaConverters._

/**
 * Protocol buffers translator
 */
class ProtobufTranslator extends ProtocolTranslator {

  def encodeTransaction(transaction: Transaction): Array[Byte] = {

    val transport = new Transport(Some(transaction), Seq())

    encodeAll(transport)
  }

  def decodeTransaction(data: Array[Byte]): Transaction = {

    val transport = decodeAll(data)
    transport.request.get
  }

  /* EncodeValue and DecodeValue doesn't use PHead, it's for legacy reason (all data in db use that format)
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

    for (trx <- transport.request) {
      System.out.println(trx.printTree("Encoding"))
    }

    tra.encodePTransport(transport).build().toByteArray
  }

  def decodeAll(data: Array[Byte]): Transport = {

    val tra = new InternalProtobufTranslator()

    val transport = tra.decodePTransport(PTransport.parseFrom(data))

    for (trx <- transport.request) {
      System.out.println(trx.printTree("Decoded"))
    }

    transport
  }
}

private class InternalProtobufTranslator {

  private var currentAddress = 1 // I don't think will have more than 2^31 object, skip 0, since 0==unassigned

  private val pb2obj = new collection.mutable.HashMap[Int, AnyRef] // Encoded address to decoded instance mapping
  private val obj2pb = new collection.mutable.HashMap[AnyRef, Int] // Live instance to encoded address mapping

  private var tempEncodingHeap = new collection.immutable.TreeMap[Int, PMryData.Builder]
  private var tempDecodingHeap = new collection.immutable.TreeMap[Int, AnyRef]

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

  private def getDecodedData[T](address: Int): T = {

    checkAddress(address)

    pb2obj(address).asInstanceOf[T]
  }

  private def checkAddress(address: Int) {
    if (address == 0)
      throw new IndexOutOfBoundsException("Invalid address, it's unassigned")
  }

  private def reserveAddress() = {
    val instanceAddress = currentAddress
    System.out.println("Id: " + instanceAddress)
    currentAddress += 1
    instanceAddress
  }

  private def addToHeap(address: Int, mryData: AnyRef): Int = {

    val pMryData = PMryData.newBuilder()

    mryData match {
      case value: PTransaction.Builder => pMryData.setTransaction(value)
      case value: PBlock.Builder => pMryData.setBlock(value)
      case value: PVariable.Builder => pMryData.setVariable(value)
      case value: POperation.Builder => pMryData.setOperation(value)
      case value: PTransactionValue.Builder => pMryData.setValue(value)
    }

    tempEncodingHeap += (address -> pMryData)
    address
  }

  private def encodeHeap(): PHeap.Builder = {

    val pEncodingHeap = PHeap.newBuilder()
    tempEncodingHeap.foreach((kv) => pEncodingHeap.addValues(kv._2))
    pEncodingHeap
  }

  private def addToHeap(mryData: AnyRef): java.lang.Integer = {

    addToHeap(reserveAddress(), mryData)
  }

  private def getFromHeap[T](address: Int): T = {

    checkAddress(address)

    tempDecodingHeap(address).asInstanceOf[T]
  }

  def loadHeap(pDecodingHeap: PHeap) = {

      var addr = 1

      for (pMryData <- pDecodingHeap.getValuesList) {

        val value = pMryData match {
          case pMryData if pMryData.hasTransaction => pMryData.getTransaction
          case pMryData if pMryData.hasBlock => pMryData.getBlock
          case pMryData if pMryData.hasVariable => pMryData.getVariable
          case pMryData if pMryData.hasOperation => pMryData.getOperation
          case pMryData if pMryData.hasValue => pMryData.getValue
        }

        tempDecodingHeap += (addr -> value)
        addr += 1
    }
  }

  def decodeBlockBoundsFromHeapToPool(block: Block) = {

    // Yield map with address

    val mappedData = tempDecodingHeap

    // Decode values and register them to pool
    val values = mappedData
      .filter(_._2.isInstanceOf[PTransactionValue])
      .map((v) => v._1 -> decodePValue(v._2.asInstanceOf[PTransactionValue]))

    values.foreach((v) => registerDecodedData(v._1 , v._2))

    // Decode variables and register them to pool
    val variables = mappedData
      .filter(_._2.isInstanceOf[PVariable])
      .map((v) => v._1 -> decodePVariable(block, v._2.asInstanceOf[PVariable]))

    variables.foreach((v) => registerDecodedData(v._1, v._2))

    // Decode operations and register them to pool
    val operations = mappedData
      .filter(_._2.isInstanceOf[POperation])
      .map((v) => v._1 -> decodePOperation(block, v._2.asInstanceOf[POperation]))

    operations.foreach((v) => registerDecodedData(v._1, v._2))
  }

  def encodePTransport(transport: Transport): PTransport.Builder =  {
    val Transport(request, response) = transport

    val pTransport = PTransport.newBuilder()

    for (r <- request) {
      val pTrans = encodePTransaction(r)
      pTransport.setRequestAddress(pTrans)
    }

    for (r <- response) {
      val value = encodePValue(r)
      pTransport.addResponseAddresses(addToHeap(value))
    }

    pTransport.setHeap(encodeHeap())

    pTransport
  }

  def decodePTransport(transport: PTransport): Transport = {

    loadHeap(transport.getHeap)

    val request: Option[Transaction] =
      if (transport.hasRequestAddress)
      {
        Some(decodePTransaction(transport))
      }
      else
        None

    val response: Seq[Value] =
      transport.getResponseAddressesList
        .map(getFromHeap[PTransactionValue](_))
        .map(decodePValue(_)).toSeq

    Transport(request, response)
  }

  def encodePObject(obj: Object): Int = {

    obj match {
      case variable: Variable => {

        // Variable are already listed in the parent block, so don't duplicate
        getEncodedId(variable)
      }
      case value: Value => {
        addToHeap(encodePValue(value))
      }
    }
  }

  def decodePObject(block: Block, objAddress: Int): Object = {
    val pObj = getFromHeap[AnyRef](objAddress)

    pObj match {
      case pVar: PVariable => getDecodedData[Variable](objAddress)
      case pValue: PTransactionValue => getDecodedData[Value](objAddress)
    }
  }

  def encodePTransaction(transaction: Transaction): Int =  {

    val pTransaction = PTransaction.newBuilder()
    val address = reserveAddress()
    registerEncodedData(address, transaction)

    pTransaction.setId(transaction.id)

    val pBlock = addToHeap(encodePBlock(transaction))

    pTransaction.setBlockAddress(pBlock)

    addToHeap(address, pTransaction)

    address
  }

  def decodePTransaction(transport: PTransport): Transaction =  {

    // Create an transaction instance, and register it in the pool
    val trx = new Transaction()
    registerDecodedData(transport.getRequestAddress, trx)

    // Decode and create object into pool (to get them by reference later)
    decodeBlockBoundsFromHeapToPool(trx)

    // Finish decoding the transaction since now, all dependant objects are live in memory
    val pTransaction = getFromHeap[PTransaction](transport.getRequestAddress)
    fillPTransaction(trx, pTransaction)
    trx
  }

  def fillPTransaction(transaction: Transaction, pTransaction: PTransaction) {

    transaction.id = pTransaction.getId

    val pBlock = getFromHeap[PBlock](pTransaction.getBlockAddress)

    decodePBlock(transaction, pBlock)
  }

  def encodePBlock(block: Block): PBlock.Builder =  {
    val pBlock = PBlock.newBuilder()

    val varId = block.variables.map(encodePVariable(_))

    varId.zipWithIndex.foreach((v) => registerEncodedData(v._1, block.variables(v._2)))
    pBlock.addAllVariableAddresses(varId.map(_.asInstanceOf[java.lang.Integer]))

    pBlock.addAllOperationAddresses(block.operations.map(encodePOperation(_)).map(addToHeap(_)))
    pBlock.setVarSeq(block.varSeq)

    pBlock
  }

  def decodePBlock(block: Block, pBlock: PBlock) {

    block.varSeq = pBlock.getVarSeq

    block.operations ++= pBlock.getOperationAddressesList().map(getDecodedData[Operation](_))
    block.variables ++= pBlock.getVariableAddressesList().map(getDecodedData[Variable](_))
  }

  def encodePVariable(variable: Variable): Int =  {
    val pVariable = PVariable.newBuilder()

    // DO NOT serialize sourceBlock, it will be available at reconstruction
    // has parent of this variable

    pVariable.setId(variable.id)
    pVariable.setValueAddress(addToHeap(encodePValue(variable.value)))

    val addr = addToHeap(pVariable)
    registerEncodedData(addr, variable)
    addr
  }

  def decodePVariable(parentBlock: Block, pVariable: PVariable): Variable =  {
    System.out.println("Called!")
    new Variable(parentBlock, pVariable.getId, getDecodedData[Value](pVariable.getValueAddress))

  }

  def encodePOperation(operation: Operation): POperation.Builder =  {

    import POperation.Type

    import Operation._

    val pOperation = POperation.newBuilder()

    pOperation.setSourceAddress(getEncodedId(operation.source))

    val withFrom = (op: WithFrom) =>
      pOperation.addAllVariableAddresses(op.from.map(encodePVariable(_)).map(_.asInstanceOf[java.lang.Integer]))


    val WithIntoAndSeqObject = (into: Variable, objects: Seq[Object]) =>
      pOperation
        .addVariableAddresses(getEncodedId(into)) // Variable are already listed in the parent block, so don't duplicate
        .addAllObjectAddresses(objects.map(encodePObject(_)).map(_.asInstanceOf[java.lang.Integer]))

    val WithIntoAndKeys = (op: WithIntoAndKeys) =>
        WithIntoAndSeqObject(op.into, op.keys)

    val WithIntoAndData = (op: WithIntoAndData) =>
        WithIntoAndSeqObject(op.into, op.data)

    operation match {
      case op: Return with WithFrom => withFrom(op); pOperation.setType(Type.Return)
      case op: From with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setType(Type.From)
      case op: Get with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setType(Type.Get)
      case op: Set with WithIntoAndData => WithIntoAndData(op); pOperation.setType(Type.Set)
      case op: Delete with WithIntoAndData => WithIntoAndData(op); pOperation.setType(Type.Delete)
      case op: Limit with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setType(Type.Limit)
      case op: Projection with WithIntoAndKeys => WithIntoAndKeys(op); pOperation.setType(Type.Projection)
    }

    pOperation
  }

  def decodePOperation(block: Block, pOperation: POperation): Operation = {

    import POperation.Type

    val os: OperationSource = getDecodedData[OperationSource](pOperation.getSourceAddress)

    val variables = pOperation.getVariableAddressesList.map(getDecodedData[Variable](_))
    val objects = pOperation.getObjectAddressesList.map(getDecodedData[Object](_)).toSeq

    pOperation.getType match {
      case Type.Return => new Operation.Return(os, variables)
      case Type.From => new Operation.From(os, variables(0), objects:_*)
      case Type.Get => new Operation.Get(os, variables(0), objects:_*)
      case Type.Set => new Operation.Set(os, variables(0), objects:_*)
      case Type.Delete => new Operation.Delete(os, variables(0), objects:_*)
      case Type.Limit => new Operation.Limit(os, variables(0), objects:_*)
      case Type.Projection => new Operation.Projection(os, variables(0), objects:_*)
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

