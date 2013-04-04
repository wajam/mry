package com.wajam.mry.api

import com.wajam.nrv.protocol.codec.Codec
import protobuf.ProtobufTranslator
import com.wajam.mry.execution.{Value, Transaction}

class MryCodec extends Codec {

  def encode(entity: Any, context: Any = null): Array[Byte] = {

    val transport = entity match {
      case request: Transaction => Transport(Some(request), None)
      case response: Seq[Value] if response.forall(_.isInstanceOf[Value]) => Transport(None, Some(response))
      case null => Transport(None, None)
      case _ => throw new RuntimeException("Unsupported type for this codec: Class: %s; ToString: %s".format(entity.getClass.toString, entity.toString))
    }

    MryCodec.protobufTranslator.encodeAll(transport)
  }

  def decode(data: Array[Byte], context: Any = null): Any = {

    val entity = MryCodec.protobufTranslator.decodeAll(data)

    entity match {
      case Transport(Some(trx), _) => trx
      case Transport(_, Some(values)) => values
      case Transport(None, None) => null
      case _ => throw new RuntimeException("Invalid data from transport.")
    }
  }
}

object MryCodec {
  private val protobufTranslator = new ProtobufTranslator
}
