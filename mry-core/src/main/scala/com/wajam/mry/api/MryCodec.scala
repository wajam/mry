package com.wajam.mry.api

import com.wajam.nrv.protocol.codec.Codec
import protobuf.ProtobufTranslator
import com.wajam.mry.execution.{Value, Transaction}

class MryCodec extends Codec {

  def encode(entity: Any, context: Any = null): Array[Byte] = {

    val transport = entity match {
      case request: Transaction => Transport(Some(request), Seq())
      case response: Seq[Value] => Transport(None, response)
      case _ => throw new RuntimeException("Unsupported type for this codec.")
    }

    MryCodec.protobufTranslator.encodeAll(transport)
  }

  def decode(data: Array[Byte], context: Any = null): Any = {

    val entity = MryCodec.protobufTranslator.decodeAll(data)

    entity match {
      case Transport(Some(request), _) => request
      case Transport(None, response: Seq[_]) => response
      case _ => throw new RuntimeException("Invalid data from transport.")
    }
  }
}

object MryCodec {
  private val protobufTranslator = new ProtobufTranslator
}
