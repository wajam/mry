package com.wajam.mry.api

import com.wajam.nrv.protocol.codec.Codec
import protobuf.ProtobufTranslator
import com.wajam.mry.execution.{Value, Transaction}

class MryCodec extends Codec {

  private val protobufTranslator = new ProtobufTranslator

  def encode(entity: Any, context: Any = null): Array[Byte] = {

    val transport = entity match {
      case request: Transaction => Transport(Some(request), None)
      case response: Seq[Value] => Transport(None, Some(response))
      case _ => throw new RuntimeException("Unsupported type for this codec.")
    }

    protobufTranslator.encodeAll(transport)
  }

  def decode(data: Array[Byte], context: Any = null): Any = {

    val entity = protobufTranslator.decodeAll(data)

    entity match {
      case Transport(Some(request), None) => request
      case Transport(None, Some(response)) => response
      case _ => throw new RuntimeException("Invalid data from transport.")
    }
  }

}
