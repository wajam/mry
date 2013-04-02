package com.wajam.mry.api

import com.wajam.nrv.protocol.codec.GenericJavaSerializeCodec
import com.wajam.nrv.protocol.codec.Codec
import com.wajam.nrv.Logging
import java.nio.ByteBuffer

/**
 * Used for migration to MryCodec // TODO: Remove
 */
@Deprecated
class HybridCodec(mode: HybridCodec.TransitionMode.Value) extends Codec with Logging {

  import HybridCodec._

  log.info("HybridCodec mode: {}", mode.toString())

  def encode(entity: Any, context: Any = null): Array[Byte] = {

    val codec =
      mode match {
        case TransitionMode.DecBothEncJava |
             TransitionMode.DecJavaEncJava => genericCodec

        case TransitionMode.DecMryEncMry |
             TransitionMode.DecBothEncMry => mryCodec
      }

    codec.encode(entity, context)
  }

  def decode(data: Array[Byte], context: Any = null): Any = {

    val handleBoth = () => {

      // If java serialized, decode with java, else decode with mryCodec.

      val magicShort: Int = ByteBuffer.wrap(data, 0, 2).getShort

      if (magicShort == JavaSerializeMagicShort)
        genericCodec
      else
        mryCodec
    }

    val codec =
      mode match {
        case TransitionMode.DecBothEncJava |
             TransitionMode.DecBothEncMry => handleBoth()

        case TransitionMode.DecJavaEncJava => genericCodec

        case TransitionMode.DecMryEncMry => mryCodec
      }

    codec.decode(data, context)
  }
}

object HybridCodec {

  private val mryCodec = new MryCodec
  private val genericCodec = new GenericJavaSerializeCodec

  // Source: http://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
  private val JavaSerializeMagicShort : Short = (0xACED).toShort

  object TransitionMode extends Enumeration {

    // Decode Both, Encode Java, used as a fallback, or a as migration first step
    val DecBothEncJava = Value(1)

    // Decode Both, Encode Mry, use when all nodes are Mry aware, to switch to Mry
    val DecBothEncMry = Value(2)

    // Decode Mry only, Encode Mry, force only Mry traffic on the cluster. Use it to detect un-updated nodes
    val DecMryEncMry = Value(3)

    // Decode Java only, Encode Java, force only Java traffic on the cluster.
    val DecJavaEncJava = Value(4)
  }
}