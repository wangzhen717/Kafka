import java.nio.charset.StandardCharsets.UTF_8
import java.util

import io.circe.{ Decoder, Encoder, Printer }
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serializer }

import scala.language.implicitConversions
import scala.util.control.NonFatal

trait CirceSupport {
  implicit def toSerializer[T >: Null](
                                        implicit encoder: Encoder[T],
                                        printer: Printer = Printer.noSpaces
                                      ): Serializer[T] =
    new Serializer[T] {
      import io.circe.syntax._
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
      override def close(): Unit                                                 = {}
      override def serialize(topic: String, data: T): Array[Byte] =
        if (data == null) null
        else
          try printer.print(data.asJson).getBytes(UTF_8)
          catch {
            case NonFatal(e) => throw new SerializationException(e)
          }
    }

  implicit def toDeserializer[T >: Null](implicit decoder: Decoder[T]): Deserializer[T] =
    new Deserializer[T] {
      import io.circe._
      import cats.syntax.either._

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
      override def close(): Unit                                                 = {}
      override def deserialize(topic: String, data: Array[Byte]): T =
        if (data == null) null
        else
          parser
            .parse(new String(data, UTF_8))
            .valueOr(e => throw new SerializationException(e))
            .as[T]
            .valueOr(e => throw new SerializationException(e))
    }

  implicit def toSerde[T >: Null](
                                   implicit encoder: Encoder[T],
                                   printer: Printer = Printer.noSpaces,
                                   decoder: Decoder[T]
                                 ): Serde[T] =
    new Serde[T] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
      override def close(): Unit                                                 = {}
      override def serializer(): Serializer[T]                                   = toSerializer[T]
      override def deserializer(): Deserializer[T]                               = toDeserializer[T]
    }
}

object CirceSupport extends CirceSupport