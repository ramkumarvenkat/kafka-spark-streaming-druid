package pipeline.common

import kafka.serializer.Encoder
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter, SpecificRecordBase }
import java.io.ByteArrayOutputStream
import pipeline.model.avro.KafkaEvent

object SerDeUtil {

  /**
   * We must explicitly require the user to supply the schema -- even though it is readily available through T -- because
   * of Java's type erasure.  Unfortunately we cannot use Scala's `TypeTag` or `Manifest` features because these will add
   * (implicit) constructor parameters to actual Java class that implements T, and this behavior will cause Kafka's
   * mechanism to instantiate T to fail because it cannot find a constructor whose only parameter is a
   * `kafka.utils.VerifiableProperties`.  Again, this is because Scala generates Java classes whose constructors always
   * include the Manifest/TypeTag parameter in addition to the normal ones.  For this reason we haven't found a better
   * way to instantiate a correct `SpecificDatumWriter[T]` other than explicitly passing a `Schema` parameter.
   *
   * @param props Properties passed to the encoder.  At the moment the encoder does not support any special settings.
   * @param schema The schema of T, which you can get via `T.getClassSchema`.
   * @tparam T The type of the record, which must be backed by an Avro schema (passed via `schema`)
   */
  class AvroEncoder[T <: SpecificRecordBase](props: VerifiableProperties = null, schema: Schema)
    extends Encoder[T] {

    private[this] val NoBinaryEncoderReuse = null.asInstanceOf[BinaryEncoder]
    private[this] val writer: DatumWriter[T] = new SpecificDatumWriter[T](schema)

    override def toBytes(record: T): Array[Byte] = {
      if (record == null) null
      else {
        val out = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get.binaryEncoder(out, NoBinaryEncoderReuse)
        writer.write(record, encoder)
        encoder.flush()
        out.close()
        out.toByteArray
      }
    }

  }

  /**
   * https://github.com/miguno/kafka-avro-codec
   * We must explicitly require the user to supply the schema -- even though it is readily available through T -- because
   * of Java's type erasure.  Unfortunately we cannot use Scala's `TypeTag` or `Manifest` features because these will add
   * (implicit) constructor parameters to actual Java class that implements T, and this behavior will cause Kafka's
   * mechanism to instantiate T to fail because it cannot find a constructor whose only parameter is a
   * `kafka.utils.VerifiableProperties`.  Again, this is because Scala generates Java classes whose constructors always
   * include the Manifest/TypeTag parameter in addition to the normal ones.  For this reason we haven't found a better
   * way to instantiate a correct `SpecificDatumReader[T]` other than explicitly passing a `Schema` parameter.
   *
   * @param props Properties passed to the decoder.  At the moment the decoder does not support any special settings.
   * @param schema The schema of T, which you can get via `T.getClassSchema`.
   * @tparam T The type of the record, which must be backed by an Avro schema (passed via `schema`)
   */
  class AvroDecoder[T <: SpecificRecordBase](props: VerifiableProperties = null, schema: Schema)
    extends Decoder[T] {

    private[this] val NoBinaryDecoderReuse = null.asInstanceOf[BinaryDecoder]
    private[this] val NoRecordReuse = null.asInstanceOf[T]
    private[this] val reader: DatumReader[T] = new SpecificDatumReader[T](schema)

    override def fromBytes(bytes: Array[Byte]): T = {
      val decoder = DecoderFactory.get().binaryDecoder(bytes, NoBinaryDecoderReuse)
      reader.read(NoRecordReuse, decoder)
    }

  }

  def serializeEvent(event: KafkaEvent): Array[Byte] = {
    val encoder = new AvroEncoder[KafkaEvent](schema = KafkaEvent.getClassSchema)
    val bytes = encoder.toBytes(event)
    bytes
  }

  def deserialiseEvent(bytes: Array[Byte]): KafkaEvent = {
    val decoder = new AvroDecoder[KafkaEvent](schema = KafkaEvent.getClassSchema)
    val event = decoder.fromBytes(bytes)
    event
  }
}