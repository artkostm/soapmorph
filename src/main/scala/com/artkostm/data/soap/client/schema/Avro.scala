package com.artkostm.data.soap.client.schema

import cats.Monad
import cats.data.EitherT
import cats.implicits._
import com.artkostm.data.soap.client.schema.DataF._
import com.artkostm.data.soap.client.schema.SchemaF._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.data.prelude._
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._
import scala.language.higherKinds


object Avro {
  protected val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  type EnvT[A] = AttrF[DataF, Fix[SchemaF], A]

  def createGenericRecord[M[_]: Monad](schema: Fix[SchemaF], data: Fix[DataF]): M[GenericRecord] =
    EitherT(scheme.hyloM(toGenericRecord[M], zipWithSchema[M]).apply((schema, data))).fold(
      a => {
        val error = new RuntimeException("The root struct cannot be of type Any")
        LOGGER.error(s"Error while creating avro.GenericRecord from $a", error)
        throw error
      },
      identity
    )

  lazy val toAvroSchema: Fix[SchemaF] => Schema = scheme.cata(fromSchemaF)

  val fromSchemaF: Algebra[SchemaF, Schema] = Algebra[SchemaF, Schema] {
    case StructF(fields, n) =>
      Schema.createRecord(
        s"record${Math.abs(fields.hashCode())}",
        null,
        "com.petrofac.data",
        false,
        fields.map {
          case (name, s) => new Schema.Field(name, s, null, null)
        }.toList.asJava)
    case ArrayF(element, n) => unionSchema(Schema.createArray(element), n)
    case BooleanF(n)        => unionSchema(Schema.create(Schema.Type.BOOLEAN), n)
    case DoubleF(n)         => unionSchema(Schema.create(Schema.Type.DOUBLE), n)
    case StringF(n)         => unionSchema(Schema.create(Schema.Type.STRING), n)
    case LongF(n)           => unionSchema(Schema.create(Schema.Type.LONG), n)
    case IntF(n)            => unionSchema(Schema.create(Schema.Type.INT), n)
    case DatetimeF(n, f)    =>
      unionSchema(f match {
        case DatetimeF.TimestampMicros | DatetimeF.StringFormat(_) => LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
      }, n)
  }

  // monadic algebra to create a generic record out of M[EnvT[(SchemaF, DataF)]]
  def toGenericRecord[M[_]: Monad]: AlgebraM[M, EnvT, Either[Any, GenericRecord]] =
    AlgebraM[M, EnvT, Either[Any, GenericRecord]] {
      case AttrF(s @ Fix(StructF(_, _)), GStruct(values)) =>
        val avroSchema = toAvroSchema(s)
        val record     = new GenericData.Record(avroSchema)
        (Right(values.foldRight(record) {
          case ((name, Left(simpleValue)), rootRecord) =>
            rootRecord.put(name, simpleValue)
            rootRecord
          case ((name, Right(nestedRecord)), rootRecord) =>
            rootRecord.put(name, nestedRecord)
            rootRecord
        }): Either[Any, GenericRecord]).pure[M]
      case AttrF(_ @Fix(ArrayF(_, _)), GArray(values)) =>
        (Left(values.map {
          case Right(value) => value
          case Left(value)  => value
        }.asJava): Either[Any, GenericRecord]).pure[M]
      case AttrF(Fix(DatetimeF(_, f)), v: GDatetime[_]) =>
        f match {
          case DatetimeF.TimestampMicros | DatetimeF.StringFormat(_) =>
            (Left(ChronoUnit.MICROS.between(Instant.EPOCH, v.value.toInstant)): Either[Any, GenericRecord]).pure[M]
        }
      case AttrF(_, v: GValue[_]) => (Left(v.value): Either[Any, GenericRecord]).pure[M]
      case AttrF(_, _)            => throw new RuntimeException("Should not happen")
    }

  private def unionSchema(s: Schema, nullable: Boolean): Schema =
    if (nullable) Schema.createUnion(s, Schema.create(Type.NULL)) else s
}
