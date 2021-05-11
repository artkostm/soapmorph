package com.artkostm.data.soap.client.schema

import cats.Monad
import cats.data.EitherT
import cats.implicits._
import com.artkostm.data.soap.client.schema.DataF._
import com.artkostm.data.soap.client.schema.SchemaF._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.data.prelude._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

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
    case StructF(fields) =>
      Schema.createRecord(fields.map {
        case (name, s) => new Schema.Field(name, s, null, null)
      }.toList.asJava)
    case ArrayF(element) => Schema.createArray(element)
    case BooleanF(n)     => unionSchema(Schema.create(Schema.Type.BOOLEAN), n)
    case DoubleF(n)      => unionSchema(Schema.create(Schema.Type.DOUBLE), n)
    case StringF(n)      => unionSchema(Schema.create(Schema.Type.STRING), n)
    case LongF(n)        => unionSchema(Schema.create(Schema.Type.LONG), n)
    case IntF(n)         => unionSchema(Schema.create(Schema.Type.INT), n)
  }

  // zip SchemaF with DataF - we need this because it is not possible to construct avro's generic record without schema
  def zipWithSchema[M[_]: Monad]: CoalgebraM[M, EnvT, (Fix[SchemaF], Fix[DataF])] =
    CoalgebraM[M, EnvT, (Fix[SchemaF], Fix[DataF])] {
      case (structF @ Fix(StructF(fields)), Fix(GStruct(values))) =>
        AttrF
          .apply[DataF, Fix[SchemaF], (Fix[SchemaF], Fix[DataF])](
            (structF, GStruct(values.map {
              case (name, value) => (name, (fields.apply(name), value))
            }))
          )
          .pure[M]
      case (arrayF @ Fix(ArrayF(elementSchema)), Fix(GArray(values))) =>
        AttrF
          .apply[DataF, Fix[SchemaF], (Fix[SchemaF], Fix[DataF])](arrayF, GArray(values.map { e =>
            elementSchema -> e
          }))
          .pure[M]
      case (schemaF, Fix(lower)) =>
        AttrF.apply[DataF, Fix[SchemaF], (Fix[SchemaF], Fix[DataF])](schemaF, lower.map((schemaF, _))).pure[M]
    }

  // monadic algebra to create a generic record out of M[EnvT[(SchemaF, DataF)]]
  def toGenericRecord[M[_]: Monad]: AlgebraM[M, EnvT, Either[Any, GenericRecord]] =
    AlgebraM[M, EnvT, Either[Any, GenericRecord]] {
      case AttrF(s @ Fix(StructF(_)), GStruct(values)) =>
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
      case AttrF(_ @Fix(ArrayF(_)), GArray(values)) =>
        (Left(values.map {
          case Right(value) => value
          case Left(value)  => value
        }): Either[Any, GenericRecord]).pure[M]
      case AttrF(_, v: GValue[_]) => (Left(v.value): Either[Any, GenericRecord]).pure[M]
      case AttrF(_, _)            => throw new RuntimeException("Should not happen")
    }

  private def unionSchema(s: Schema, nullable: Boolean): Schema = nullable match {
    case true  => Schema.createUnion(s, Schema.create(Type.NULL))
    case false => s
  }
}
