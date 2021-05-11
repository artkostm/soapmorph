package com.artkostm.data.soap.client.schema

import cats.implicits._
import cats.{Applicative, Functor, Traverse}
import com.artkostm.data.soap.client.schema.DataF._
import higherkindness.droste.util.DefaultTraverse

sealed trait DataF[A] {
  def traverse[F[_]: Applicative, B](f: A => F[B]): F[DataF[B]] = this match {
    case GStruct(fields) =>
      fields
        .foldRight(Map.empty[String, B].pure[F]) {
          case ((name, v), acc) =>
            (name.pure[F], f(v), acc).mapN { (n, a, m) =>
              m + (n -> a)
            }
        }
        .map(GStruct(_))
    case GArray(elements) =>
      elements.foldRight(Seq.empty[B].pure[F]) {
        case (e, acc) => (f(e), acc).mapN(_ +: _)
      }.map(GArray.apply)
    case GDouble(n)        => (GDouble[B](n): DataF[B]).pure[F]
    case GBoolean(n)       => (GBoolean[B](n): DataF[B]).pure[F]
    case GFloat(n)         => (GFloat[B](n): DataF[B]).pure[F]
    case GString(n)        => (GString[B](n): DataF[B]).pure[F]
    case GLong(n)          => (GLong[B](n): DataF[B]).pure[F]
    case GInt(n)           => (GInt[B](n): DataF[B]).pure[F]
    case GNull()           => (GNull[B](): DataF[B]).pure[F]
  }
}

sealed trait GValue[A] extends DataF[A] {
  val value: Any
}

object DataF {

  final case class GStruct[A](fields: Map[String, A]) extends DataF[A]
  final case class GArray[A](elements: Seq[A])        extends DataF[A]
  final case class GString[A](value: String)          extends GValue[A]
  final case class GBoolean[A](value: Boolean)        extends GValue[A]
  final case class GDouble[A](value: Double)          extends GValue[A]
  final case class GFloat[A](value: Float)            extends GValue[A]
  final case class GInt[A](value: Int)                extends GValue[A]
  final case class GLong[A](value: Long)              extends GValue[A]

  final case class GNull[A]() extends GValue[A] {
    override val value: Any = null
  }

  implicit val dataTraverse: Traverse[DataF] = new DefaultTraverse[DataF] {
    override def traverse[G[_]: Applicative, A, B](fa: DataF[A])(f: A => G[B]): G[DataF[B]] =
      fa.traverse(f)
  }

  implicit val dataFunctor: Functor[DataF] = new Functor[DataF] {
    override def map[A, B](fa: DataF[A])(f: A => B): DataF[B] = fa match {
      case GStruct(fields)  => GStruct[B](fields.mapValues(f))
      case GArray(elements) => GArray[B](elements.map(f))
      case GString(v)       => GString[B](v)
      case GBoolean(v)      => GBoolean[B](v)
      case GDouble(v)       => GDouble[B](v)
      case GFloat(v)        => GFloat[B](v)
      case GInt(v)          => GInt[B](v)
      case GLong(v)         => GLong[B](v)
      case GNull()          => GNull[B]()
    }
  }
}
