package com.artkostm.data.soap.client.schema

import com.artkostm.data.soap.client.schema.DataF.nullF
import higherkindness.droste.data.Fix
import jto.validation._
import jto.validation.xml._
import jto.validation.xml.Rules._
import cats.implicits._

import scala.xml.Node

object XsdRules {
  def pickNullable[O](nullable: Boolean)(ifNotNullable: O => Fix[DataF])(implicit r: RuleLike[String, O]): Rule[Node, Fix[DataF]] =
    Rule
      .fromMapping[Node, Fix[DataF]] { node =>
        val children = node \ "_"
        if (children.isEmpty)
          if (nullable) {
            val nodeText = node.text
            if (nodeText == null || nodeText.trim.isEmpty) Valid[Fix[DataF]](nullF)
            else r.validate(node.text).bimap(e => e.flatMap(_._2), ifNotNullable)
          } else r.validate(node.text).bimap(e => e.flatMap(_._2), ifNotNullable)
        else
          Invalid(Seq(ValidationError(
            "error.invalid",
            "a non-leaf node can not be validated to String")))
      }

  // For cases when A is the name of an array element, and XML is of the following structure
  // <parent><B>...</B><A>...</A><A>...</A><parent>,
  // where parent is a parent structure element, B is an element, and A is an array element.
  // At the same time, some of the APIs can return XML like <parent><B>...</B><A><item>...</item><item>...</item></A><parent>
  // In such case, remove the case branch with pickElementsByName from dataValidator
  def pickElementsByName(name: String): Rule[Node, Node] =
    Rule.fromMapping[Node, Node] { n =>
      Valid(<_internal>{ n \ name }</_internal>)
    }

  def stringAs[T](f: PartialFunction[BigDecimal, Validated[Seq[ValidationError], T]])(args: Any*): Rule[String, T] =
    Rule.fromMapping[String, T] {
      val toB: PartialFunction[String, BigDecimal] = {
        case s if s.matches("""^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$""") => BigDecimal(s)
      }
      toB
        .lift(_)
        .flatMap(f.lift)
        .getOrElse(Invalid(Seq(ValidationError("error.number", args: _*))))
    }

  implicit def pickInNodeOpt[II <: Node, O](nullable: Boolean)(p: Path)(
    implicit r: RuleLike[Node, O]): Rule[II, O] = {
    def search(path: Path, node: Node): Option[Node] = path.path match {
      case KeyPathNode(key) :: tail =>
        (node \ key).headOption.flatMap(childNode =>
          search(Path(tail), childNode))

      case IdxPathNode(idx) :: tail =>
        (node \ "_")
          .lift(idx)
          .flatMap(childNode => search(Path(tail), childNode))

      case Nil => Some(node)
    }

    Rule[II, Node] { node =>
      search(p, node) match {
        case None if !nullable =>
          Invalid(Seq(Path -> Seq(ValidationError("error.required"))))
        case Some(resNode) => Valid(resNode)
        case None          => Valid(EmptyNode)
      }
    }.andThen(r)
  }

  implicit def doubleR: Rule[String, Double] =
    stringAs {
      case s if s.isDecimalDouble => Valid(s.toDouble)
    }("Double")

  val EmptyNode = <empty></empty>
}
