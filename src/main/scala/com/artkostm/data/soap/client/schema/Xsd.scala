package com.artkostm.data.soap.client.schema

import cats.implicits._
import com.artkostm.data.soap.client.schema.DataF._
import com.artkostm.data.soap.client.schema.SchemaF._
import higherkindness.droste.data.{Coattr, Fix}
import higherkindness.droste.syntax.all.toFixSyntaxOps
import higherkindness.droste.{CVCoalgebra, Gather, RAlgebra, scheme}
import jto.validation.xml.Rules
import jto.validation.xml.Rules._
import jto.validation.{Path, _}
import org.apache.ws.commons.schema._
import org.apache.ws.commons.schema.constants.Constants

import scala.collection.JavaConverters._
import scala.xml.Node

object Xsd {
  type XRule[A] = Rule[Node, A]
  type FixedDataRule = XRule[Fix[DataF]]

  val InternalValColName     = "_VALUE"
  val DefaultWildcardColName = "xs_any"

  def getValidatorFor(schemeF: Fix[SchemaF]): XRule[Fix[DataF]] = dataValidator(schemeF)

  val toSchemaF: CVCoalgebra[SchemaF, (XmlSchema, XmlSchemaType)] =
    CVCoalgebra[SchemaF, (XmlSchema, XmlSchemaType)] {
      case (xmlSchema, xmlElementType) =>
        xmlElementType match {
          case simpleType: XmlSchemaSimpleType =>
            simpleType.getContent match {
              case restriction: XmlSchemaSimpleTypeRestriction =>
                simpleType.getQName match {
                  case Constants.XSD_BOOLEAN => BooleanF(true)
                  case Constants.XSD_DECIMAL =>
                    val scale = restriction.getFacets.asScala.collectFirst[Int] {
                      case facet: XmlSchemaFractionDigitsFacet => facet.getValue.toString.toInt
                    }
                    scale match {
                      case Some(scale) => DecimalF(38, scale, nullable = true)
                      case None        => DecimalF(38, 18, nullable = true)
                    }
                  case Constants.XSD_UNSIGNEDLONG                       => DecimalF(38, 0, nullable = true)
                  case Constants.XSD_DOUBLE                             => DoubleF(true)
                  case Constants.XSD_FLOAT                              => FloatF(true)
                  case Constants.XSD_BYTE                               => ByteF(true)
                  case Constants.XSD_SHORT | Constants.XSD_UNSIGNEDBYTE => ShortF(true)
                  case Constants.XSD_INTEGER | Constants.XSD_NEGATIVEINTEGER |
                      Constants.XSD_NONNEGATIVEINTEGER | Constants.XSD_NONPOSITIVEINTEGER |
                      Constants.XSD_POSITIVEINTEGER | Constants.XSD_UNSIGNEDSHORT =>
                    IntF(true)
                  case Constants.XSD_LONG | Constants.XSD_UNSIGNEDINT => LongF(true)
                  case Constants.XSD_DATE | Constants.XSD_DATETIME    => StringF(true)
                  case _                                              => StringF(true)
                }
              case _ => StringF(true)
            }
          case complexType: XmlSchemaComplexType =>
            complexType.getContentModel match {
              case content: XmlSchemaSimpleContent =>
                // xs:simpleContent
                content.getContent match {
                  case extension: XmlSchemaSimpleContentExtension =>
                    val baseType =
                      (InternalValColName,
                       Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)](
                         (xmlSchema, xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                       ))
                    val attributes = extension.getAttributes.asScala.map {
                      case attribute: XmlSchemaAttribute =>
                        (s"_${attribute.getName}",
                         Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)](
                           (xmlSchema, xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
                         ))
                    }
                    StructF((baseType +: attributes).toMap, true)
                }
              case null =>
                val childFields: Seq[(String, Coattr[SchemaF, (XmlSchema, XmlSchemaType)])] =
                  complexType.getParticle match {
                    case all: XmlSchemaAll =>
                      all.getItems.asScala.toList.map {
                        case element: XmlSchemaElement =>
                          val nullable = element.getMinOccurs == 0
                          (element.getName,
                           if (element.getMaxOccurs == 1)
                             Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)](
                               (xmlSchema, element.getSchemaType)
                             )
                           else
                             Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                               ArrayF(Coattr.pure((xmlSchema, element.getSchemaType)), nullable)
                             ))
                      }
                    case choice: XmlSchemaChoice =>
                      choice.getItems.asScala.toList.map {
                        case element: XmlSchemaElement =>
                          (element.getName,
                           if (element.getMaxOccurs == 1)
                             Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)](
                               (xmlSchema, element.getSchemaType)
                             )
                           else
                             Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                               ArrayF(
                                 Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)](
                                   (xmlSchema, element.getSchemaType)
                                 ), true
                               )
                             ))
                        case any: XmlSchemaAny =>
                          (DefaultWildcardColName,
                           if (any.getMaxOccurs > 1)
                             Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                               ArrayF(Coattr.roll(StringF[Coattr[SchemaF, (XmlSchema, XmlSchemaType)]](true)), true)
                             )
                           else
                             Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                               StringF[Coattr[SchemaF, (XmlSchema, XmlSchemaType)]](true)
                             ))
                      }
                    case sequence: XmlSchemaSequence =>
                      // flatten xs:choice nodes
                      sequence.getItems.asScala.toList.flatMap {
                        _ match {
                          case choice: XmlSchemaChoice =>
                            choice.getItems.asScala.map {
                              case e: XmlSchemaElement =>
                                (e.getName,
                                 if (e.getMaxOccurs > 1)
                                   Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                                     ArrayF(Coattr.pure((xmlSchema, e.getSchemaType)), true)
                                   )
                                 else
                                   Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)](
                                     (xmlSchema, e.getSchemaType)
                                   ))
                            }
                          case e: XmlSchemaElement =>
                            val nullable = e.getMinOccurs == 0
                            Seq(
                              (e.getName,
                               if (e.getMaxOccurs > 1)
                                 Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                                   ArrayF(Coattr.pure((xmlSchema, e.getSchemaType)), nullable)
                                 )
                               else
                                 Coattr
                                   .pure[SchemaF, (XmlSchema, XmlSchemaType)]((xmlSchema, e.getSchemaType)))
                            )
                          case any: XmlSchemaAny =>
                            val nullable = any.getMinOccurs == 0
                            Seq(
                              (DefaultWildcardColName,
                               if (any.getMaxOccurs > 1)
                                 Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](
                                   ArrayF(Coattr.roll(StringF(nullable)), nullable)
                                 )
                               else
                                 Coattr.roll[SchemaF, (XmlSchema, XmlSchemaType)](StringF(nullable)))
                            )
                        }
                      }
                  }
                val attributes: Seq[(String, Coattr[SchemaF, (XmlSchema, XmlSchemaType)])] =
                  complexType.getAttributes.asScala.toList.map {
                    case attribute: XmlSchemaAttribute =>
                      val baseType = xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName)
                      (s"_${attribute.getName}",
                       Coattr.pure[SchemaF, (XmlSchema, XmlSchemaType)]((xmlSchema, baseType)))
                  }
                StructF((childFields ++ attributes).toMap, true)
            }
          case unsupported =>
            throw new IllegalArgumentException(s"Unsupported schema element type: $unsupported")
        }
    }

  import XsdRules._

  val dataValidationAlgebra: RAlgebra[Fix[SchemaF], SchemaF, XRule[Fix[DataF]]] =
    RAlgebra[Fix[SchemaF], SchemaF, XRule[Fix[DataF]]] {
      case StructF(fields, n) =>
        fields.toList
          .traverse[XRule, (String, Fix[DataF])] {
            case (name, (schema, validation)) if isArray(schema) =>
              Path.read(
                pickElementsByName(name).andThen(validation.map(fx => (name, fx)))
              )(pickInNodeOpt(n))
            case (name, (_, validation)) =>
              (Path \ name).read(validation.map(fx => (name, fx)))(pickInNodeOpt(n))
          }
          .map(fs => GStruct(fs.toMap).fix[DataF])
      case ArrayF((_, elements), _) => Rules.pickSeq(elements).map(GArray(_).fix[DataF])
      case BooleanF(n)              => pickNullable[Boolean](n)(GBoolean(_).fix[DataF])
      case DoubleF(n)               => pickNullable[Double](n)(GDouble(_).fix[DataF])
      case FloatF(n)                => pickNullable[Float](n)(GFloat(_).fix[DataF])
      case IntF(n)                  => pickNullable[Int](n)(GInt(_).fix[DataF])
      case LongF(n)                 => pickNullable[Long](n)(GLong(_).fix[DataF])
      case StringF(n)               => pickNullable[String](n)(GString(_).fix[DataF])
    }

  protected lazy val dataValidator: Fix[SchemaF] => XRule[Fix[DataF]] =
    scheme.gcata(dataValidationAlgebra)(Gather.para)
}
