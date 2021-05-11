package com.artkostm.data.soap.client

import com.artkostm.data.soap.client.schema.{SchemaF, Xsd}
import com.sandinh.xml.{Xml, XmlReader}
import higherkindness.droste.data.Fix
import higherkindness.droste.scheme
import org.apache.ws.commons.schema.{XmlSchema, XmlSchemaType}
import zio.{Has, Task, ULayer, ZLayer}

import javax.xml.namespace.QName
import scala.xml.NodeSeq

package object wsdl {

  type WsdlLoader = Has[WsdlLoader.Service]

  object WsdlLoader {
    trait Service {
      def definitionFor(wsdlUrl: String): Task[WsdlDefinition]
    }

    val live: ULayer[WsdlLoader] = ZLayer.succeed(new UrlWsdlLoader())
  }

  lazy val xmlTypeToSchemaF: ((XmlSchema, XmlSchemaType)) => Fix[SchemaF] = scheme.zoo.futu(Xsd.toSchemaF)

  final case class WsdlOperation(name: String,
                                 inputMessage: String,
                                 outputMessage: String,
                                 faultMessage: String)

  final case class WsdlPortType(name: String, operations: Seq[WsdlOperation])

  final case class WsdlBinging(name: String, portType: WsdlPortType)

  final case class WsdlPort(name: String, location: String, binding: WsdlBinging)

  final case class WsdlServiceDefinition(name: String, targetNamespace: String, port: WsdlPort)

  final case class WsdlDefinition(service: WsdlServiceDefinition, types: Map[QName, Fix[SchemaF]])

  object WsdlOperation {
    implicit val WsdlOperationReader: XmlReader[WsdlOperation] = (x: NodeSeq) =>
      for {
        name   <- (x \ "@name").headOption
        input  <- (x \ "input" \ "@message").headOption
        output <- (x \ "output" \ "@message").headOption
        fault  <- (x \ "fault" \ "@message").headOption
      } yield
        WsdlOperation(name = name.text,
                      inputMessage = input.text.replace("tns:", ""),
                      outputMessage = output.text.replace("tns:", ""),
                      faultMessage = fault.text.replace("tns:", "")) // todo: create QName
  }

  object WsdlPortType {

    import cats.instances.list._
    import cats.instances.option._
    import cats.syntax.traverse._

    implicit val WsdlPortTypeReader: XmlReader[WsdlPortType] = (x: NodeSeq) =>
      for {
        name       <- (x \ "@name").headOption
        operations <- (x \\ "operation").map(Xml.fromXml[WsdlOperation](_)).toList.sequence
      } yield WsdlPortType(name = name.text, operations = operations)
  }

  object WsdlServiceDefinition {
    implicit val WsdlServiceDefinitionReader: XmlReader[WsdlServiceDefinition] = (x: NodeSeq) =>
      for {
        serviceNode  <- (x \ "service").headOption
        name         <- (serviceNode \\ "@name").headOption
        tns          <- (x \\ "@targetNamespace").headOption
        portNode     <- (serviceNode \ "port").headOption
        bindingNode  <- (x \ "binding").find(_ \@ "name" == (portNode \@ "binding").replace("tns:", ""))
        portTypeNode <- (x \ "portType").find(_ \@ "name" == (bindingNode \@ "type").replace("tns:", ""))
        binding <- for {
                    bindingName <- (bindingNode \ "@name").headOption
                    portType    <- Xml.fromXml[WsdlPortType](portTypeNode)
                  } yield WsdlBinging(name = bindingName.text, portType = portType)
        port <- for {
                 portName <- (portNode \ "@name").headOption
                 location <- (portNode \ "address" \ "@location").headOption
               } yield
                 WsdlPort(
                   name = portName.text,
                   location = location.text,
                   binding = binding
                 )
      } yield
        WsdlServiceDefinition(
          name = name.text,
          targetNamespace = tns.text,
          port = port
      )
  }
}
