package com.artkostm.data.soap.client.wsdl

import com.artkostm.data.soap.client.wsdl
import com.sandinh.xml.Xml
import org.apache.ws.commons.schema.XmlSchemaCollection
import zio.Task

import java.io.StringReader
import java.net.URL
import scala.collection.JavaConverters._
import scala.xml.{NodeSeq, XML}

/*protected[wsdl]*/ class UrlWsdlLoader() extends WsdlLoader.Service {
  override def definitionFor(wsdlUrl: String): Task[WsdlDefinition] = getWsdlDefinition(wsdlUrl)

  private def getWsdlDefinition(wsdlUrl: String): Task[WsdlDefinition] =
    for {
      wsdlElement <- Task.effect(XML.load(new URL(wsdlUrl)))
      service     <- readService(wsdlElement)
      xmlSchema <- Task.effect(
                    new XmlSchemaCollection()
                      .read(new StringReader((wsdlElement \ "types" \ "schema").toString()))
                  )
    } yield
      WsdlDefinition(
        service = service,
        types = xmlSchema.getElements.asScala.map {
          case (qname, schemaElement) =>
            qname -> wsdl.xmlTypeToSchemaF.apply((xmlSchema, schemaElement.getSchemaType))
        }.toMap
      )

  private def readService(wsdlDefinition: NodeSeq): Task[WsdlServiceDefinition] =
    Xml.fromXml[WsdlServiceDefinition](wsdlDefinition) match {
      case Some(service) => Task.succeed(service)
      case _             => Task.fail(new RuntimeException("Cannot convert wsdl element to WsdlServiceDefinition"))
    }
}
