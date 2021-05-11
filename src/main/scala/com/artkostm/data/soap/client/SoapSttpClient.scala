package com.artkostm.data.soap.client

import com.artkostm.data.soap.config.SoapClientConfig
import com.artkostm.data.landing.rest.client.ClientUtils.handleResponse
import com.artkostm.data.soap.client.schema.{Avro, DataF, SchemaF, Xsd}
import com.artkostm.data.soap.client.wsdl.{WsdlDefinition, WsdlLoader}
import com.sandinh.soap.SOAP
import com.sandinh.xml.{BasicWriters, SpecialWriters, Xml}
import higherkindness.droste.data.Fix
import org.apache.avro.generic.GenericRecord
import sttp.capabilities.zio.ZioStreams
import sttp.client3.asynchttpclient.zio.SttpClient
import sttp.client3.{UriContext, asStreamUnsafe, basicRequest}
import sttp.model.{Header, MediaType, Uri}
import xs4s.XmlElementExtractor
import xs4s.syntax.zio.RichXmlElementExtractor
import xs4s.ziocompat.byteStreamToXmlEventStream
import zio.blocking.Blocking
import zio.stream.{Stream, ZStream}
import zio.{Ref, Task, ZIO}

import javax.xml.namespace.QName
import scala.xml.{Elem, NamespaceBinding, NodeSeq}

class SoapSttpClient(config: SoapClientConfig,
                     wsdlLoader: WsdlLoader.Service,
                     wsdlDefStore: Ref[Map[String, WsdlDefinition]])(backend: SttpClient.Service) extends SoapClient.Service with BasicWriters with SpecialWriters {
  import SoapSttpClient._

   override def readEntity(service: String, action: String, xmlElement: String, namespace: NamespaceBinding)(data: Map[String, List[String]]): Task[Stream[Throwable, Map[String, String]]] =
     call(uri"${config.baseUrl}/$service", action, namespace)(data)
       .map(xmlStreamToMapStream(xmlElement)(_))

  private def call(url: Uri, action: String, namespace: NamespaceBinding)(data: Map[String, List[String]]): Task[Stream[Throwable, Byte]] =
    handleResponse(config.retry) {
      basicRequest
        .post(url)
        .header(Header.contentType(MediaType.parse("text/xml").getOrElse(MediaType.ApplicationXml)))
        .header("SOAPAction", action)
        .body(
          envelope(
            payload(action, namespace.prefix)(data),
            security(config.auth.username.value, config.auth.password.value)(WsseSn)
          )(namespace).toString()
        )
        .readTimeout(config.readTimeOut)
        .response(asStreamUnsafe(ZioStreams))
        .send(backend)
    }

  override def read(action: String, wsdlUrl: String)(data: Map[String, List[String]]): Task[Stream[Throwable, GenericRecord]] = {
    for {
      wsdlStore <- wsdlDefStore.get
      definition <-
        if(wsdlStore.contains(wsdlUrl)) Task.effect(wsdlStore.apply(wsdlUrl))
        else wsdlLoader.definitionFor(wsdlUrl).flatMap(d => wsdlDefStore.update(old => old + (wsdlUrl -> d)) *> Task.succeed(d))
      operation <- definition.service.port.binding.portType.operations.find(_.name == action) match {
        case Some(op) => Task.succeed(op)
        case _ => Task.fail(new RuntimeException(
          s"Cannot find action: $action, possible actions ${definition.service.port.binding.portType.operations.mkString("[", ", ", "]")}"
        ))
      }
      Some((topElementLabel, dataSchema)) = SchemaF.getHigherLevelFieldType(definition.types(new QName(definition.service.targetNamespace, operation.outputMessage)))
      xmlValidator = Xsd.getValidatorFor(dataSchema)
      entityStream <- call(uri"${definition.service.port.location}",
        action,
        NamespaceBinding("v1", definition.service.targetNamespace, SOAP.SoapNS))(data)
    } yield xmlStreamToGenericRecord(topElementLabel, xmlValidator, dataSchema)(entityStream)
  }

  private def payload(rootTag: String, prefix: String)(data: Map[String, List[String]]) =
    <x>{ data.map {
      case (element, values) => Xml.toXml(values, <x/>.copy(label = element, prefix = prefix))
    } }</x>.copy(label = rootTag, prefix = prefix)

  private def security(user: String, password: String)(ns: NamespaceBinding): Elem = {
    val security =
      <wsse:Security soapenv:mustUnderstand="1">
        <wsse:UsernameToken>
          <wsse:Username>{ user }</wsse:Username>
          <wsse:Password>{ password }</wsse:Password>
        </wsse:UsernameToken>
      </wsse:Security>

    security.copy(scope = ns)
  }

  private def envelope(body: NodeSeq = NodeSeq.Empty, header: NodeSeq = NodeSeq.Empty)(ns: NamespaceBinding): NodeSeq = {
    val env =
      <soapenv:Envelope>
        <soapenv:Header>{ header }</soapenv:Header>
        <soapenv:Body>{ body }</soapenv:Body>
      </soapenv:Envelope>
    env.copy(scope = ns)
  }
}

object SoapSttpClient {

  protected val WsseSn = NamespaceBinding("wsse", "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd", xml.TopScope)

  protected def xmlStreamToMapStream(xmlElementName: String)(xmlByteStream: Stream[Throwable, Byte]): Stream[Throwable, Map[String, String]] = {
    val anchorElementExtractor: XmlElementExtractor[Elem] =
      XmlElementExtractor.filterElementsByName(xmlElementName)

    val anchorElements: ZStream[Blocking, Throwable, Elem] =
      xmlByteStream.via(byteStreamToXmlEventStream()(_))
        .via(anchorElementExtractor.toZIOPipeThrowError)

    anchorElements.map { e =>
      val (complex, regular) = e.child.filter(!_.isAtom).partition(n => (n \ "_").length > 0)
      val c = complex.headOption.map { first =>
        <x>{ complex.reduceLeftOption[NodeSeq]((n1, n2) => n1 ++ n2).getOrElse(Seq.empty) }</x>.copy(label = first.label + "s")
      }.map(n => n.label -> n.toString)
      (regular.map(n => n.label -> n.text) ++ c).toMap
    }
      .provideLayer(Blocking.live)
  }

  protected def xmlStreamToGenericRecord(xmlElementName: String, validator: Xsd.FixedDataRule, schemaF: Fix[SchemaF])
                                        (xmlByteStream: Stream[Throwable, Byte]): Stream[Throwable, GenericRecord] = {
    val anchorElementExtractor: XmlElementExtractor[Elem] =
      XmlElementExtractor.filterElementsByName(xmlElementName)

    val anchorElements: ZStream[Blocking, Throwable, Elem] =
      xmlByteStream.via(byteStreamToXmlEventStream()(_))
        .via(anchorElementExtractor.toZIOPipeThrowError)

    anchorElements.flatMap { e =>
      validator.validate(e).fold(errors =>
        Stream.fail(new RuntimeException(s"Error while validating streaming data labeled ${e.label}: \n${errors.mkString(",\n")}\n\n$e")),
        Stream.succeed(_)
      )
    }.map(data => Avro.createGenericRecord(schemaF, data))
      .provideLayer(Blocking.live)
  }
}
