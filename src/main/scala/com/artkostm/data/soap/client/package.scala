package com.artkostm.data.soap

import com.artkostm.data.soap.client.wsdl.{UrlWsdlLoader, WsdlDefinition}
import com.artkostm.data.soap.config.SoapClientConfig
import org.apache.avro.generic.GenericRecord
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio.stream.Stream
import zio.{Has, Ref, Task, ZIO, ZLayer}

import scala.xml.{Elem, NamespaceBinding}


package object client {
  type SoapClient  = Has[SoapClient.Service]

  final def read(action: String, wsdlUrl: String)(data: Map[String, List[String]]): ZIO[SoapClient, Throwable, Stream[Throwable, GenericRecord]] =
    ZIO.accessM(_.get.read(action, wsdlUrl)(data))

  object SoapClient {

    def live: ZLayer[Has[SoapClientConfig], Throwable, SoapClient] =
      ZLayer.fromServiceManaged[SoapClientConfig, Any, Throwable, SoapClient.Service] {
        config => AsyncHttpClientZioBackend.managed()
          .zip(Ref.makeManaged(Map.empty[String, WsdlDefinition]))
          .map(pair => new SoapSttpClient(config, new UrlWsdlLoader, pair._2)(pair._1))
      }

    trait Service {
      def read(action: String, wsdlUrl: String)(data: Map[String, List[String]]): Task[Stream[Throwable, GenericRecord]]
      def readEntity(service: String, action: String, xmlElement: String, namespace: NamespaceBinding)(data: Map[String, List[String]]): Task[Stream[Throwable, Map[String, String]]]
    }
  }
}
