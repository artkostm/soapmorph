package com.artkostm.data.soap

import com.artkostm.data.soap.client.schema.{DataF, SchemaF}
import com.artkostm.data.soap.client.wsdl.{UrlWsdlLoader, WsdlDefinition}
import com.artkostm.data.soap.config.SoapClientConfig
import higherkindness.droste.data.Fix
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio.stream.Stream
import zio.{Has, Ref, Task, ZLayer}

import scala.xml.NamespaceBinding


package object client {
  type SoapClient = Has[SoapClient.Service]

  type GenericStreamingResponse = (Fix[SchemaF], Stream[Throwable, Fix[DataF]])

  object SoapClient {

    def live: ZLayer[Has[SoapClientConfig], Throwable, SoapClient] =
      ZLayer.fromServiceManaged[SoapClientConfig, Any, Throwable, SoapClient.Service] {
        config => AsyncHttpClientZioBackend.managed()
          .zip(Ref.makeManaged(Map.empty[String, WsdlDefinition]))
          .map(pair => new SoapSttpClient(config, new UrlWsdlLoader, pair._2)(pair._1))
      }

    trait Service {
      def readData(action: String, wsdlUrl: String)(data: Map[String, List[String]]): Task[GenericStreamingResponse]
      def readData(service: String, action: String, xmlElement: String, namespace: NamespaceBinding)
                  (data: Map[String, List[String]]): Task[Stream[Throwable, Map[String, String]]]
    }
  }
}
