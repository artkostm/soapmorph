package com.petrofac.data.ingest.p6

import com.petrofac.data.ingest.p6.config.SoapTransportConfig
import com.petrofac.data.ingest.p6.transport.SoapTransport.PayloadTransformer
import com.petrofac.data.landing.rest.fs.AvroWriter
import com.petrofac.data.soap.client.SoapClient
import izumi.reflect.Tag
import zio.{Has, Task, ZIO, ZLayer}

package object transport {
  type SoapTransport = Has[SoapTransport.Service]

  final def run(enrichPayload: PayloadTransformer = Seq(_)): ZIO[SoapTransport, Throwable, Unit] =
    ZIO.accessM(_.get.run(enrichPayload))

  object SoapTransport {

    type PayloadTransformer = Map[String, List[String]] => Seq[Map[String, List[String]]]

    def live[C <: SoapTransportConfig : Tag]: ZLayer[SoapClient with AvroWriter with Has[C], Nothing, SoapTransport] =
      ZLayer.fromServices[SoapClient.Service, AvroWriter.Service, C, SoapTransport.Service] {
        (client, avro, config) => new SoapDataTransport(client, avro, config)
      }

    trait Service {
      def run(enrichPayload: PayloadTransformer = Seq(_)): Task[Unit]
    }
  }
}
