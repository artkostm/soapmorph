package com.artkostm.data.ingest.p6

import com.artkostm.data.ingest.p6.config.transformer.ChildPayloadTransformer
import com.artkostm.data.landing.rest.config.{AppConfig, AzureBlobConfig, ConfigProvider, PureConfig}

import zio.{Layer, ZLayer}

import java.io.File
import java.util.UUID
import scala.concurrent.duration.Duration

// format: off
import pureconfig.configurable._
import pureconfig.generic.auto._
// format: on


package object config {
  import com.artkostm.data.landing.rest.config.PureConfig._
  import com.artkostm.data.soap.config._

  final case class P6TransportConfig(client: SoapClientConfig,
                                     transport: Map[String, ServiceEntity],
                                     azure: AzureBlobConfig = AzureBlobConfig.default,
                                     projectsToLoad: Map[String, Int] = Map.empty,
                                     chunkSize: Int = 10000,
                                     parallelization: Int = 1,
                                     monitoringConnectionKey: String = "",
                                     dataSource: String = "P6Ingest",
                                     appName: String = "P6TransportApp",
                                     launchId: String = UUID.randomUUID().toString,
                                     rootDir: File = new File("out/P6"))
    extends SoapTransportConfig

  trait SoapTransportConfig extends AppConfig {
    val parallelization: Int
    val transport: Map[String, ServiceEntity]
    val chunkSize: Int
  }

  final case class ServiceEntity(action: String,
                                 wsdlUrl: String,
                                 name: String,
                                 payload: Map[String, List[String]] = Map.empty[String, List[String]],
                                 children: Map[String, ServiceEntity] = Map.empty[String, ServiceEntity],
                                 childPayloadTransformer: ChildPayloadTransformer = ChildPayloadTransformer.Identity,
                                 subsequentBatchSize: Int = 50,
                                 delayCall: Option[Duration] = None)


  def configProvider: Layer[Nothing, ConfigProvider[P6TransportConfig]] =
    ZLayer.succeed(new PureConfig[P6TransportConfig])
}
