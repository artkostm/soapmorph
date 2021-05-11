package com.artkostm.data.soap

import com.artkostm.data.landing.rest.config._
import com.sandinh.soap.SOAP
import pureconfig.ConfigConvert
import pureconfig.ConfigConvert.{catchReadError, viaNonEmptyString}
import zio.{Layer, ZLayer}

import scala.xml.NamespaceBinding
// format: off
import pureconfig.configurable._
import pureconfig.generic.auto._
// format: on

import java.io.File
import java.util.UUID
import scala.concurrent.duration.Duration

package object config {
  import com.artkostm.data.landing.rest.config.PureConfig._

  final case class SoapEntityConfig(entity: String)

  final case class SoapAuthConfig(username: Secret, password: Secret) extends AuthConfig

  final case class SoapClientConfig(baseUrl: String,
                                    auth: SoapAuthConfig,
                                    retry: RetryConfig = RetryConfig.default,
                                    readTimeOut: Duration = Duration("10 minutes"))
    extends ClientConfig

  trait SoapTransportConfig extends AppConfig {
    val parallelization: Int
    val transport: Map[String, ServiceEntity]
    val chunkSize: Int
  }

  sealed trait ChildPayloadTransformer extends ((List[Map[String, String]], Map[String, List[String]]) => Seq[Map[String, List[String]]])

  object ChildPayloadTransformer {
    object Identity extends ChildPayloadTransformer {
      override def apply(rows: List[Map[String, String]], payload: Map[String, List[String]]): Seq[Map[String, List[String]]] = Seq(payload)
    }

    object RiskAssignmentSpreadTransformer extends ChildPayloadTransformer {
      override def apply(rows: List[Map[String, String]], payload: Map[String, List[String]]): Seq[Map[String, List[String]]] = {
        Seq(
          payload ++ Seq(
            "ResourceAssignmentObjectId" -> rows.flatMap(_.get("ObjectId")),
            "StartDate"                  -> List(rows.flatMap(r => r.get("FinishDate")).min),
            "EndDate"                    -> List(rows.flatMap(r => r.get("ActualFinishDate") orElse r.get("PlannedFinishDate") orElse r.get("FinishDate")).max)
          )
        )
      }
    }
  }

  final case class ServiceEntity(service: String,
                                 action: String,
                                 xmlElement: String,
                                 namespace: NsWrapper,
                                 payload: Map[String, List[String]] = Map.empty,
                                 children: Map[String, ServiceEntity] = Map.empty,
                                 childPayloadTransformer: ChildPayloadTransformer = ChildPayloadTransformer.Identity,
                                 subsequentBatchSize: Int = 50,
                                 delayCall: Option[Duration] = None,
                                 name: String)

  final case class NsWrapper(value: NamespaceBinding) extends AnyVal

  implicit val namespaceConverter: ConfigConvert[NsWrapper] =
    viaNonEmptyString[NsWrapper](
      catchReadError { key =>
        NsWrapper(NamespaceBinding("v1", key, SOAP.SoapNS))
      },
      _.value.uri
    )


}
