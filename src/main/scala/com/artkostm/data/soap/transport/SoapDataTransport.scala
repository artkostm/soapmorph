package com.artkostm.data.soap.transport

import com.artkostm.data.soap.client.SoapClient
import com.artkostm.data.soap.config.{SoapTransportConfig, ServiceEntity}
import com.artkostm.data.soap.transport.SoapTransport.PayloadTransformer
import com.artkostm.data.landing.rest.avro.AvroHelper.{createAvroSchemaFromRow, createGenericRecord}
import com.artkostm.data.landing.rest.fs.AvroWriter
import com.artkostm.data.landing.rest.logger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.{Logger, LoggerFactory}
import zio._
import zio.clock.Clock
import zio.stream._

import java.time.{Duration, LocalDateTime, Clock => JClock}
import java.util.UUID

class SoapDataTransport(client: SoapClient.Service, avroWriter: AvroWriter.Service, config: SoapTransportConfig)
  extends SoapTransport.Service {
  import SoapDataTransport._

  type ApiCall = (ServiceEntity, Map[String, List[String]]) => Task[Stream[Throwable, Map[String, String]]]

  override def run(enrichPayload: PayloadTransformer): Task[Unit] =
    ZIO.foreach_(config.transport) {
      case (entityName, c) =>
        val partition = partitions(entityName, LocalDateTime.now(JClock.systemUTC()), config.launchId)
        Ref.make(false).flatMap { dataWritten =>
          load(dataWritten, entityName, c, enrichPayload(c.payload), partition) { (se, payload) =>
            delayCall(c) *> client.readEntity(se.service, se.action, se.xmlElement, se.namespace.value)(payload)
          } *> dataWritten.get >>= (ZIO.when(_)(logger.state(config, entityName, partition, "SUCCESS")))
        }
    }

  private def load(dataWritten: Ref[Boolean],
                   entityName: String,
                   c: ServiceEntity,
                   payload: Seq[Map[String, List[String]]],
                   partition: String)(call: ApiCall): Task[Unit] =
    for {
      _ <- logger.info(s"Loading data for $entityName...")
      streams <- ZIO.foreachParN(config.parallelization)(payload) { p =>
        call(c, p).map(s => s.chunkN(config.chunkSize))
      }
      mainStream = streams.foldLeft(Stream.succeed(Map.empty[String, String]): Stream[Throwable, Map[String,String]]) {
        case (accStream, stream) => accStream.filter(_.nonEmpty).merge(stream)
      }.broadcast(c.children.size + 1, MaxLag)
      _ <- mainStream.use {
        case toSave :: tail =>
          for {
            _ <- toSave.foreachChunk { chunk =>
              chunk.headOption
                .map(createAvroSchemaFromRow)
                .map { avroSchema =>
                  saveAvro(chunk.map(row => createGenericRecord(row, avroSchema)), avroSchema)(
                    dataWritten,
                    avroWriter,
                    partition
                  )
                }
                .getOrElse(ZIO.unit)
            }
            _ <- ZIO.foreach_(tail.zip(c.children)) { case (parentStream, (childEntity, serviceConfig)) =>
              val childEntityPartition = partitions(childEntity, LocalDateTime.now(JClock.systemUTC()), config.launchId)
              Ref.make(false).flatMap { writeFlag =>
                parentStream.chunkN(serviceConfig.subsequentBatchSize).foreachChunk { rowChunk =>
                  val updatedPayload = serviceConfig.childPayloadTransformer(rowChunk.toList, serviceConfig.payload)
                  load(writeFlag, childEntity, serviceConfig, updatedPayload, childEntityPartition)(call)
                } *> writeFlag.get >>= (ZIO.when(_)(logger.state(config, childEntity, childEntityPartition, "SUCCESS")))
              }
            }
          } yield ()
      }
      _ <- logger.info("...done")
    } yield ()
}

object SoapDataTransport {
  val MaxLag = 1000

  protected def delayCall(s: ServiceEntity) = s.delayCall match {
    case Some(duration) =>
      ZIO.unit.delay(Duration.ofSeconds(duration.toSeconds)).provideLayer(Clock.live)
    case None => ZIO.unit
  }

  protected def saveAvro(chunk: Chunk[GenericRecord], avroSchema: Schema)(
    dataWritten: Ref[Boolean],
    avroWriter: AvroWriter.Service,
    partition: String
  ): ZIO[Any, Throwable, String] =
    avroWriter.writeAvro(s"$partition/data_${UUID.randomUUID()}.avro", chunk, avroSchema) <*
    dataWritten.set(true)

  protected def partitions(root: String, ldt: LocalDateTime, id: String) =
    f"$root/year=${ldt.getYear}/month=${ldt.getMonth.getValue}%02d/day=${ldt.getDayOfMonth}%02d/hour=${ldt.getHour}%02d/etlid=$id"

  protected implicit val LOGGER: Logger = LoggerFactory.getLogger(getClass)
}
