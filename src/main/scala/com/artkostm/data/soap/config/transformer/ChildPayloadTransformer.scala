package com.artkostm.data.ingest.p6.config.transformer

import com.artkostm.data.soap.client.schema.DataF
import com.artkostm.data.soap.client.schema.DataF.FixedData
import higherkindness.droste.data.Fix

sealed trait ChildPayloadTransformer extends ((List[Fix[DataF]], Map[String, List[String]]) => Seq[Map[String, List[String]]])

object ChildPayloadTransformer {
  object Identity extends ChildPayloadTransformer {
    override def apply(rows: List[Fix[DataF]], payload: Map[String, List[String]]): Seq[Map[String, List[String]]] = Seq(payload)
  }

  object RiskAssignmentSpreadTransformer extends ChildPayloadTransformer {
    override def apply(rows: List[Fix[DataF]], payload: Map[String, List[String]]): Seq[Map[String, List[String]]] = {
      Seq(
        payload ++ Seq(
          "ResourceAssignmentObjectId" -> rows.flatMap(gr => gr.get("ObjectId").map(_.toString)),
          "StartDate" -> List(rows.flatMap { r =>
            r.get("ActualStartDate").map(_.toString) orElse
              r.get("StartDate").map(_.toString)
          }.min),
          "EndDate" -> List(rows.flatMap { r =>
            r.get("PlannedFinishDate").map(_.toString) orElse
              r.get("ActualFinishDate").map(_.toString) orElse
              r.get("FinishDate").map(_.toString)
          }.max)
        )
      )
    }
  }
}
