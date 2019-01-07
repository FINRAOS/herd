package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import scala.util.{Success, Try}

object FileStatusShim {

  val serializableFileStatusClazz =
    Try(Class.forName("org.apache.spark.sql.execution.datasources.SerializableFileStatus")) match {
      case Success(clazz) => Some(clazz)
      case _ => None
    }

  val isDBR = serializableFileStatusClazz.isDefined

  val fromStatus = serializableFileStatusClazz.map(_.getDeclaredMethod("fromStatus", classOf[FileStatus]))

  private val getPath = serializableFileStatusClazz.map(_.getDeclaredMethod("getPath"))

  private val getLen = serializableFileStatusClazz.map(_.getDeclaredMethod("getLen"))

  def getPath(status: Any): Path = status match {
    case status if serializableFileStatusClazz.isDefined =>
      getPath.get.invoke(status).asInstanceOf[Path]
    case status: FileStatus => status.getPath()
  }

  def getLen(status: Any): Long = status match {
    case status if serializableFileStatusClazz.isDefined =>
      getLen.get.invoke(status).asInstanceOf[Long]
    case status: FileStatus => status.getLen
  }

}

object PartitionDirectoryShim {
  import FileStatusShim._

  private val constructor = serializableFileStatusClazz.map { serType =>
    val clazz = Class.forName("org.apache.spark.sql.execution.datasources.PartitionDirectory")
    clazz.getConstructor(classOf[InternalRow], classOf[Seq[serType.type]])
  }

  def apply(values: InternalRow, files: Seq[FileStatus]): PartitionDirectory = {
    if (isDBR) {
      constructor.get
        .newInstance(values, files.map(fromStatus.get.invoke(null, _)))
        .asInstanceOf[PartitionDirectory]
    } else {
      PartitionDirectory(values, files)
    }
  }

}
