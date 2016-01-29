package batch_layer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

object HdfsUtils {

  def getConfig(url: String): Configuration = {
    val hc = new Configuration();
    hc.set("fs.default.name", url);
    hc
  }

  def getFileSystem(conf: Configuration): FileSystem = FileSystem.get(conf);

  def getFileSystem(url: String): FileSystem = getFileSystem(getConfig(url))

  def listFiles(fileSystem: FileSystem, sourceDir: String, recursive: Boolean)(filePathFilter: String => Boolean = Function.const(true)): Option[Seq[String]] = {
    val path = new Path(sourceDir)

    var result = Seq.empty[String]

    if (fileSystem.exists(path)) {
      val files = fileSystem.listFiles(path, recursive)

      while (files.hasNext) {
        val next = files.next()

        if (next.isFile) {
          val filePath = next.getPath.toString

          if (filePathFilter(filePath)) result = filePath +: result
        }
      }

      Option(result)
    }
    else None
  }

  def deleteFile(fileSystem: FileSystem, toDelete: String, recursive: Boolean) = fileSystem.delete(new Path(toDelete), recursive)
}
