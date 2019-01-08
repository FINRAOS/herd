/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.sql.herd

import java.net.URI

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/** Provides a minimal shim to emulate an actual S3A filesystem implementation for unit testing */
class MockS3AFileSystem extends FileSystem {
  private val local = new LocalFileSystem()
  private var uri: URI = _

  override def rename(path: Path, path1: Path): Boolean = {
    local.rename(localizePath(path), localizePath(path1))
  }

  override def listStatus(path: Path): Array[FileStatus] = {
    local.listStatus(localizePath(path))
  }

  override def append(path: Path, i: Int, progressable: Progressable): FSDataOutputStream = {
    local.append(localizePath(path), i, progressable)
  }

  override def delete(path: Path, b: Boolean): Boolean = {
    local.delete(localizePath(path), b)
  }

  override def setWorkingDirectory(path: Path): Unit = {
    local.setWorkingDirectory(localizePath(path))
  }

  override def mkdirs(path: Path, fsPermission: FsPermission): Boolean = {
    local.mkdirs(localizePath(path), fsPermission)
  }

  override def getWorkingDirectory: Path = local.getWorkingDirectory

  override def open(path: Path, i: Int): FSDataInputStream = {
    local.open(localizePath(path), i)
  }

  override def create(path: Path, fsPermission: FsPermission, b: Boolean, i: Int, i1: Short, l: Long, progressable: Progressable): FSDataOutputStream = {
    local.create(localizePath(path), fsPermission, b, i, i1, l, progressable)
  }

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)

    uri = URI.create(name.getScheme + "://" + name.getAuthority)

    local.initialize(URI.create("file://" + FilenameUtils.separatorsToUnix(System.getProperty("user.dir"))), conf)
  }

  override def getScheme(): String = "s3a"

  override def getUri: URI = uri

  override def getFileStatus(path: Path): FileStatus = {
    local.getFileStatus(localizePath(path))
  }

  private def localizePath(path: Path): Path = new Path(uri.relativize(path.toUri))
}
