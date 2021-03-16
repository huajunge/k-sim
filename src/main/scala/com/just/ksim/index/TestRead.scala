package com.just.ksim.index

import util.WKTUtils

import scala.io.Source

object TestRead {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("D:\\工作文档\\data\\T-drive\\release\\out\\part-00000")
    for (elem <- source.getLines()) {
      val geoString = elem.split("-")(1)
      val geo = WKTUtils.read(geoString)
      geo.toString
    }
  }
}
