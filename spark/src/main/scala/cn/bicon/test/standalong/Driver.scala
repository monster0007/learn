package cn.bicon.test.standalong

import java.io.ObjectOutputStream
import java.net.Socket

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 16:41
  **/
object Driver {
  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost",9999)
    val out = client.getOutputStream
    val objOut = new ObjectOutputStream(out)


    val task = new Task()

    objOut.writeObject(task)
    objOut.flush()
    objOut.close()

    /*out.write(2)
    out.flush()
    out.close()
    */client.close()

  }
}
