package cn.bicon.test.sparkCore.distrbute

import java.io.ObjectOutputStream
import java.net.Socket

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 16:41
  **/
object Driver {
  def main(args: Array[String]): Unit = {

    val client1 = new Socket("localhost",8888)
    val out1 = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)
    val task = new Task()

    val subTask1 = new SubTask()
    //取出前两个将数据 复制给 subTask1
    subTask1.datas = task.datas.take(2)
    subTask1.logic = task.logic
    objOut1.writeObject(subTask1)
    objOut1.flush()
    objOut1.close()
    client1.close()

    val client2 = new Socket("localhost",9999)
    val out2 = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)

    val subTask2 = new SubTask()
    //取出前两个将数据 复制给 subTask2
    subTask2.datas = task.datas.takeRight(2)
    subTask2.logic = task.logic
    objOut2.writeObject(subTask2)
    objOut2.flush()
    objOut2.close()
    client2.close()

  }
}
