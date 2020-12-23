package cn.bicon.test.sparkCore.distrbute

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 16:46
  **/
object Executor2 {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(8888)
    println("等待客户端连接")

    //等待连接
    val client = server.accept()


    val input = client.getInputStream
    val objInput = new ObjectInputStream(input)
    val task: SubTask = objInput.readObject().asInstanceOf[SubTask]

    val ints: List[Int] = task.compute
    println("计算节点[8888]结果为:" + ints)
    input.close()
    objInput.close()
    /*
    val res = input.read()
    println("接收到客户端发送的数据"+res)

    client.close()
    input.close()*/
    server.close()
  }
}
