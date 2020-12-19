package cn.bicon.test.standalong

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 16:46
  **/
object Executor {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("等待客户端连接")

    //等待连接
    val client = server.accept()


    val input = client.getInputStream
    val objInput = new ObjectInputStream(input)
    val task: Task = objInput.readObject().asInstanceOf[Task]

    val ints: List[Int] = task.compute()
    println("计算节点结果为:" + ints)
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
