package cn.bicon.test.sparkCore.distrbute

/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-18 16:52
  **/
class Task extends  Serializable {
  val datas = List(1, 2, 3, 4)
   //val logic = (num: Int) => {num * 2}
  val logic : (Int) => Int = _ * 2

  //计算
  def compute() = {
    datas.map(logic)
  }

}
