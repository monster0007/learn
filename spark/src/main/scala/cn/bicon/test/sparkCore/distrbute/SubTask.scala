package cn.bicon.test.sparkCore.distrbute



/**
  * @program: learn
  * @author: shiyu
  * @create: 2020-12-19 14:07
  **/

class SubTask extends  Serializable {

  // 数据
  var datas: List[Int] = _
  //计算逻辑
  var logic : (Int) => Int = _
  //计算
  //计算
  def compute() = {
    datas.map(logic)
  }}