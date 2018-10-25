package utils

object CustomOrdering {

  object SecondValueSorting extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      a._2.compare(b._2)
    }
  }

  object SecondValueLongSorting extends Ordering[(String, Long)] {
    def compare(a: (String, Long), b: (String, Long)) = {
      a._2.compare(b._2)
    }
  }

}
