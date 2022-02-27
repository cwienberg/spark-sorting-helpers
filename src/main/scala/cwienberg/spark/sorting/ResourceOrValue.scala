package cwienberg.spark.sorting

private[sorting] sealed abstract class ResourceOrValue[R, V] extends Product with Serializable with Ordered[ResourceOrValue[R,V]] {
  def isResource: Boolean
  def isValue: Boolean

  def getResource: R
  def getValue: V
}

private[sorting] final case class Resource[R, V](resource: R) extends ResourceOrValue[R, V] {
  def isResource: Boolean = true
  def isValue: Boolean = false

  override def getResource: R = {
    resource
  }

  override def getValue: V = {
    throw new IllegalArgumentException("Resource.getValue")
  }

  override def compare(that: ResourceOrValue[R, V]): Int = {
    that match {
      case Resource(_) => throw new IllegalArgumentException(
        "Cannot compare two resources. Do not provide two resources for the same key."
      )
      case Value(_) => -1
    }
  }
}

private[sorting] final case class Value[R, V: Ordering](value: V) extends ResourceOrValue[R, V] {
  def isResource: Boolean = false
  def isValue: Boolean = true

  override def getResource: R = {
    throw new IllegalArgumentException("Value.getResource")
  }

  override def getValue: V = {
    value
  }

  override def compare(that: ResourceOrValue[R, V]): Int = {
    that match {
      case Resource(_) => 1
      case Value(otherValue) => implicitly[Ordering[V]].compare(value, otherValue)
    }
  }
}
