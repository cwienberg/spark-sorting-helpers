package cwienberg.spark.sorting

private[sorting] sealed abstract class ResourceOrValue[R, V]
    extends Product
    with Serializable {
  def isResource: Boolean
  def isValue: Boolean

  def getResource: R
  def getValue: V

  def compare(that: ResourceOrValue[R, V]): Int
}

private[sorting] object ResourceOrValue {
  implicit def ordering[R, V: Ordering]: Ordering[ResourceOrValue[R, V]] =
    _.compare(_)
}

private[sorting] final case class Resource[R, V](resource: R)
    extends ResourceOrValue[R, V] {
  override def isResource: Boolean = true
  override def isValue: Boolean = false

  override def getResource: R = {
    resource
  }

  override def getValue: V = {
    throw new IllegalArgumentException("Resource.getValue")
  }

  override def compare(that: ResourceOrValue[R, V]): Int = {
    that match {
      case Resource(_) =>
        throw new IllegalArgumentException(
          "Cannot compare two resources. Do not provide two resources for the same key."
        )
      case Value(_) => -1
    }
  }
}

private[sorting] final case class Value[R, V: Ordering](value: V)
    extends ResourceOrValue[R, V] {
  override def isResource: Boolean = false
  override def isValue: Boolean = true

  override def getResource: R = {
    throw new IllegalArgumentException("Value.getResource")
  }

  override def getValue: V = {
    value
  }

  override def compare(that: ResourceOrValue[R, V]): Int = {
    that match {
      case Resource(_) => 1
      case Value(otherValue) =>
        implicitly[Ordering[V]].compare(value, otherValue)
    }
  }
}
