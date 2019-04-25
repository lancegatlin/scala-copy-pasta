package org

import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{BiFunction, Function => JFunction}

package object ldg {
    // thread safe map
  type TSMap[A,B] = ConcurrentHashMap[A,B]

  implicit class ConcurrentHashMapExt[A,B](val self: TSMap[A,B]) extends AnyVal {
    def getOrCompute(key: A)(calc: () => B) : B =
      self.computeIfAbsent(key, new JFunction[A,B] {
        override def apply(t: A): B = calc()
      })

    def applyOrCompute(key: A)(calc: B => B) : B =
      self.computeIfPresent(key, new BiFunction[A,B,B] {
        override def apply(t: A, u: B): B = calc(u)
      })
  }

  implicit class AnyEffectExt[A](val self: A) extends AnyVal {
    /**
      * Sugar that allows creating a side-effect in a dot function stream:
      *
      * @param f side-effect function
      * @return self
      */
    def effect(f: A => Unit) : A = {
      f(self)
      self
    }

    /**
      * Transform self
      *
      * @param f transformation function
      * @return new self
      */
    def xfrm(f: A => A) : A = {
      f(self)
    }
  }

  implicit class MapStringStringExt(val self: Map[String,String]) extends AnyVal {
    def toProperties : java.util.Properties = {
      val retv = new java.util.Properties
      self.foreach { case (k,v) => retv.setProperty(k,v) }
      retv
    }
  }

  /**
    * Used to end job execution when a fatal error occurs with a message
    * @param msg message explaining job failure
    */
  def die(msg: String) = throw new RuntimeException(msg)

  implicit class OptionExt[A](val self: Option[A]) extends AnyVal {
    /**
      * Get the value inside an option or end job with a message
      * @param msg if no value, message to end job with
      * @return value
      */
    def getOrDie(msg: String) : A =
      self.getOrElse(die(msg))
  }

  implicit class EitherStringExt[A](val self: Either[String,A]) extends AnyVal {
    /**
      * Get the right value inside an Either or end job with a message
      * @param msg if no value, message to end job with
      * @return value
      */
    def getOrDie(msg: String) : A =
      self.fold(innerMsg => die(s"$msg: $innerMsg"), identity)
  }

  implicit class MapExt[A,B](val self: Map[A,B]) extends AnyVal {
    /**
      * Get the value for a key or end job with a message
      * @param msg if no value, message to end job with
      * @return value
      */
    def getOrDie(key: A, msg: String) : B =
      self.getOrElse(key, die(msg))
  }

  implicit class LocalDateExt(val self: LocalDate) extends AnyVal {
    def toSqlDate : java.sql.Date =
      java.sql.Date.valueOf(self)
  }
}
