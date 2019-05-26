import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers

trait TestUtil extends RegexParsers {

  /**
   * A convenience method that makes sure the given 
   * string matches the given regex.
   */
  def shouldMatch( function: Regex, str: String)
  {
    parseAll(function, str)
      match
    {
      case Success(matched,_) => assert(true)
      case Failure(msg,_) => assert(false) 
      case Error(msg,_) => assert(false) 
    }
  }

  /**
   * A convenience method that makes sure the given 
   * string does not match the given regex.
   */
  def shouldntMatch( function: Regex, str: String)
  {
    parseAll(function, str)
      match
    {
      case Success(matched,_) => assert(false)
      case Failure(msg,_) => assert(true) 
      case Error(msg,_) => assert(true) 
    }
  }

}
