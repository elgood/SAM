package sal.parsing.sam

import scala.util.parsing.combinator.RegexParsers
import scala.collection.mutable.HashMap

/**
 * @author elgood
 */
trait BaseParsing extends RegexParsers with uuid {

  // This keeps track of things specified in the preamble that
  // have to be used later.  It also keeps track of variables 
  // (e.g. vector of key fields) that have been associated with
  // variables specified in the script.
  protected var memory = new HashMap[String, String]
 

  def posInt        = """[1-9][0-9]*""".r ^^ {_.toInt}
  def negInt        = """-[1-9][0-9]*""".r ^^ {_.toInt}
  def int           = posInt | negInt | "0".r ^^ {_.toInt}
  
  def float         = """[+-]?([0-9]*)?[.][0-9]+""".r ^^ {_.toFloat}
  def identifier    = """[_\p{L}][_\p{L}\p{Nd}]*""".r ^^ {_.toString}
  def identifiers   = repsep(identifier , ",")
  
  // Keywords
  def filterKeyWord = "(?i)filter".r
  def streamKeyWord = "(?i)stream".r 
  def byKeyWord     = "(?i)by".r 
  def forEachKeyWord = "(?i)foreach".r 
  def generateKeyWord = "(?i)generate".r
  def hashKeyWord = "(?i)hash".r
  def withKeyWord = "(?i)with".r
  def partitionKeyWord = "(?i)partition".r
  
  // Preamble related key words for operator default values
  def windowSizeKeyWord = "(?i)windowsize".r
  def basicWindowSizeKeyWord = "(?i)basicwindowsize".r
  def ehKKeyWord = "(?i)ehk".r | "(?i)ExponentialHistogramK".r
  def topKKKeyWord = "topk".r
  
  // Preamble related key words for sam implementation parameters
  def queueLengthKeyWord = "(?i)queuelength".r
  def highWaterMarkKeyWord = "(?i)highwatermark".r | "(?i)hwm".r
 
  // TODO: come up with better url pattern 
  def url = """\".*\"""".r ^^ {_.toString}
  def port = """[0-9][0-9]*""".r ^^ {_.toInt}
  
  // Operators
  def topKKeyWord = "(?i)topk".r 
  def ehSumKeyWord = "(?i)ehsum".r
  def sumKeyWord = "(?i)sum".r
  def ehAveKeyWord = "(?i)ehave".r
  def aveKeyWord = "(?i)ave".r
  def ehVarKeyWord = "(?i)ehvar".r
  def varKeyWord = "(?i)var".r
  def simpleSumKeyWord = "(?i)simplesum".r
  
  // Arithmetic Operators
  def arithmeticOperator = plus | minus
  def plus = "[+]".r
  def minus = "[-]".r

  // Hash functions
  def hashFunction = ipHashFunction
  def ipHashFunction = "(?i)iphashfunction".r
}

