import org.scalatest.FlatSpec
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.TupleTypes

/**
 * In this class we conduct unit tests regarding the preamble keywords
 */
class PreambleKeyWordsSpec extends FlatSpec with BaseParsing with TestUtil {

  "The windowsize preamble keyword" should "match regardless of case" in
  {
    shouldMatch(windowSizeKeyWord, "windowsize")
    shouldMatch(windowSizeKeyWord, "WindowSize")
    shouldMatch(windowSizeKeyWord, "WINDOWSize")
    shouldntMatch(windowSizeKeyWord, "WINDOWSizes")
  } 

  "The basicwindowsize preamble keyword" should "match regardless of case" in
  {
    shouldMatch(basicWindowSizeKeyWord, "basicwindowsize")
    shouldMatch(basicWindowSizeKeyWord, "BasicWindowSize")
    shouldMatch(basicWindowSizeKeyWord, "bASicWINDOWSize")
    shouldntMatch(basicWindowSizeKeyWord, "basicWINDOWSizes")
  }


}
