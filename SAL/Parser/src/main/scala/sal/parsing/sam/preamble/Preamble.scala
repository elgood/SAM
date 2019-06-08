package sal.parsing.sam.preamble

import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

trait Preamble extends BaseParsing with LazyLogging
{
  // This parses the preamble statements and stores the values in a hashmap.

  def preambleStatement = queueLengthStatement | highWaterMarkStatement |
    ehKStatement | basicWindowSizeStatement | windowSizeStatement |
    topKKStatement

  /********************* Preamble stuff *******************/
  def queueLengthStatement =
    queueLengthKeyWord ~ "=" ~ posInt ~ ";" ^^
    {case kw ~ eq ~ length ~ semi =>
      memory += Constants.QueueLength -> length.toString}
 
  def highWaterMarkStatement =
    highWaterMarkKeyWord ~ "=" ~ posInt ~ ";" ^^
    {case kw ~ eq ~ hwm ~ semi =>
      logger.info("Preamble.highWaterMarkStatement")
      memory += Constants.HighWaterMark -> hwm.toString}
 
  def ehKStatement =
    ehKKeyWord ~ "=" ~ posInt ~ ";" ^^
    {case kkw ~ eq ~ k ~ semi =>
      memory += Constants.EHK -> k.toString}

  def basicWindowSizeStatement =
    basicWindowSizeKeyWord ~ "=" ~ posInt ~ ";" ^^
    {case bwskw ~ eq ~ size ~ semi =>
      memory += Constants.BasicWindowSize -> size.toString}

  def windowSizeStatement =
    windowSizeKeyWord ~ "=" ~ posInt ~ ";" ^^
    {case wskw ~ eq ~ size ~ semi =>
     memory += Constants.WindowSize -> size.toString}

  def topKKStatement =
    topKKKeyWord ~ "=" ~ posInt ~ ";" ^^
    {case tkkkw ~ eq ~ size ~ semi =>
      memory += Constants.TopKK -> size.toString}
}


