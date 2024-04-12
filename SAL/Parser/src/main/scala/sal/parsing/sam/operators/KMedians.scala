package sal.parsing.sam.operators

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import sal.parsing.sam.Constants
import sal.parsing.sam.Util

trait KMedians extends BaseParsing {
  val kMediansKeyWord: String = "kmedians"

  def kMediansOperator: Parser[KMediansExp] =
    kMediansKeyWord ~ "(" ~ identifier ~ "," ~ int ~ ")" ^^
      { case kmed ~ lpar ~ id ~ c1 ~ k ~ rpar =>
        KMediansExp(id, k, memory)
      }
}

case class KMediansExp(field: String, k: Int, memory: HashMap[String, String])
    extends OperatorExp(field, memory) with Util {

    override def createOpString(): String = 
    {
      val lstream = memory.getOrElse(Constants.CurrentLStream, "")
      val rstream = memory.getOrElse(Constants.CurrentRStream, "")

      // Get the tuple type of the input stream
      val tupleType = memory.getOrElse(lstream + Constants.TupleType, "")
      memory += Constants.TupleType -> tupleType

    // Generate SAM code for the K-medians operator
    val opString = s"""identifier = "$field";
    auto $field = std::make_shared<KMedians<$tupleType>>($k);
    ${addRegisterStatements(field, rstream, memory)}""".replace("$tupleType", tupleType)

    opString
}


  override def addRegisterStatements(identifier: String, rstream: String, 
                                    memory: HashMap[String, String]): String = {
    val producer = "producer" // Replace with the appropriate producer object
    val subscriber = "subscriber" // Replace with the appropriate subscriber object

    s"""addOperator($identifier);
registerConsumer($identifier, "$identifier");
if ($subscriber != NULL) {
  $producer->registerSubscriber($subscriber, $identifier);
}"""
  }
      
}

