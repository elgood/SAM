package sal.parsing.sam.statements

import scala.collection.mutable.HashMap
import sal.parsing.sam.BaseParsing
import com.typesafe.scalalogging.LazyLogging

trait Transform extends BaseParsing with LazyLogging {

  /*def transformStatement: Parser[TransformStatement] = {
    identifier ~ "=" ~ forEachKeyWord ~ identifier ~ transformKeyWord ~
      transformExpression ~ ":" ~ identifier  ~ ";" ^^ 
    {
      case lstream ~ eq ~ fkw ~ rstream ~ tkw ~ expression ~ colon ~ field ~ colon =>
      {
        logger.info("TransformStatement")
        TransformStatement(lstream, rstream, expression, field, memory)
      }
    }
  }*/

  //def transformExpression = transformExpression + term  
   
  //def term 


}

case class TransformStatement(lstream: String,
                             rstream: String,
                             features: List[String],
                             memory: HashMap[String, String])
  extends Statement with LazyLogging
{


}

