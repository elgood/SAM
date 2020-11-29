package sal.parsing.sam.statements

import sal.parsing.sam.BaseParsing
import com.typesafe.scalalogging.LazyLogging

trait Transform extends BaseParsing with LazyLogging {

  /*def transformStatement: Parser[TransformStatement] = {
    identifier ~ "=" ~ foreachKeyWord ~ identifier ~ transformKeyWord ~
      transformExpression ~ ";" ^^ 
    {
      case lstream ~ eq ~ fkw ~ rstream ~ tkw ~ expression ~ colon =>
      {
        logger.info("FilterStatement")
        TransformStatement(lstream, rstream, expression, memory)
      }
    }
  }*/

  //def transformExpression = transformExpression + term  
   
  //def term 


}
