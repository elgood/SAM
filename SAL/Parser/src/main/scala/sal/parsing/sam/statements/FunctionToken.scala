package sal.parsing.sam.statements

import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.LazyLogging
import sal.parsing.sam.Constants



object FunctionToken { 
  var functionCount = 0 
  var tokenCount = 0  
}

/**
 * This class represents all the functions that can be expressed in a filter
 * expression.  Right now, there is only one defined, i.e. value(index)
 * for topk variables.  This can be expanded to include other functions
 * for other operator types and functions.  However, there is the limitation
 * that the function have the same schema for the function, i.e. a single
 * int as the parameter.
 *
 * @param id The variable upon which a function is being called (e.g. top2)
 * @param function The function being used (e.g. value)
 * @param index The value of the first parameter of the function.
 * @param memory The HashMap that is storing state from line to line of the
 *   program.
 *
 * @author elgood
 */
case class FunctionToken (id : String, 
                             function : String,
                             index : Int,
                             memory: HashMap[String, String])
extends LazyLogging
{  

  override def toString = {
    logger.info("FunctionToken.toString")

    var rString = ""

    // There can be different function tokens.  We find out what kind it
    // is by looking into memory.  This value should have been set by 
    // 
    val operatorType = memory(id + Constants.OperatorType)

    operatorType match 
    {
      case Constants.TopKKey => 

        function match
        {
          case "value" =>

            // The current left stream (variable being assigned) and its 
            // associated tuple type should have been placed in memory.  
            // We need the tuple type for a template parameter of the SAM class.
            val lstream = memory( Constants.CurrentLStream )
            val tupleType = memory( lstream + Constants.TupleTypeStr )

            
            // Create unique variable names for both the lambda function
            // and the function token.  Increment the counters so that 
            // the variable names will be unique from later instances.
            val functionVar = "function" + FunctionToken.functionCount
            FunctionToken.functionCount += 1
            val tokenVar = "funcToken" + FunctionToken.tokenCount
            FunctionToken.tokenCount += 1

            rString = rString + "  auto " + functionVar + " = [](Feature " +
              "const * feature)->double {\n" +
              "    int index  = " + index + ";\n" + 
              "    auto topKFeature = " +
              "static_cast<TopKFeature const *>(feature);\n"+
              "    return topKFeature->getFrequences()[index];\n" +
              "  };\n\n" +
              "  auto " + tokenVar + 
              " = std::make_shared<FuncToken<" + tupleType +
              ">>(featureMap,\n" +
              "                                                     " +
              functionVar + ", \n" +
              "                                                     \"" +
              id + "\");\n" +
              "\n"
        }
    }
    rString
  }
}

