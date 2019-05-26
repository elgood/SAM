package sal.parsing.sam.statements

import scala.collection.mutable.HashMap

import sal.parsing.sam.uuid
import sal.parsing.sam.Constants



/**
 * @author elgood
 */
case class FunctionToken (id : String, 
                             function : String,
                             index : Int,
                             memory: HashMap[String, String])
                             extends uuid
{  
  override def toString = {
    val indexVar = "index" + uuid
    var rString = "  int " + indexVar + " = " + index + "\n"
    val operatorType = memory.get(id + Constants.OperatorType).get
    val inputType = memory.get(id + Constants.InputType).get
    operatorType match 
    {
      case Constants.TopKKey => 
        
        val functionVar = "function" + uuid
        val tokenVar = "funcToken" + uuid
        rString = rString + "  auto " + functionVar + " = [&" + indexVar + "](Feature " +
          "const * feature)->double {\n" +
          "    auto topKFeature = static_cast<TopKFeature const *>(feature);\n" +
          "    return topKFeature->getFrequences()[index1];\n" +
          "  };\n" +
          "  auto " + tokenVar + " = std::make_shared<FuncToken<Netflow>>(featureMap,\n" +
          "                                                     " + functionVar + ", \n" +
          "                                                     \"" + id + "\");\n"
          
      
    }
    rString
  }
}

