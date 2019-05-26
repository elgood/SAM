package sal.parsing.sam


/**
 * @author elgood
 */
object TestParsing extends Parsing {
  
  def main(args: Array[String]) {
    
    parse(posInt, "1") match
    {
      case Success(matched,_) => println(matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    
    parse(int, "-11") match
    {
      case Success(matched,_) => println(matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    
    parse(float, "0.9") match
    {
      case Success(matched,_) => println(matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    
    memory += Constants.CurrentInputType -> TupleTypes.VastNetflow
    parse(token, "0") match
    {
      case Success(matched,_) => println(matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)  
    }
    memory.clear
    
    memory += Constants.CurrentInputType -> TupleTypes.VastNetflow
    parseAll(token, "0.9")
        match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    memory += "top2" + Constants.OperatorType -> Constants.TopKKey
    memory += "top2" + Constants.InputType -> TupleTypes.VastNetflow
    parseAll(functionToken, "top2.value(1)")
      match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    memory += Constants.CurrentInputType -> TupleTypes.VastNetflow
    parseAll(arithmeticExpression, "0.7")
      match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    parseAll(arithmeticOperator, "+")
      match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    
    memory += Constants.CurrentInputType -> TupleTypes.VastNetflow
    parseAll(floatToken, "0.9")
      match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    
    memory += "top2" + Constants.OperatorType -> Constants.TopKKey
    memory += "top2" + Constants.InputType -> TupleTypes.VastNetflow
    parseAll(arithmeticExpression, "0.5 + top2.value(0)")
      match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    /************** ehsum *****************/
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement, 
              "feature1 = FOREACH VerticesBySrc GENERATE ehsum(SrcTotalBytes,10000,2);")
    match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    /************** sum *****************/
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement, 
              "feature1 = FOREACH VerticesBySrc GENERATE sum(SrcTotalBytes);")
    match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    /*************** ehave *************/
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement, 
              "feature1 = FOREACH VerticesBySrc GENERATE ehave(SrcTotalBytes,10000,2);")
    match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    /*************** ave *************/
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement, 
              "feature1 = FOREACH VerticesBySrc GENERATE ave(SrcTotalBytes);")
    match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    /*************** ehvar *************/
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement, 
              "feature1 = FOREACH VerticesBySrc GENERATE ehvar(SrcTotalBytes,10000,2);")
    match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    /*************** var *************/
    memory += "VerticesBySrc" + Constants.NumKeysStr -> 1.toString
    memory += "VerticesBySrc" + Constants.KeyStr + 0 -> "DestIp"
    memory += "VerticesBySrc" + Constants.TupleTypeStr -> "Netflow"
    parseAll(forEachStatement, 
              "feature1 = FOREACH VerticesBySrc GENERATE var(SrcTotalBytes);")
    match
    {
      case Success(matched,_) => println("MATCHED: " + matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
    memory.clear
    
    parse(document,
          "Edges = FlowStream(\"localhost\", 9999);\n" +
          "VerticesByDest = STREAM edges BY DestIp;\n" +
          "VerticesBySrc = STREAM edges BY DestIp;\n" +
          "feature1 = FOREACH VerticesBySrc GENERATE ehave(SrcTotalBytes,10000,2);\n" +
          "feature2 = FOREACH VerticesBySrc GENERATE ehvar(SrcTotalBytes,10000,2);\n" + 
          "feature3 = FOREACH VerticesBySrc GENERATE ehave(DestTotalBytes,10000,2);\n" +
          "feature4 = FOREACH VerticesBySrc GENERATE ehvar(DestTotalBytes,10000,2);\n" +
          "feature5 = FOREACH VerticesBySrc GENERATE ehave(DurationSeconds,10000,2);\n" +
          "feature6 = FOREACH VerticesBySrc GENERATE ehvar(DurationSeconds,10000,2);\n" +
          "feature7 = FOREACH VerticesBySrc GENERATE ehave(SrcPayloadBytes,10000,2);\n" +
          "feature8 = FOREACH VerticesBySrc GENERATE ehvar(SrcPayloadBytes,10000,2);\n" +
          "feature9 = FOREACH VerticesBySrc GENERATE ehave(DestPayloadBytes,10000,2);\n" +
          "feature10 = FOREACH VerticesBySrc GENERATE ehvar(DestPayloadBytes,10000,2);\n" +
          "feature11 = FOREACH VerticesBySrc GENERATE ehave(SrcPacketCount,10000,2);\n" +
          "feature12 = FOREACH VerticesBySrc GENERATE ehvar(SrctPacketCount,10000,2);\n" +
          "feature13 = FOREACH VerticesBySrc GENERATE ehave(DestPacketCount,10000,2);\n" +
          "feature14 = FOREACH VerticesBySrc GENERATE ehvar(DestPacketCount,10000,2);\n" +
          "feature15 = FOREACH VerticesByDest GENERATE ehave(SrcTotalBytes,10000,2);\n" +
          "feature16 = FOREACH VerticesByDest GENERATE ehvar(SrcTotalBytes,10000,2);\n" +
          "feature17 = FOREACH VerticesByDest GENERATE ehave(DestTotalBytes,10000,2);\n" +
          "feature18 = FOREACH VerticesByDest GENERATE ehvar(DestTotalBytes,10000,2);\n" +
          "feature19 = FOREACH VerticesByDest GENERATE ehave(DurationSeconds,10000,2);\n" +
          "feature20 = FOREACH VerticesByDest GENERATE ehvar(DurationSeconds,10000,2);\n" +
          "feature21 = FOREACH VerticesByDest GENERATE ehave(SrcPayloadBytes,10000,2);\n" +
          "feature22 = FOREACH VerticesByDest GENERATE ehvar(SrcPayloadBytes,10000,2);\n" +
          "feature23 = FOREACH VerticesByDest GENERATE ehave(DestPayloadBytes,10000,2);\n" +
          "feature24 = FOREACH VerticesByDest GENERATE ehvar(DestPayloadBytes,10000,2);\n" +
          "feature25 = FOREACH VerticesByDest GENERATE ehave(SrcPacketCount,10000,2);\n" +
          "feature26 = FOREACH VerticesByDest GENERATE ehvar(SrctPacketCount,10000,2);\n" +
          "feature27 = FOREACH VerticesByDest GENERATE ehave(DestPacketCount,10000,2);\n" +
          "feature28 = FOREACH VerticesByDest GENERATE ehvar(DestPacketCount,10000,2);\n"
          )
          match
    {
      case Success(matched,_) => println(matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }    
    
    /*parse(document,
          "Edges = FlowStream(\"localhost\", 9999);\n" +
          "VerticesByDest = STREAM edges BY DestIp;\n" +
          //"verticesByDestSrc = STREAM edges BY DestIp SrcIp;\n" +
          
          "top2 = FOREACH VerticesByDest GENERATE topk(DestPort,10000,1000,2);\n" +
          //"servers = FILTER verticesByDest BY top2.value(0) + top2.value(1) < 0.9;\n"
          "servers = FILTER VerticesByDest BY 0.9;\n"
          //"feature = FOREACH verticesByDest GENERATE topk(DestPort,10000,1000,2);\n" +
          //"feature2 = FOREACH verticesByDest GENERATE ehsum(DestPort,10000,2);\n" + 
          //"feature3 = FOREACH verticesByDest GENERATE ehvar(DestPort,10000,2);\n" +
          //"feature4 = FOREACH verticesByDest GENERATE simplesum(DestPort, 10000);\n" 
        )
         match
    {
      case Success(matched,_) => println(matched)
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }*/
    
  }
}
