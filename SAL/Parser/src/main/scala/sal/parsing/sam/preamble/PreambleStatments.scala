package sal.parsing.sam.preamble

import scala.collection.mutable.HashMap

import sal.parsing.sam.Constants

case class PreambleStatements(memory : HashMap[String, String])
{
  /*override def toString = {
    var rString = createPipeline
    //rString += addPushPull
    rString
  }

  private def createPipeline =
  {
    val inputType = memory(Constants.ConnectionInputType)

    "void createPipeline(std::shared_ptr<ProducerType> producer,\n" +
    "            std::shared_ptr<FeatureMap> featureMap,\n" +
    "            std::shared_ptr<FeatureSubscriber> subscriber,\n"+
    //"         std::shared_ptr<" + Constants.PartitionType +"> partioner,\n" +
    "            size_t queueLength,\n"+
    "            size_t numNodes,\n"+
    "            size_t nodeId,\n"+
    "            vector<std::string> const& hostnames,\n"+
    "            size_t timeout,\n" +
    "            size_t hwm,\n"+
    "            size_t N,\n"+
    "            size_t b,\n"+
    "            size_t k,\n"+
    "            size_t startingPort)\n"+
    "{\n"+
    "  std::string identifier = \"\";\n"

  }

  private def addPushPull =
  {
    var rString = "  auto partitioner = std::make_shared<PartitionType" +
                  ">(queueLength,\n"
    rString += "                           numNodes,\n"
    rString += "                           nodeId,\n"
    rString += "                           hostnames,\n"
    rString += "                           startingPort,\n"
    rString += "                           timeout,\n"
    rString += "                           false,\n"
    rString += "                           hwm);\n"
    rString
  }*/

}


