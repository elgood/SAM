package sal.parsing.sam

import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.LazyLogging

import sal.parsing.sam.preamble.Preamble
import sal.parsing.sam.statements.ConnectionStatement
import sal.parsing.sam.statements.PartitionStatement
import sal.parsing.sam.statements.Statement
import sal.parsing.sam.statements.HashWithStatement
import sal.parsing.sam.subgraph.SubgraphQuery


case class EntireQuery(connectionStatement : ConnectionStatement,
                       partitionStatement : PartitionStatement,
                       hashStatements : List[HashWithStatement],
                       queryStatements: List[Statement],
                       subgraphs: List[SubgraphQuery],
                       memory: HashMap[String, String])
  extends Util with LazyLogging
{
  override def toString = {
    
    logger.info("EntireQuery.toString")

    var rString = opening + "\n"

    // We have to run this before some of the other toString methods
    // becuase it sets the input tuple type in the memory hashmap that
    // is used by other toString methods.
    val connectionString = connectionStatement.toString

    // We run the hash statements before the partitionStatement because
    // the partitionStatement needs the hash functions for its template.
    rString += hashDecl

    rString += partitionStatement.toString
    rString += createPipelineDecl
    rString = queryStatements.foldLeft(rString)((agg,e) => agg + e.toString)
    rString = subgraphs.foldLeft(rString)((agg,e) => agg + e.toString)
    rString += "}\n\n"
    rString += main
    rString += connectionString 
    rString += closing
    rString
  }
  
  private def opening = 
  {
    logger.info("EntireQuery.opening")
    "#include <string>\n" +
    "#include <vector>\n" +
    "#include <stdlib.h>\n" +
    "#include <iostream>\n" +
    "#include <chrono>\n" +
    "#include <boost/program_options.hpp>\n" +
    "#include <sam/sam.hpp>\n" +
    "using std::string;\n" +
    "using std::vector;\n" +
    "using std::cout;\n" +
    "using std::endl;\n" +
    "namespace po = boost::program_options;\n" +
    "using namespace std::chrono;\n" +
    "using namespace sam;\n" 
  }

  private def hashDecl =
  {
    logger.info("EntireQuery.hashDecl")
    var hashesString = ""
    hashesString = flatten(hashStatements.productIterator.toList).
                         foldLeft(hashesString)((agg,e) => agg + e.toString)

    "// Hash function(s) used to physically partition the tuples\n" +
    "// across the cluster\n" +
    hashesString +
    "\n"

  }

  private def createPipelineDecl =
  {
    logger.info("EntireQuery.createPipelineDecl")
    val inputType = memory(Constants.ConnectionInputType)
     
    "void createPipeline(std::shared_ptr<ProducerType> producer,\n" +
    "            std::shared_ptr<FeatureMap> featureMap,\n" +
    "            std::shared_ptr<FeatureSubscriber> subscriber,\n"+
    "            size_t numNodes,\n" +
    "            size_t nodeId,\n" +
    "            std::vector<std::string> hostnames,\n" +
    "            size_t startingPort,\n" +
    "            size_t hwm,\n" +
    "            size_t graphCapacity,\n" + 
    "            size_t tableCapacity,\n" +
    "            size_t resultsCapacity,\n" +
    "            size_t numSockets,\n" +
    "            size_t numPullThreads,\n" +
    "            size_t timeout,\n" +
    "            double timeWindow,\n" +
    "            size_t queueLength)\n"+
    "{\n"+
    "  std::string identifier = \"\";\n" +
    "\n"
    
  }


  /**
   * This generates the variables declared at the beginning of the main
   * method.
   */
  private def mainVariables = 
  {
    logger.info("EntireQuery.mainVariables")

    "  /******************* Variables ******************************/\n" +
    "\n" +
    "  // There is assumed to be numNodes nodes within the cluster.\n" +
    "  // Each node has name prefix[0,numNodes).  Each of these\n" +
    "  // fields are specified with command line parameters\n" +
    "  int numNodes; //The total number of nodes in the cluster\n" +
    "  int nodeId; //Cardinal number of node within cluster\n" +
    "  string prefix; //Common prefix to all nodes in cluster\n" +
    "\n" +
    "  // Ports for communications are allocated over a contiguous \n" +
    "  // block of numbers, starting at this number\n" +
    "  size_t startingPort;\n" + 
    "\n" +
    "  // These two parameters relate to how the node receives data\n" +
    "  // Right now we can receive data in two ways, from a socket\n" +
    "  // and a CSV file.  These two are for reading data from a socket.\n" +
    "  // This is an area where SAM can be matured down the road.\n" +
    "  string ncIp; // The ip to read the nc data from.\n" +
    "  size_t ncPort; // The port to read the nc data from.\n" +
    "\n" +
    "  // We use ZeroMQ Push/Pull sockets to communicate between\n" +
    "  // nodes.  We found that having multiple sockets to communicate\n" +
    "  // to a single node helped avoid contention.  numSockets\n" +
    "  // sets the total number of push and corresponding pull sockets\n" +
    "  // per node, so in total ((numNodes-1) * numSockets) sockets are.\n" +
    "  // defined per node.  It also helped to have multiple\n" +
    "  // threads to receive the data.  numPullThreads specifie\n" +
    "  // how many threads are used to cover all of the pull sockets\n" +
    "  // for a node.\n" +
    "  size_t numSockets;\n" +
    "  size_t numPullThreads;\n" +
    "\n" +
    "  // The timeWindow (s) controls how long intermediate subgraph results\n" +
    "  // are kept.  It should be a little longer than the longest\n" +
    "  // subgraph query\n"+
    "  double timeWindow;\n" +
    "  \n" +
    "  // A small set zeromq communications take forever.  The timeout\n" +
    "  // sets a hard limit on how long for push sockets to wait.\n" +
    "  // The value is in milliseconds\n" +
    "  size_t timeout;\n" +
    "\n" +
    "  // The high water mark is a parameter for zeromq communications\n" +
    "  // and it controls how many messages can buffer before being\n" +
    "  // dropped.  This can be set with a SAL preamble statement:\n" +
    "  // HighWaterMark = <int>;\n" +
    "  // This sets a default value in the code that can be overriden\n" +
    "  // with the command-line interface of this generated code.\n" +
    "  size_t hwm;\n" +
    "\n" +
    "  // There are two data structures to store the graph for edges\n" +
    "  // local to this node: a compressed sparse row (csr) and \n" +
    "  // compressed sparse column (csc).  A hash structure is used \n" +
    "  // to index the edges, either by the source vertex (csr) or the\n" +
    "  // target vertex (csc).  The array for the hash structure is\n" +
    "  // set once and not grown dynamically, though each slot can\n" +
    "  // grow arbitrarily large.  This should be set to a sufficiently\n" +
    "  // large value or much time will be spent linearly probing.\n" +
    "  size_t graphCapacity;\n" +
    "\n" +
    "  // Both the SubgraphQueryResultMap (where intermediate subgraphs\n" +
    "  // are stored) and the EdgeRequestMap (where edge requests from\n" +
    "  // other nodes are stored) use a hash structure with an array.\n" +
    "  // The array does not grow dynmically, but each slot can get\n" +
    "  // arbitrarily big.  This should be set to a sufficiently\n" +
    "  // large value or much time will be spent linearly probing.\n" +
    "  // In the future, may want to split these into two different\n" +
    "  // parameters.\n" +
    "  size_t tableCapacity;\n" +
    "\n"+
    "  // FeatureMap is another hashtable that needs a capacity\n" +
    "  // specified.  FeatureMap holds all the generated features.\n" +
    "  // FeatureSubscriber is used to create features.  Right now\n" +
    "  // it also needs a capacity specified, but in the future\n" +
    "  // this can likely be removed.  featureCapacity covers both\n" +
    "  // cases.\n" +
    "  size_t featureCapacity;\n" +
    "\n" +
    "  // For many of the vertex centric computations we buffer\n" +
    "  // the values and then execute a parallel for loop.\n" +
    "  // queueLength determines the length of the buffer.\n" +
    "  size_t queueLength;\n" +
    "\n" +
    "  // inputfile and outputfile are used when we are creating\n" +
    "  // features.  The inputfile has tuples with labels.\n" +
    "  // We then run the pipeline over the inputfile, create features\n" +
    "  // and write the results to the outputfile.\n" +
    "  string inputfile;\n" +
    "  string outputfile;\n" +
    "\n"
  }

  /**
   * Covers the generation of command line argument processing code.
   */
  private def commandLineProcessing = {

    logger.info("EntireQuery.commandLineProcessing")

    val hwm = memory.getOrElse(Constants.HighWaterMark, 10000); 
    val ql  = memory.getOrElse(Constants.QueueLength, 1000);
    val N   = memory.getOrElse(Constants.WindowSize, 10000);
    val b   = memory.getOrElse(Constants.BasicWindowSize, 1000);
    val k   = memory.getOrElse(Constants.TopKK, 2);

    "  /****************** Process commandline arguments ****************/\n"+
    "\n" +
    "  po::options_description desc(\n" +
    "    \"There are two basic modes supported right now: \"\n" +
    "    \"1) Running the pipeline against data coming from a socket.\\n\"\n"+
    "    \"2) Running the pipeline against an input file and creating\\n\"\n"+
    "    \" features.\\n\"\n"+
    "    \"These of course should be expanded.  Right now the process\\n\"\n"+
    "    \"allows for creating features on existing data to train\\n\"\n"+
    "    \"offline.  However, using the trained model on live data\\n\"\n"+
    "    \"is currently not supported\\n\"\n"+ 
    "    \"Allowed options:\");\n" +
    "  desc.add_options()\n" +
    "    (\"help\", \"help message\")\n" +
    "    (\"numNodes\",\n" +
    "      po::value<int>(&numNodes)->default_value(1),\n" +
    "      \"The number of nodes involved in the computation\")\n" +
    "    (\"nodeId\",\n" +
    "       po::value<int>(&nodeId)->default_value(0),\n" +
    "       \"The node id of this node.\")\n" +
    "    (\"prefix\",\n" +
    "      po::value<string>(&prefix)->default_value(\"node\"),\n" +
    "      \"The prefix common to all nodes.  The hostnames are formed\"\n" +
    "      \"by concatenating the prefix with the node id (in\"\n" +
    "      \"[0, numNodes-1]).  However, when there is only one node\"\n" +
    "      \"we use localhost.\")\n" +
    "    (\"startingPort\",\n" +
    "       po::value<size_t>(&startingPort)->default_value(10000),\n" +
    "       \"The starting port for the zeromq communications\")\n" +  
    "    (\"ncIp\",\n" +
    "      po::value<string>(&ncIp)->default_value(\"localhost\"),\n" +
    "      \"The ip to receive the data from nc (netcat).  Right now\"\n"+
    "      \" each node receives data from a socket connection.  \"\n" +
    "      \"This can be improved in the future.\")\n" +
    "    (\"ncPort\",\n" +
    "      po::value<size_t>(&ncPort)->default_value(9999),\n" +
    "      \"The port to receive the data from nc\")\n" +
    "    (\"numPullThreads\",\n" +
    "      po::value<size_t>(&numPullThreads)->default_value(1),\n" +
    "      \"Number of pull threads (default 1)\")\n" +
    "    (\"numSockets\",\n" +
    "      po::value<size_t>(&numSockets)->default_value(1),\n" +
    "      \"Number of push sockets a node creates to talk to another\"\n" +
    "      \"node (default 1)\")\n" +
    "    (\"timeWindow\",\n" +
    "      po::value<double>(&timeWindow)->default_value(10),\n" +
    "      \"How long in seconds to keep intermediate results around\")\n"+ 
    "    (\"timeout\",\n" +
    "      po::value<size_t>(&timeout)->default_value(1000),\n" +
    "      \"How long in milliseconds to wait before giving up on push\"\n"+
    "      \"socket send\")\n" + 
    "    (\"graphCapacity\",\n" +
    "      po::value<size_t>(&graphCapacity)->default_value(100000),\n" +
    "      \"How many slots in the csr and csc (default: 100000).\")\n" +
    "    (\"tableCapacity\",\n" +
    "      po::value<size_t>(&tableCapacity)->default_value(1000),\n" +
    "      \"How many slots in SubgraphQueryResultMap and EdgeRequestMap\"\n"+
    "      \" (default 1000).\")\n" +
    "    (\"featureCapacity\",\n" +
    "      po::value<size_t>(&featureCapacity)->default_value(10000),\n"+
    "      \"The capacity of the FeatureMap and FeatureSubcriber\")\n" +
    "    (\"hwm\",\n" +
    "      po::value<size_t>(&hwm)->default_value(" + hwm + "),\n" +
    "      \"The high water mark (how many items can queue up before we\"\n" +
    "      \"start dropping\")\n" +
    "    (\"queueLength\",\n" +
    "      po::value<size_t>(&queueLength)->default_value(" + ql + "),\n"+
    "      \"We fill a queue before sending things in parallel to all\"\n" +
    "      \" consumers.  This controls the size of that queue.\")\n" +
    "    (\"create_features\",\n" +
    "      \"If specified, will read tuples from --inputfile and\"\n" +
    "      \" output to --outputfile a csv feature file\")\n" +
    "    (\"inputfile\",\n" +
    "      po::value<string>(&inputfile),\n" +
    "      \"If --create_features is specified, the input should be\"\n" +
    "      \" a file with labeled tuples.\")\n" + 
    "    (\"outputfile\",\n" +
    "      po::value<string>(&outputfile),\n" +
    "      \"If --create_features is specified, the produced file will\"\n" +
    "      \" be a csv file of features.\")\n" + 
    "  ;\n" +
    "\n" +
    "  po::variables_map vm;\n" +
    "  po::store(po::parse_command_line(argc, argv, desc), vm);\n" +
    "  po::notify(vm);\n" +
    "  if (vm.count(\"help\")) {\n" +
    "    cout << desc << endl;\n" +
    "    return 1;\n" +
    "  }\n" +
    "\n"

  }

  /**
   * Sets the hostnames by taking a prefix and adding the numbers [0-numNodes).
   */
  private def setHostnamesAndPorts =
  {
    logger.info("EntireQuery.setHostnamesAndPorts")
    "  vector<string> hostnames(numNodes);\n" +
    "\n" +
    "  // When we are operating on one nodes, we set the hostname to be\n" +
    "  // local host or 127.0.0.1\n" +
    // TODO: Need to add local logic to command line.
    "  bool local = false;\n" +
    "  if (numNodes == 1) { \n" +
    "    hostnames[0] = \"127.0.0.1\";\n" +
    "    local = true;\n" +
    "  } else { \n" +
    "    for (int i = 0; i < numNodes; i++) {\n" +
    "      // Assumes all the host names can be composed by adding prefix\n" +
    "      // with [0,numNodes).\n" +
    "      hostnames[i] = prefix + boost::lexical_cast<string>(i);\n" +
    "    }\n" +
    "  }\n" +
    "\n"
  }

  private def featureMap =
  {
    logger.info("EntireQuery.featureMap")
    "  // The FeatureMap keeps track of all generated features produced\n" +
    "  // by the specified pipeline.\n" +
    "  auto featureMap = std::make_shared<FeatureMap>(featureCapacity);\n" +
    "\n"
  }

  private def createFeatures =
  {
    logger.info("EntireQuery.createFeatures")
    "  /***************** Creating Features ***********************/\n" + 
    "\n" +
    "  if(vm.count(\"create_features\"))\n" +
    "  {\n" +
    "\n" +
    "    if (inputfile == \"\") {\n" +
    "       std::cout << \"--create_features was specified but no input\"\n" +
    "                 << \"file was listed with --inputfile.\" << std::endl;\n"+
    "       return -1;\n" +
    "    }\n" +
    "    if (outputfile == \"\") {\n" +
    "      std::cout << \"--create_features was specified but no output\"\n" +
    "                << \"file was listed with --outputfile.\" << std::endl;\n"+
    "      return -1;\n" +
    "    }\n" +
    "\n" +
    "    auto receiver = std::make_shared<ReadCSVType>(inputfile);\n" +
    "    auto subscriber = std::make_shared<FeatureSubscriber>(outputfile,\n" +
    "                                                      featureCapacity);\n"+
    "\n" +
    "    // We create a pointer to a parent class of ReadCSV so that we can\n" +
    "    // use the same logic for both ReadCSV and partitioner in\n" +
    "    // createPipeline\n" +
    "    auto producer = std::static_pointer_cast<ProducerType>(receiver);\n"+
    "\n" +
    "    size_t resultsCapacity = 1000;\n" +
    "    createPipeline(producer,\n" +
    "              featureMap,\n" +
    "              subscriber,\n" +
    "              numNodes,\n" +
    "              nodeId,\n" +
    "              hostnames,\n" +
    "              startingPort,\n" +
    "              hwm,\n" +
    "              graphCapacity, tableCapacity, resultsCapacity,\n" +
    "              numSockets, numPullThreads, timeout, timeWindow,\n" +
    "              queueLength);\n" +
    "\n" +
    "    subscriber->init();\n" +
    "    if (!receiver->connect()) {\n" +
    "      std::cout << \"Problems opening file \" << inputfile << std::endl;\n" +
    "      return -1;\n" +
    "    }\n" +
    "    milliseconds ms1 = duration_cast<milliseconds>(\n" +
    "    system_clock::now().time_since_epoch()\n" +
    "      );\n" +
    "    receiver->receive();\n" +
    "    milliseconds ms2 = duration_cast<milliseconds>(\n" +
    "      system_clock::now().time_since_epoch()\n" +
    "    );\n" +
    "    std::cout << \"Seconds for Node\" << nodeId << \": \"\n" +
    "              << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;\n" +
    "    std::cout << \"Finished\" << std::endl;\n" +
    "    return 0;\n" +
    "  }\n" 
  }


  private def runPipeline = 
  {
    logger.info("EntireQuery.runPipeline")
    "  else\n" +
    "  {\n" +
    "    auto receiver = std::make_shared<ReadSocket>(ncIp, ncPort);\n" +
    "\n" +
    "    auto partitioner = std::make_shared<PartitionType>(\n" +
    "                                           queueLength,\n" +
    "                                           numNodes,\n" +
    "                                           nodeId,\n" +
    "                                           hostnames,\n" +
    "                                           startingPort,\n" +
    "                                           timeout,\n" +
    "                                           local,\n" +
    "                                           hwm);\n" +
    "\n" +
    "    receiver->registerConsumer(partitioner);\n" +
    "\n" +
    "    // We create a pointer to a parent class of partitioner so that we\n"+
    "    // can use the same logic for both ReadCSV and ZeroMQPushPull\n" +
    "    // in createPipeline.\n"+
    "    auto producer =std::static_pointer_cast<ProducerType>(partitioner);\n"+
    "\n" +
    "    size_t resultsCapacity = 1000;\n" +
    "    createPipeline(producer,\n"+
    "              featureMap,\n"+
    "              NULL,\n" +
    "              numNodes,\n" +
    "              nodeId,\n" +
    "              hostnames,\n" +
    "              startingPort,\n" +
    "              hwm,\n" +
    "              graphCapacity, tableCapacity, resultsCapacity,\n" +
    "              numSockets, numPullThreads, timeout, timeWindow,\n" +
    "              queueLength);\n" +
    "\n" +
    "    if (!receiver->connect()) {\n" +
    "      std::cout << \"Couldn't connected to \" << ncIp << \":\" << ncPort << std::endl;\n" +
    "      return -1;\n" +
    "    }\n" +
    "    milliseconds ms1 = duration_cast<milliseconds>(\n" +
    "      system_clock::now().time_since_epoch()\n" +
    "    );\n" +
    "    receiver->receive();\n" +
    "    milliseconds ms2 = duration_cast<milliseconds>(\n" +
    "      system_clock::now().time_since_epoch()\n" +
    "    );\n" +
    "    std::cout << \"Seconds for Node\" << nodeId << \": \"\n" +
    "      << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;\n" +
    "  }\n"; 
  }

  private def main =   
  {
    logger.info("EntireQuery.main")  
    val rString = "int main(int argc, char** argv) {\n" +
      "\n" +
      mainVariables +
      commandLineProcessing +
      setHostnamesAndPorts +
      featureMap +
      createFeatures +
      runPipeline 
    
    rString
  }
  
  private def closing = {
    logger.info("EntireQuery.closing")
    var rString = "\n}"
    rString
  }
}


