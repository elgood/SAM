/**
 * This reads a list of netflows from a file and finds all the triangles.
 * This is used as a check on what is found using the SAM parallel 
 * infrastructure.
 */

//#define DEBUG
#define DETAIL_TIMING
#include <fstream>
#include <boost/program_options.hpp>
#include "Util.hpp"
#include "Netflow.hpp"


namespace po = boost::program_options;
using namespace sam;

int main(int argc, char** argv) {

  double queryTimeWindow; ///> Amount of time within a triangle can occur.
  std::string infile; ///> The location of the netflows

  po::options_description desc("Reads netflows from a file and counts "
    "how many triangles");
  desc.add_options()
    ("help", "help message")
    ("queryTimeWindow",
      po::value<double>(&queryTimeWindow)->default_value(10),
      "Time window for the query to be satisfied (default: 10).")
    ("infile", po::value<std::string>(&infile),
      "The file with the netflows.")
  ;

  // Parse the command line variables
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  // Print out the help and exit if --help was specified.
  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  std::vector<Netflow> netflows;

  std::ifstream netflowFile;
  netflowFile.open(infile);

  std::string line;
  size_t i = 0;
  while (std::getline(netflowFile, line))
  {
    Netflow netflow = makeNetflow(i, line);
    netflows.push_back(netflow);
    i++;
  }

  size_t numTri = sam::numTriangles<Netflow, SourceIp, DestIp,
                    TimeSeconds, DurationSeconds>(netflows, queryTimeWindow);

  std::cout << "Number of triangles " << numTri << std::endl;

}
