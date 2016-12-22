/*
 * TestNCSpeed.cpp
 *
 *  Created on: Nov 12, 2016
 *      Author: elgood
 */

#include <iostream>
#include <string>
#include <boost/program_options.hpp>
#include <time.h>

#include "ReadSocket.h"


namespace po = boost::program_options;
using std::string;

int main(int argc, char* argv[])
{
	string ip;
	int port;
	int n;
	time_t timestamp_sec1, timestamp_sec2;

	po::options_description desc("Allowed options");
	desc.add_options()
		("help","help message")
		("ip", po::value<string>(&ip)->default_value("localhost"), "The ip to receive data")
		("port", po::value<int>(&port)->default_value(9999), "The port to receive data")
	;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 1;
	}

	ReadSocket socket(ip, port);
	if (!socket.connect()) {
		std::cout << "Couldn't connect to " << ip << ":" << port << std::endl;
		return 1;
	}

	time(&timestamp_sec1);
	string line = "";
	int count = 0;
	do {
		line = socket.readline();
		if (line != "") {
			count++;
		}
		//std::cout << line << std::endl;
		//if (count % 100000 == 0) std::cout << "Count " << count << std::endl;
	} while (line != "");
	time(&timestamp_sec2);
	std::cout << "Seconds " << timestamp_sec2 - timestamp_sec1 << std::endl;

	std::cout << "count: " << count << std::endl;
	return 0;
}



