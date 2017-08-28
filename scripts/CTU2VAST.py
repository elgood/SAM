"""
  Converts data from CTU to VAST.  CTU uses Argus, so the ra.conf file
  should be used to get the netflow data.  The ra.conf file specifies the
  format of how each netflow should look like.  The format is

  label, //The label (good or bad)
  stime, //Time stamp
  protocol, 
  source address, //Source IP address
  dest address,
  source port,
  dest port,
  duration,
  source app bytes,
  dest app bytes,
  source bytes,
  dest byes,
  source packets,
  dest packets

  We convert the above into the VAST format, which is:
  Label
  TimeSeconds
  ParseDate
  DateTime
  IpLayerProtocol
  IpLayerProtocolCode
  SourceIp
  DestIp
  SourcePort
  DestPort
  MoreFragments   
  CountFragments
  DurationSeconds
  SrcPayloadBytes
  DestPayloadBytes
  SrcTotalBytes
  DestTotalBytes
  FirstSeenSrcPacketCount
  FirstSeenDestPacketCount
  RecordForceOut

"""

BOTNET_LABEL = "Botnet"

LABEL     = 0
TIMESTAMP = 1
PROTOCOL  = 2
SRC_IP    = 3
DEST_IP   = 4 
SRC_PORT  = 5
DEST_PORT = 6
DURATION  = 7
SAPPBYTES = 8
DAPPBYTES = 9
SBYTES    = 10
DBYTES    = 11
SPKTS     = 12
DPKTS     = 13

# Started this because argus was spitting out names, but I think I figured
# out how to have argus just spit out the port numbers instead of names.
portmappings = {"hypercube-lm": "1577",
                "http" : "80",
                "sun-as-nodeagt" : "4850",
                "timbuktu-srv3" : "1419",
                "anynetgateway" : "1491",
                "ibm-abtact": "1586"}

import argparse
import time
import datetime

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--inputfile', type=str,
                      required=True,
                      help="The file to convert")
  parser.add_argument('--outputfile', type=str,
                      required=True,
                      help="The output file after conversion")
  FLAGS = parser.parse_args()

  with open(FLAGS.inputfile, "r") as infile:
    with open(FLAGS.outputfile, "w") as outfile:
      infile.readline() # Skip first line which is the column info

      for line in infile:
        line = line.strip()
        splitline = line.split(",")

        # Create the newline
        newline = ""
        
        ### Label ###
        # We convert the label to be 0 or 1 where 0 is benign and 1 is 
        # malicious.  The CTU data has multiple labels.  The label with
        # "Botnet" in it are the bad netflows.
        if BOTNET_LABEL in splitline[LABEL]:
          newline = newline + "1,"
        else:
          newline = newline + "0,"
        
        ### TimeSeconds ###
        stringtime = splitline[TIMESTAMP]
        datetime_object = datetime.datetime.strptime(stringtime, 
                                                     "%Y-%m-%d %H:%M:%S.%f")
        timestamp = datetime_object.timestamp()
        newline = newline + str(timestamp) + ","

        ### ParseDate ###
        newline = newline + stringtime.split(".")[0] + ","

        ### DateTime ###
        # Incomplete.  Not a match to VAST but too lazy to finish because
        # we don't use it.
        newline = (newline + str(datetime_object.year) + 
                            str(datetime_object.month) +
                            str(datetime_object.day) +
                            str(datetime_object.hour) +
                            str(datetime_object.minute) +
                            str(datetime_object.second) + ",")

      
        ### IpLayerProtocol ###
        newline = newline + splitline[PROTOCOL] + ","

        ### IpLayerProtocolCode ###
        protocol = splitline[PROTOCOL]
        if protocol == "icmp":
          newline = newline + "1,"
        elif protocol == "tcp":
          newline = newline + "6,"
        elif protocol == "udp":
          newline = newline + "17,"
        elif protocol == "rtp":
          newline = newline + "28,"
        elif protocol == "arp":
          newline = newline + "54,"
        elif protocol == "pim":
          newline = newline + "103,"
        elif protocol == "ipx/spx":
          newline = newline + "111,"
        else:
          #TODO: Finish
          newline = newline + "255,"
          #raise Exception("No mapping for protocol " + protocol)

        ### SourceIp ###
        newline = newline + splitline[SRC_IP] + "," 
    
        ### DestIp ###
        newline = newline + splitline[DEST_IP] + "," 
   
        ### SourcePort ###
        source_port = splitline[SRC_PORT]
        if source_port.isdigit():
          newline = newline + source_port + "," 
        elif len(source_port) == 0:
          newline = newline + "100000,"
        elif source_port[0:2] == "0x":
          newline = newline + str(int(source_port, 16)) + ","
        #elif source_port in portmappings:
        #  newline = newline + portmappings[source_port] + ","
        else:
          raise Exception("No mapping for port " + source_port)

        ### DestPort ###
        # For some reason ports aren't always ints
        dest_port = splitline[DEST_PORT]
        if dest_port.isdigit():
          newline = newline + dest_port + "," 
        elif len(dest_port) == 0:
          newline = newline + "100000,"
        elif dest_port[0:2] == "0x":
          newline = newline + str(int(dest_port, 16)) + ","
        #elif dest_port in portmappings:
        #  newline = newline + portmappings[dest_port] + ","
        else:
          raise Exception("No mapping for port " + dest_port)


        ### MoreFragments ###
        newline = newline + "0,"  
    
        ### CountFragments ###
        newline = newline + "0,"
        
        ### DurationSeconds ###
        newline = newline + splitline[DURATION] + ","

        ### SrcPayloadBytes ###
        newline = newline + splitline[SAPPBYTES] + ","
    
        ### DestPayloadBytes ###
        newline = newline + splitline[DAPPBYTES] + ","

        ### SrcTotalBytes ###
        newline = newline + splitline[SBYTES] + ","
    
        ### DestTotalBytes ###
        newline = newline + splitline[DBYTES] + ","

        ### FirstSeenSrcPacketCount ###
        newline = newline + splitline[SPKTS] + ","

        ### FirstSeenDestPacketCount ###
        newline = newline + splitline[DPKTS] + ","

        ### RecordForceOut ###
        newline = newline + "0\n"

        outfile.write(newline)
      outfile.close()
    infile.close()
 
main()

