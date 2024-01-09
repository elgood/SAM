'''
TODO
'''
#!/usr/bin/env python

import geni.portal as portal
import geni.rspec.pg as RSpec
import geni.rspec.igext as IG
from lxml import etree as ET
import crypt
import random

pc = portal.Context()

pc.defineParameter("n", "The number of nodes", 
                                portal.ParameterType.INTEGER, 1)

pc.defineParameter("hardware_type", "Optional physical node type",
                   portal.ParameterType.STRING, "")

pc.defineParameter("link_best_effort", "If set to true, will accept less than 10G links",
                   portal.ParameterType.BOOLEAN, False)

pc.defineParameter("disk_image", "Disk image to use.",
                  portal.ParameterType.STRING, 
                  "urn:publicid:IDN+wisc.cloudlab.us+image+streaminggraphs-PG0:SAM_v3")

# Retreive the values the user specifies during instantiation
params = portal.context.bindParameters()



# Create a Request object to start building the RSpec.
request = portal.context.makeRequestRSpec()

if params.n < 1 or params.n > 128:
    pc.reportError( portal.ParameterError ("You must choose 1-128 nodes"))


link = request.LAN("lan")

if params.link_best_effort == True:
    link.best_effort = True

for i in range( 0, params.n ):
    node = request.RawPC( "node" + str(i) )
    node.disk_image = params.disk_image
    #node.disk_image = "urn:publicid:IDN+wisc.cloudlab.us+image+streaminggraphs-PG0:SAM"
    if params.hardware_type != "":
        node.hardware_type = params.hardware_type
        
    iface = node.addInterface("if0")
    iface.addAddress(RSpec.IPv4Address("192.168.1." + str(i+1), "255.255.255.0"))
    link.addInterface(iface)

    node.addService(RSpec.Execute(shell="sh", command="sudo chmod +x /local/repository/initpw.sh"))
    node.addService(RSpec.Execute(shell="sh", command="sudo /local/repository/initpw.sh"))
    




pc.printRequestRSpec()
