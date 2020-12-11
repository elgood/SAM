# Parser

## Running

Running with sbt, the build tool for scala:

1. cd Parser

   Go into the root directory of the Parser project.  

2. sbt

   This may take a while if running for the first time because it will download the dependencies. At the end you should be presented with a prompt: SAL-Parser.

3. test

  This runs the tests that have been defined. 

4. run input [output]

   This will run the sal parser and create the c++ code.  *input* is a path to the SAL program.  *output*, which is optional, is where the generated code will go.  If not specified, the generated code will print to standard out. 

5. exit

   When done type exit.

## Requirements

To run using sbt, it must be installed.  sbt requires Java 1.8 or later.

## Extending the Parser for SAM

To extend the parser for additional SAM functionality, below we outline common use cases.

* Additional Tuple types: 
  * Edit src/main/scala/sal/parsing/sam/TupleTypes.scala and add the SAM data structures for the tuple type, the tuplizer, the connection statement, and the tuple's namespace.  For example, looking at the definition for NetflowV5.
  ```scala
  val NetflowV5           = "NetflowV5"
  val NetflowV5Tuplizer   = "MakeNetflowV5"
  val NetflowV5Connect    = "NetflowV5Stream"
  val NetflowV5Namespace  = "sam::netflowv5"
  ```
  * Add ConnectionStatement[NameTuple].scala to src/main/scala/sal/parsing/sam/statements.  This should have a parser for the new connection statement (e.g. *NetflowV5*).  There should be a method defined of the form 
  ```scala
  def connectionStatementNameTuple: Parser[ConnectionStatementNameTuple] = ...
  ```

  * Add the parser method to the list of connections in src/main/scala/sal/parsing/sam/statemetns/Connections.scala. 
  ```scala
  def connectionStatement = connectionStatementVast | connectionStatementNameTuple
  ```

* Additional operators:
  * Edit src/main/scala/sal/parsing/sam/Operator.scala.  
    * Add the operator to the list of operators under "def operator = ...".  
    * Add a corresponding Parser for the operator.
  * Edit sal/parsing/sam/OperatorExp.scala and add a class that extends OperatorExp.  The class will need to override createOpString.
