package sal.parsing.sam

import scala.io.Source
import java.io.PrintWriter
import java.io.File

object Sal extends Parsing
{
  val usage = """
    Usage: sal input [output]
  """
  def main(args: Array[String])
  {
    if (args.length < 1 || args.length > 2) { 
      println(usage)
      System.exit(1)
    }

    val infile = args(0)

    val fileContents = Source.fromFile(infile).getLines.mkString
 
    parse(document, fileContents) match
    {
      case Success(matched,_) => 
        if (args.length > 1) 
        {
          val outfile = args(1)
          val pw = new PrintWriter(new File(outfile))
          pw.write(matched.toString)
          pw.close

        } else {
          println(matched)
        }
      case Failure(msg,_) => println("FAILURE: " + msg)
      case Error(msg,_) => println("ERROR: " + msg)
    }
  }
}
