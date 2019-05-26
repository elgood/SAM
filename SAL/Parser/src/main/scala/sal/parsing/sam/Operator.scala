package sal.parsing.sam

/**
 *
 */
trait Operator extends BaseParsing {

  def operator = topKOperator |
                 ehSumOperator | sumOperator |
                 ehVarOperator | varOperator |
                 ehAveOperator | aveOperator |
                 simpleSumOperator

  def topKOperator : Parser[TopKExp] =
    topKKeyWord ~ "(" ~ identifier ~ "," ~
      posInt ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
      {case topk ~ lpar ~ id ~ c1 ~ n ~ c2 ~ b ~ c3 ~ k ~ rpar =>
        TopKExp(id, n, b, k, memory)}

  def ehSumOperator : Parser[EHSumExp] =
    ehSumKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehsum ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHSumExp(id, n, k, memory)}

  def sumOperator : Parser[EHSumExp] =
    sumKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehsum ~ lpar ~ id  ~ rpar =>
      EHSumExp(id, 10000, 2, memory)}

  def ehAveOperator : Parser[EHAveExp] =
    ehAveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)}

  def aveOperator : Parser[EHAveExp] =
    aveKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehave ~ lpar ~ id ~ rpar =>


      EHAveExp(id, 10000, 2, memory)} |
    aveKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehave ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHAveExp(id, n, k, memory)}


  def ehVarOperator : Parser[EHVarExp] =
    ehVarKeyWord ~ "(" ~ identifier ~ "," ~ posInt ~ "," ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ c2 ~ k ~ rpar =>
      EHVarExp(id, n, k, memory)}

  def varOperator : Parser[EHVarExp] =
    varKeyWord ~ "(" ~ identifier ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ rpar =>
      EHVarExp(id, 10000, 2, memory)}

  def simpleSumOperator : Parser[SimpleSumExp] =
    simpleSumKeyWord ~ "(" ~ identifier ~ ","  ~ posInt ~ ")" ^^
    {case ehvar ~ lpar ~ id ~ c1 ~ n ~ rpar =>
      SimpleSumExp(id, n, memory)}


}
