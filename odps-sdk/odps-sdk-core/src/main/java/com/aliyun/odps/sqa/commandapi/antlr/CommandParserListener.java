package com.aliyun.odps.sqa.commandapi.antlr;// Generated from CommandParser.g4 by ANTLR 4.9.2

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link CommandParser}.
 */
public interface CommandParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link CommandParser#command}.
	 * @param ctx the parse tree
	 */
	void enterCommand(CommandParser.CommandContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#command}.
	 * @param ctx the parse tree
	 */
	void exitCommand(CommandParser.CommandContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(CommandParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(CommandParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#whoamiStatement}.
	 * @param ctx the parse tree
	 */
	void enterWhoamiStatement(CommandParser.WhoamiStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#whoamiStatement}.
	 * @param ctx the parse tree
	 */
	void exitWhoamiStatement(CommandParser.WhoamiStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#sqlCostStatement}.
	 * @param ctx the parse tree
	 */
	void enterSqlCostStatement(CommandParser.SqlCostStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#sqlCostStatement}.
	 * @param ctx the parse tree
	 */
	void exitSqlCostStatement(CommandParser.SqlCostStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#compoundStatement}.
	 * @param ctx the parse tree
	 */
	void enterCompoundStatement(CommandParser.CompoundStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#compoundStatement}.
	 * @param ctx the parse tree
	 */
	void exitCompoundStatement(CommandParser.CompoundStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#cteStatement}.
	 * @param ctx the parse tree
	 */
	void enterCteStatement(CommandParser.CteStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#cteStatement}.
	 * @param ctx the parse tree
	 */
	void exitCteStatement(CommandParser.CteStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableAliasWithCols}.
	 * @param ctx the parse tree
	 */
	void enterTableAliasWithCols(CommandParser.TableAliasWithColsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableAliasWithCols}.
	 * @param ctx the parse tree
	 */
	void exitTableAliasWithCols(CommandParser.TableAliasWithColsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#subQuerySource}.
	 * @param ctx the parse tree
	 */
	void enterSubQuerySource(CommandParser.SubQuerySourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#subQuerySource}.
	 * @param ctx the parse tree
	 */
	void exitSubQuerySource(CommandParser.SubQuerySourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#functionParameters}.
	 * @param ctx the parse tree
	 */
	void enterFunctionParameters(CommandParser.FunctionParametersContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#functionParameters}.
	 * @param ctx the parse tree
	 */
	void exitFunctionParameters(CommandParser.FunctionParametersContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#parameterDefinition}.
	 * @param ctx the parse tree
	 */
	void enterParameterDefinition(CommandParser.ParameterDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#parameterDefinition}.
	 * @param ctx the parse tree
	 */
	void exitParameterDefinition(CommandParser.ParameterDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#parameterTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterParameterTypeDeclaration(CommandParser.ParameterTypeDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#parameterTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitParameterTypeDeclaration(CommandParser.ParameterTypeDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#functionTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTypeDeclaration(CommandParser.FunctionTypeDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#functionTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTypeDeclaration(CommandParser.FunctionTypeDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#parameterTypeDeclarationList}.
	 * @param ctx the parse tree
	 */
	void enterParameterTypeDeclarationList(CommandParser.ParameterTypeDeclarationListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#parameterTypeDeclarationList}.
	 * @param ctx the parse tree
	 */
	void exitParameterTypeDeclarationList(CommandParser.ParameterTypeDeclarationListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#parameterColumnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void enterParameterColumnNameTypeList(CommandParser.ParameterColumnNameTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#parameterColumnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void exitParameterColumnNameTypeList(CommandParser.ParameterColumnNameTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#parameterColumnNameType}.
	 * @param ctx the parse tree
	 */
	void enterParameterColumnNameType(CommandParser.ParameterColumnNameTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#parameterColumnNameType}.
	 * @param ctx the parse tree
	 */
	void exitParameterColumnNameType(CommandParser.ParameterColumnNameTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#varSizeParam}.
	 * @param ctx the parse tree
	 */
	void enterVarSizeParam(CommandParser.VarSizeParamContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#varSizeParam}.
	 * @param ctx the parse tree
	 */
	void exitVarSizeParam(CommandParser.VarSizeParamContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#preSelectClauses}.
	 * @param ctx the parse tree
	 */
	void enterPreSelectClauses(CommandParser.PreSelectClausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#preSelectClauses}.
	 * @param ctx the parse tree
	 */
	void exitPreSelectClauses(CommandParser.PreSelectClausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#postSelectClauses}.
	 * @param ctx the parse tree
	 */
	void enterPostSelectClauses(CommandParser.PostSelectClausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#postSelectClauses}.
	 * @param ctx the parse tree
	 */
	void exitPostSelectClauses(CommandParser.PostSelectClausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectRest}.
	 * @param ctx the parse tree
	 */
	void enterSelectRest(CommandParser.SelectRestContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectRest}.
	 * @param ctx the parse tree
	 */
	void exitSelectRest(CommandParser.SelectRestContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#multiInsertFromRest}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertFromRest(CommandParser.MultiInsertFromRestContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#multiInsertFromRest}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertFromRest(CommandParser.MultiInsertFromRestContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#fromRest}.
	 * @param ctx the parse tree
	 */
	void enterFromRest(CommandParser.FromRestContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#fromRest}.
	 * @param ctx the parse tree
	 */
	void exitFromRest(CommandParser.FromRestContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#simpleQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleQueryExpression(CommandParser.SimpleQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#simpleQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleQueryExpression(CommandParser.SimpleQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSelectQueryExpression(CommandParser.SelectQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSelectQueryExpression(CommandParser.SelectQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#fromQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFromQueryExpression(CommandParser.FromQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#fromQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFromQueryExpression(CommandParser.FromQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#setOperationFactor}.
	 * @param ctx the parse tree
	 */
	void enterSetOperationFactor(CommandParser.SetOperationFactorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#setOperationFactor}.
	 * @param ctx the parse tree
	 */
	void exitSetOperationFactor(CommandParser.SetOperationFactorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#queryExpression}.
	 * @param ctx the parse tree
	 */
	void enterQueryExpression(CommandParser.QueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#queryExpression}.
	 * @param ctx the parse tree
	 */
	void exitQueryExpression(CommandParser.QueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#queryExpressionWithCTE}.
	 * @param ctx the parse tree
	 */
	void enterQueryExpressionWithCTE(CommandParser.QueryExpressionWithCTEContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#queryExpressionWithCTE}.
	 * @param ctx the parse tree
	 */
	void exitQueryExpressionWithCTE(CommandParser.QueryExpressionWithCTEContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#setRHS}.
	 * @param ctx the parse tree
	 */
	void enterSetRHS(CommandParser.SetRHSContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#setRHS}.
	 * @param ctx the parse tree
	 */
	void exitSetRHS(CommandParser.SetRHSContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#multiInsertSetOperationFactor}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertSetOperationFactor(CommandParser.MultiInsertSetOperationFactorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#multiInsertSetOperationFactor}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertSetOperationFactor(CommandParser.MultiInsertSetOperationFactorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#multiInsertSelect}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertSelect(CommandParser.MultiInsertSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#multiInsertSelect}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertSelect(CommandParser.MultiInsertSelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#multiInsertSetRHS}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertSetRHS(CommandParser.MultiInsertSetRHSContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#multiInsertSetRHS}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertSetRHS(CommandParser.MultiInsertSetRHSContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#subQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubQueryExpression(CommandParser.SubQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#subQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubQueryExpression(CommandParser.SubQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void enterLimitClause(CommandParser.LimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void exitLimitClause(CommandParser.LimitClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#fromSource}.
	 * @param ctx the parse tree
	 */
	void enterFromSource(CommandParser.FromSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#fromSource}.
	 * @param ctx the parse tree
	 */
	void exitFromSource(CommandParser.FromSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableVariableSource}.
	 * @param ctx the parse tree
	 */
	void enterTableVariableSource(CommandParser.TableVariableSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableVariableSource}.
	 * @param ctx the parse tree
	 */
	void exitTableVariableSource(CommandParser.TableVariableSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void enterTableFunctionSource(CommandParser.TableFunctionSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void exitTableFunctionSource(CommandParser.TableFunctionSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#variableName}.
	 * @param ctx the parse tree
	 */
	void enterVariableName(CommandParser.VariableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#variableName}.
	 * @param ctx the parse tree
	 */
	void exitVariableName(CommandParser.VariableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#atomExpression}.
	 * @param ctx the parse tree
	 */
	void enterAtomExpression(CommandParser.AtomExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#atomExpression}.
	 * @param ctx the parse tree
	 */
	void exitAtomExpression(CommandParser.AtomExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#variableRef}.
	 * @param ctx the parse tree
	 */
	void enterVariableRef(CommandParser.VariableRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#variableRef}.
	 * @param ctx the parse tree
	 */
	void exitVariableRef(CommandParser.VariableRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#variableCall}.
	 * @param ctx the parse tree
	 */
	void enterVariableCall(CommandParser.VariableCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#variableCall}.
	 * @param ctx the parse tree
	 */
	void exitVariableCall(CommandParser.VariableCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#funNameRef}.
	 * @param ctx the parse tree
	 */
	void enterFunNameRef(CommandParser.FunNameRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#funNameRef}.
	 * @param ctx the parse tree
	 */
	void exitFunNameRef(CommandParser.FunNameRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#lambdaExpression}.
	 * @param ctx the parse tree
	 */
	void enterLambdaExpression(CommandParser.LambdaExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#lambdaExpression}.
	 * @param ctx the parse tree
	 */
	void exitLambdaExpression(CommandParser.LambdaExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#lambdaParameter}.
	 * @param ctx the parse tree
	 */
	void enterLambdaParameter(CommandParser.LambdaParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#lambdaParameter}.
	 * @param ctx the parse tree
	 */
	void exitLambdaParameter(CommandParser.LambdaParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableOrColumnRef}.
	 * @param ctx the parse tree
	 */
	void enterTableOrColumnRef(CommandParser.TableOrColumnRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableOrColumnRef}.
	 * @param ctx the parse tree
	 */
	void exitTableOrColumnRef(CommandParser.TableOrColumnRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#newExpression}.
	 * @param ctx the parse tree
	 */
	void enterNewExpression(CommandParser.NewExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#newExpression}.
	 * @param ctx the parse tree
	 */
	void exitNewExpression(CommandParser.NewExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#existsExpression}.
	 * @param ctx the parse tree
	 */
	void enterExistsExpression(CommandParser.ExistsExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#existsExpression}.
	 * @param ctx the parse tree
	 */
	void exitExistsExpression(CommandParser.ExistsExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#scalarSubQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterScalarSubQueryExpression(CommandParser.ScalarSubQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#scalarSubQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitScalarSubQueryExpression(CommandParser.ScalarSubQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#classNameWithPackage}.
	 * @param ctx the parse tree
	 */
	void enterClassNameWithPackage(CommandParser.ClassNameWithPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#classNameWithPackage}.
	 * @param ctx the parse tree
	 */
	void exitClassNameWithPackage(CommandParser.ClassNameWithPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#classNameOrArrayDecl}.
	 * @param ctx the parse tree
	 */
	void enterClassNameOrArrayDecl(CommandParser.ClassNameOrArrayDeclContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#classNameOrArrayDecl}.
	 * @param ctx the parse tree
	 */
	void exitClassNameOrArrayDecl(CommandParser.ClassNameOrArrayDeclContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#classNameList}.
	 * @param ctx the parse tree
	 */
	void enterClassNameList(CommandParser.ClassNameListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#classNameList}.
	 * @param ctx the parse tree
	 */
	void exitClassNameList(CommandParser.ClassNameListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#odpsqlNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterOdpsqlNonReserved(CommandParser.OdpsqlNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#odpsqlNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitOdpsqlNonReserved(CommandParser.OdpsqlNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#relaxedKeywords}.
	 * @param ctx the parse tree
	 */
	void enterRelaxedKeywords(CommandParser.RelaxedKeywordsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#relaxedKeywords}.
	 * @param ctx the parse tree
	 */
	void exitRelaxedKeywords(CommandParser.RelaxedKeywordsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(CommandParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(CommandParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#aliasIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterAliasIdentifier(CommandParser.AliasIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#aliasIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitAliasIdentifier(CommandParser.AliasIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#identifierWithoutSql11}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierWithoutSql11(CommandParser.IdentifierWithoutSql11Context ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#identifierWithoutSql11}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierWithoutSql11(CommandParser.IdentifierWithoutSql11Context ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#anythingButSemi}.
	 * @param ctx the parse tree
	 */
	void enterAnythingButSemi(CommandParser.AnythingButSemiContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#anythingButSemi}.
	 * @param ctx the parse tree
	 */
	void exitAnythingButSemi(CommandParser.AnythingButSemiContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#memberAccessOperator}.
	 * @param ctx the parse tree
	 */
	void enterMemberAccessOperator(CommandParser.MemberAccessOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#memberAccessOperator}.
	 * @param ctx the parse tree
	 */
	void exitMemberAccessOperator(CommandParser.MemberAccessOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#methodAccessOperator}.
	 * @param ctx the parse tree
	 */
	void enterMethodAccessOperator(CommandParser.MethodAccessOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#methodAccessOperator}.
	 * @param ctx the parse tree
	 */
	void exitMethodAccessOperator(CommandParser.MethodAccessOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#isNullOperator}.
	 * @param ctx the parse tree
	 */
	void enterIsNullOperator(CommandParser.IsNullOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#isNullOperator}.
	 * @param ctx the parse tree
	 */
	void exitIsNullOperator(CommandParser.IsNullOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#inOperator}.
	 * @param ctx the parse tree
	 */
	void enterInOperator(CommandParser.InOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#inOperator}.
	 * @param ctx the parse tree
	 */
	void exitInOperator(CommandParser.InOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#betweenOperator}.
	 * @param ctx the parse tree
	 */
	void enterBetweenOperator(CommandParser.BetweenOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#betweenOperator}.
	 * @param ctx the parse tree
	 */
	void exitBetweenOperator(CommandParser.BetweenOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#mathExpression}.
	 * @param ctx the parse tree
	 */
	void enterMathExpression(CommandParser.MathExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#mathExpression}.
	 * @param ctx the parse tree
	 */
	void exitMathExpression(CommandParser.MathExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#unarySuffixExpression}.
	 * @param ctx the parse tree
	 */
	void enterUnarySuffixExpression(CommandParser.UnarySuffixExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#unarySuffixExpression}.
	 * @param ctx the parse tree
	 */
	void exitUnarySuffixExpression(CommandParser.UnarySuffixExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#unaryPrefixExpression}.
	 * @param ctx the parse tree
	 */
	void enterUnaryPrefixExpression(CommandParser.UnaryPrefixExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#unaryPrefixExpression}.
	 * @param ctx the parse tree
	 */
	void exitUnaryPrefixExpression(CommandParser.UnaryPrefixExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#fieldExpression}.
	 * @param ctx the parse tree
	 */
	void enterFieldExpression(CommandParser.FieldExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#fieldExpression}.
	 * @param ctx the parse tree
	 */
	void exitFieldExpression(CommandParser.FieldExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalExpression(CommandParser.LogicalExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalExpression(CommandParser.LogicalExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#notExpression}.
	 * @param ctx the parse tree
	 */
	void enterNotExpression(CommandParser.NotExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#notExpression}.
	 * @param ctx the parse tree
	 */
	void exitNotExpression(CommandParser.NotExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#equalExpression}.
	 * @param ctx the parse tree
	 */
	void enterEqualExpression(CommandParser.EqualExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#equalExpression}.
	 * @param ctx the parse tree
	 */
	void exitEqualExpression(CommandParser.EqualExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#mathExpressionListInParentheses}.
	 * @param ctx the parse tree
	 */
	void enterMathExpressionListInParentheses(CommandParser.MathExpressionListInParenthesesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#mathExpressionListInParentheses}.
	 * @param ctx the parse tree
	 */
	void exitMathExpressionListInParentheses(CommandParser.MathExpressionListInParenthesesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#mathExpressionList}.
	 * @param ctx the parse tree
	 */
	void enterMathExpressionList(CommandParser.MathExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#mathExpressionList}.
	 * @param ctx the parse tree
	 */
	void exitMathExpressionList(CommandParser.MathExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(CommandParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(CommandParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#instanceId}.
	 * @param ctx the parse tree
	 */
	void enterInstanceId(CommandParser.InstanceIdContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#instanceId}.
	 * @param ctx the parse tree
	 */
	void exitInstanceId(CommandParser.InstanceIdContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#authorizationStatement}.
	 * @param ctx the parse tree
	 */
	void enterAuthorizationStatement(CommandParser.AuthorizationStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#authorizationStatement}.
	 * @param ctx the parse tree
	 */
	void exitAuthorizationStatement(CommandParser.AuthorizationStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#listUsers}.
	 * @param ctx the parse tree
	 */
	void enterListUsers(CommandParser.ListUsersContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#listUsers}.
	 * @param ctx the parse tree
	 */
	void exitListUsers(CommandParser.ListUsersContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#listGroups}.
	 * @param ctx the parse tree
	 */
	void enterListGroups(CommandParser.ListGroupsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#listGroups}.
	 * @param ctx the parse tree
	 */
	void exitListGroups(CommandParser.ListGroupsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#addUserStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddUserStatement(CommandParser.AddUserStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#addUserStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddUserStatement(CommandParser.AddUserStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#addGroupStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddGroupStatement(CommandParser.AddGroupStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#addGroupStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddGroupStatement(CommandParser.AddGroupStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#removeUserStatement}.
	 * @param ctx the parse tree
	 */
	void enterRemoveUserStatement(CommandParser.RemoveUserStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#removeUserStatement}.
	 * @param ctx the parse tree
	 */
	void exitRemoveUserStatement(CommandParser.RemoveUserStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#removeGroupStatement}.
	 * @param ctx the parse tree
	 */
	void enterRemoveGroupStatement(CommandParser.RemoveGroupStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#removeGroupStatement}.
	 * @param ctx the parse tree
	 */
	void exitRemoveGroupStatement(CommandParser.RemoveGroupStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#addAccountProvider}.
	 * @param ctx the parse tree
	 */
	void enterAddAccountProvider(CommandParser.AddAccountProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#addAccountProvider}.
	 * @param ctx the parse tree
	 */
	void exitAddAccountProvider(CommandParser.AddAccountProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#removeAccountProvider}.
	 * @param ctx the parse tree
	 */
	void enterRemoveAccountProvider(CommandParser.RemoveAccountProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#removeAccountProvider}.
	 * @param ctx the parse tree
	 */
	void exitRemoveAccountProvider(CommandParser.RemoveAccountProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showAcl}.
	 * @param ctx the parse tree
	 */
	void enterShowAcl(CommandParser.ShowAclContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showAcl}.
	 * @param ctx the parse tree
	 */
	void exitShowAcl(CommandParser.ShowAclContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#describeRole}.
	 * @param ctx the parse tree
	 */
	void enterDescribeRole(CommandParser.DescribeRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#describeRole}.
	 * @param ctx the parse tree
	 */
	void exitDescribeRole(CommandParser.DescribeRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#listRoles}.
	 * @param ctx the parse tree
	 */
	void enterListRoles(CommandParser.ListRolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#listRoles}.
	 * @param ctx the parse tree
	 */
	void exitListRoles(CommandParser.ListRolesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#listTrustedProjects}.
	 * @param ctx the parse tree
	 */
	void enterListTrustedProjects(CommandParser.ListTrustedProjectsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#listTrustedProjects}.
	 * @param ctx the parse tree
	 */
	void exitListTrustedProjects(CommandParser.ListTrustedProjectsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#addTrustedProject}.
	 * @param ctx the parse tree
	 */
	void enterAddTrustedProject(CommandParser.AddTrustedProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#addTrustedProject}.
	 * @param ctx the parse tree
	 */
	void exitAddTrustedProject(CommandParser.AddTrustedProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#removeTrustedProject}.
	 * @param ctx the parse tree
	 */
	void enterRemoveTrustedProject(CommandParser.RemoveTrustedProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#removeTrustedProject}.
	 * @param ctx the parse tree
	 */
	void exitRemoveTrustedProject(CommandParser.RemoveTrustedProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showSecurityConfiguration}.
	 * @param ctx the parse tree
	 */
	void enterShowSecurityConfiguration(CommandParser.ShowSecurityConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showSecurityConfiguration}.
	 * @param ctx the parse tree
	 */
	void exitShowSecurityConfiguration(CommandParser.ShowSecurityConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showPackages}.
	 * @param ctx the parse tree
	 */
	void enterShowPackages(CommandParser.ShowPackagesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showPackages}.
	 * @param ctx the parse tree
	 */
	void exitShowPackages(CommandParser.ShowPackagesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showItems}.
	 * @param ctx the parse tree
	 */
	void enterShowItems(CommandParser.ShowItemsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showItems}.
	 * @param ctx the parse tree
	 */
	void exitShowItems(CommandParser.ShowItemsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#installPackage}.
	 * @param ctx the parse tree
	 */
	void enterInstallPackage(CommandParser.InstallPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#installPackage}.
	 * @param ctx the parse tree
	 */
	void exitInstallPackage(CommandParser.InstallPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#uninstallPackage}.
	 * @param ctx the parse tree
	 */
	void enterUninstallPackage(CommandParser.UninstallPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#uninstallPackage}.
	 * @param ctx the parse tree
	 */
	void exitUninstallPackage(CommandParser.UninstallPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#createPackage}.
	 * @param ctx the parse tree
	 */
	void enterCreatePackage(CommandParser.CreatePackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#createPackage}.
	 * @param ctx the parse tree
	 */
	void exitCreatePackage(CommandParser.CreatePackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#deletePackage}.
	 * @param ctx the parse tree
	 */
	void enterDeletePackage(CommandParser.DeletePackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#deletePackage}.
	 * @param ctx the parse tree
	 */
	void exitDeletePackage(CommandParser.DeletePackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#addToPackage}.
	 * @param ctx the parse tree
	 */
	void enterAddToPackage(CommandParser.AddToPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#addToPackage}.
	 * @param ctx the parse tree
	 */
	void exitAddToPackage(CommandParser.AddToPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#removeFromPackage}.
	 * @param ctx the parse tree
	 */
	void enterRemoveFromPackage(CommandParser.RemoveFromPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#removeFromPackage}.
	 * @param ctx the parse tree
	 */
	void exitRemoveFromPackage(CommandParser.RemoveFromPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#allowPackage}.
	 * @param ctx the parse tree
	 */
	void enterAllowPackage(CommandParser.AllowPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#allowPackage}.
	 * @param ctx the parse tree
	 */
	void exitAllowPackage(CommandParser.AllowPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#disallowPackage}.
	 * @param ctx the parse tree
	 */
	void enterDisallowPackage(CommandParser.DisallowPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#disallowPackage}.
	 * @param ctx the parse tree
	 */
	void exitDisallowPackage(CommandParser.DisallowPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#putPolicy}.
	 * @param ctx the parse tree
	 */
	void enterPutPolicy(CommandParser.PutPolicyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#putPolicy}.
	 * @param ctx the parse tree
	 */
	void exitPutPolicy(CommandParser.PutPolicyContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#getPolicy}.
	 * @param ctx the parse tree
	 */
	void enterGetPolicy(CommandParser.GetPolicyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#getPolicy}.
	 * @param ctx the parse tree
	 */
	void exitGetPolicy(CommandParser.GetPolicyContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#clearExpiredGrants}.
	 * @param ctx the parse tree
	 */
	void enterClearExpiredGrants(CommandParser.ClearExpiredGrantsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#clearExpiredGrants}.
	 * @param ctx the parse tree
	 */
	void exitClearExpiredGrants(CommandParser.ClearExpiredGrantsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#grantLabel}.
	 * @param ctx the parse tree
	 */
	void enterGrantLabel(CommandParser.GrantLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#grantLabel}.
	 * @param ctx the parse tree
	 */
	void exitGrantLabel(CommandParser.GrantLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#revokeLabel}.
	 * @param ctx the parse tree
	 */
	void enterRevokeLabel(CommandParser.RevokeLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#revokeLabel}.
	 * @param ctx the parse tree
	 */
	void exitRevokeLabel(CommandParser.RevokeLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showLabel}.
	 * @param ctx the parse tree
	 */
	void enterShowLabel(CommandParser.ShowLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showLabel}.
	 * @param ctx the parse tree
	 */
	void exitShowLabel(CommandParser.ShowLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#grantSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void enterGrantSuperPrivilege(CommandParser.GrantSuperPrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#grantSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void exitGrantSuperPrivilege(CommandParser.GrantSuperPrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#revokeSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void enterRevokeSuperPrivilege(CommandParser.RevokeSuperPrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#revokeSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void exitRevokeSuperPrivilege(CommandParser.RevokeSuperPrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#createRoleStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateRoleStatement(CommandParser.CreateRoleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#createRoleStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateRoleStatement(CommandParser.CreateRoleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#dropRoleStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropRoleStatement(CommandParser.DropRoleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#dropRoleStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropRoleStatement(CommandParser.DropRoleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#addRoleToProject}.
	 * @param ctx the parse tree
	 */
	void enterAddRoleToProject(CommandParser.AddRoleToProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#addRoleToProject}.
	 * @param ctx the parse tree
	 */
	void exitAddRoleToProject(CommandParser.AddRoleToProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#removeRoleFromProject}.
	 * @param ctx the parse tree
	 */
	void enterRemoveRoleFromProject(CommandParser.RemoveRoleFromProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#removeRoleFromProject}.
	 * @param ctx the parse tree
	 */
	void exitRemoveRoleFromProject(CommandParser.RemoveRoleFromProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#grantRole}.
	 * @param ctx the parse tree
	 */
	void enterGrantRole(CommandParser.GrantRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#grantRole}.
	 * @param ctx the parse tree
	 */
	void exitGrantRole(CommandParser.GrantRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#revokeRole}.
	 * @param ctx the parse tree
	 */
	void enterRevokeRole(CommandParser.RevokeRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#revokeRole}.
	 * @param ctx the parse tree
	 */
	void exitRevokeRole(CommandParser.RevokeRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#grantPrivileges}.
	 * @param ctx the parse tree
	 */
	void enterGrantPrivileges(CommandParser.GrantPrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#grantPrivileges}.
	 * @param ctx the parse tree
	 */
	void exitGrantPrivileges(CommandParser.GrantPrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#privilegeProperties}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeProperties(CommandParser.PrivilegePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#privilegeProperties}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeProperties(CommandParser.PrivilegePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#revokePrivileges}.
	 * @param ctx the parse tree
	 */
	void enterRevokePrivileges(CommandParser.RevokePrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#revokePrivileges}.
	 * @param ctx the parse tree
	 */
	void exitRevokePrivileges(CommandParser.RevokePrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#purgePrivileges}.
	 * @param ctx the parse tree
	 */
	void enterPurgePrivileges(CommandParser.PurgePrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#purgePrivileges}.
	 * @param ctx the parse tree
	 */
	void exitPurgePrivileges(CommandParser.PurgePrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showGrants}.
	 * @param ctx the parse tree
	 */
	void enterShowGrants(CommandParser.ShowGrantsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showGrants}.
	 * @param ctx the parse tree
	 */
	void exitShowGrants(CommandParser.ShowGrantsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showRoleGrants}.
	 * @param ctx the parse tree
	 */
	void enterShowRoleGrants(CommandParser.ShowRoleGrantsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showRoleGrants}.
	 * @param ctx the parse tree
	 */
	void exitShowRoleGrants(CommandParser.ShowRoleGrantsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showRoles}.
	 * @param ctx the parse tree
	 */
	void enterShowRoles(CommandParser.ShowRolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showRoles}.
	 * @param ctx the parse tree
	 */
	void exitShowRoles(CommandParser.ShowRolesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showRolePrincipals}.
	 * @param ctx the parse tree
	 */
	void enterShowRolePrincipals(CommandParser.ShowRolePrincipalsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showRolePrincipals}.
	 * @param ctx the parse tree
	 */
	void exitShowRolePrincipals(CommandParser.ShowRolePrincipalsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#user}.
	 * @param ctx the parse tree
	 */
	void enterUser(CommandParser.UserContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#user}.
	 * @param ctx the parse tree
	 */
	void exitUser(CommandParser.UserContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#userRoleComments}.
	 * @param ctx the parse tree
	 */
	void enterUserRoleComments(CommandParser.UserRoleCommentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#userRoleComments}.
	 * @param ctx the parse tree
	 */
	void exitUserRoleComments(CommandParser.UserRoleCommentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#accountProvider}.
	 * @param ctx the parse tree
	 */
	void enterAccountProvider(CommandParser.AccountProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#accountProvider}.
	 * @param ctx the parse tree
	 */
	void exitAccountProvider(CommandParser.AccountProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#privilegeObjectName}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeObjectName(CommandParser.PrivilegeObjectNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#privilegeObjectName}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeObjectName(CommandParser.PrivilegeObjectNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#privilegeObjectType}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeObjectType(CommandParser.PrivilegeObjectTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#privilegeObjectType}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeObjectType(CommandParser.PrivilegeObjectTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#roleName}.
	 * @param ctx the parse tree
	 */
	void enterRoleName(CommandParser.RoleNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#roleName}.
	 * @param ctx the parse tree
	 */
	void exitRoleName(CommandParser.RoleNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#packageName}.
	 * @param ctx the parse tree
	 */
	void enterPackageName(CommandParser.PackageNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#packageName}.
	 * @param ctx the parse tree
	 */
	void exitPackageName(CommandParser.PackageNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#packageNameWithProject}.
	 * @param ctx the parse tree
	 */
	void enterPackageNameWithProject(CommandParser.PackageNameWithProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#packageNameWithProject}.
	 * @param ctx the parse tree
	 */
	void exitPackageNameWithProject(CommandParser.PackageNameWithProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#principalSpecification}.
	 * @param ctx the parse tree
	 */
	void enterPrincipalSpecification(CommandParser.PrincipalSpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#principalSpecification}.
	 * @param ctx the parse tree
	 */
	void exitPrincipalSpecification(CommandParser.PrincipalSpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#principalName}.
	 * @param ctx the parse tree
	 */
	void enterPrincipalName(CommandParser.PrincipalNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#principalName}.
	 * @param ctx the parse tree
	 */
	void exitPrincipalName(CommandParser.PrincipalNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#describePackage}.
	 * @param ctx the parse tree
	 */
	void enterDescribePackage(CommandParser.DescribePackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#describePackage}.
	 * @param ctx the parse tree
	 */
	void exitDescribePackage(CommandParser.DescribePackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#principalIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterPrincipalIdentifier(CommandParser.PrincipalIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#principalIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitPrincipalIdentifier(CommandParser.PrincipalIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#privilege}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege(CommandParser.PrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#privilege}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege(CommandParser.PrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#privilegeType}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeType(CommandParser.PrivilegeTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#privilegeType}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeType(CommandParser.PrivilegeTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#privilegeObject}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeObject(CommandParser.PrivilegeObjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#privilegeObject}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeObject(CommandParser.PrivilegeObjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#filePath}.
	 * @param ctx the parse tree
	 */
	void enterFilePath(CommandParser.FilePathContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#filePath}.
	 * @param ctx the parse tree
	 */
	void exitFilePath(CommandParser.FilePathContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#adminOptionFor}.
	 * @param ctx the parse tree
	 */
	void enterAdminOptionFor(CommandParser.AdminOptionForContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#adminOptionFor}.
	 * @param ctx the parse tree
	 */
	void exitAdminOptionFor(CommandParser.AdminOptionForContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#withAdminOption}.
	 * @param ctx the parse tree
	 */
	void enterWithAdminOption(CommandParser.WithAdminOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#withAdminOption}.
	 * @param ctx the parse tree
	 */
	void exitWithAdminOption(CommandParser.WithAdminOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#withGrantOption}.
	 * @param ctx the parse tree
	 */
	void enterWithGrantOption(CommandParser.WithGrantOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#withGrantOption}.
	 * @param ctx the parse tree
	 */
	void exitWithGrantOption(CommandParser.WithGrantOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#grantOptionFor}.
	 * @param ctx the parse tree
	 */
	void enterGrantOptionFor(CommandParser.GrantOptionForContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#grantOptionFor}.
	 * @param ctx the parse tree
	 */
	void exitGrantOptionFor(CommandParser.GrantOptionForContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#label}.
	 * @param ctx the parse tree
	 */
	void enterLabel(CommandParser.LabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#label}.
	 * @param ctx the parse tree
	 */
	void exitLabel(CommandParser.LabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameList(CommandParser.ColumnNameListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameList(CommandParser.ColumnNameListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnName}.
	 * @param ctx the parse tree
	 */
	void enterColumnName(CommandParser.ColumnNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnName}.
	 * @param ctx the parse tree
	 */
	void exitColumnName(CommandParser.ColumnNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#allIdentifiers}.
	 * @param ctx the parse tree
	 */
	void enterAllIdentifiers(CommandParser.AllIdentifiersContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#allIdentifiers}.
	 * @param ctx the parse tree
	 */
	void exitAllIdentifiers(CommandParser.AllIdentifiersContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#options}.
	 * @param ctx the parse tree
	 */
	void enterOptions(CommandParser.OptionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#options}.
	 * @param ctx the parse tree
	 */
	void exitOptions(CommandParser.OptionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#projectName}.
	 * @param ctx the parse tree
	 */
	void enterProjectName(CommandParser.ProjectNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#projectName}.
	 * @param ctx the parse tree
	 */
	void exitProjectName(CommandParser.ProjectNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatement(CommandParser.AlterStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatement(CommandParser.AlterStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterTableStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableStatementSuffix(CommandParser.AlterTableStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterTableStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableStatementSuffix(CommandParser.AlterTableStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterStatementSuffixArchive}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixArchive(CommandParser.AlterStatementSuffixArchiveContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterStatementSuffixArchive}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixArchive(CommandParser.AlterStatementSuffixArchiveContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterStatementSuffixMergeFiles}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixMergeFiles(CommandParser.AlterStatementSuffixMergeFilesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterStatementSuffixMergeFiles}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixMergeFiles(CommandParser.AlterStatementSuffixMergeFilesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterStatementSuffixCompact}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixCompact(CommandParser.AlterStatementSuffixCompactContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterStatementSuffixCompact}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixCompact(CommandParser.AlterStatementSuffixCompactContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#compactType}.
	 * @param ctx the parse tree
	 */
	void enterCompactType(CommandParser.CompactTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#compactType}.
	 * @param ctx the parse tree
	 */
	void exitCompactType(CommandParser.CompactTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterStatementSuffixFreeze}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixFreeze(CommandParser.AlterStatementSuffixFreezeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterStatementSuffixFreeze}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixFreeze(CommandParser.AlterStatementSuffixFreezeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#alterStatementSuffixRestore}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixRestore(CommandParser.AlterStatementSuffixRestoreContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#alterStatementSuffixRestore}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixRestore(CommandParser.AlterStatementSuffixRestoreContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#descStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescStatement(CommandParser.DescStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#descStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescStatement(CommandParser.DescStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#descSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescSchemaStatement(CommandParser.DescSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#descSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescSchemaStatement(CommandParser.DescSchemaStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#descTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescTableStatement(CommandParser.DescTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#descTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescTableStatement(CommandParser.DescTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#descTableExtendedStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescTableExtendedStatement(CommandParser.DescTableExtendedStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#descTableExtendedStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescTableExtendedStatement(CommandParser.DescTableExtendedStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#descProjectStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescProjectStatement(CommandParser.DescProjectStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#descProjectStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescProjectStatement(CommandParser.DescProjectStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#descInstanceStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescInstanceStatement(CommandParser.DescInstanceStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#descInstanceStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescInstanceStatement(CommandParser.DescInstanceStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStatement(CommandParser.ShowStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStatement(CommandParser.ShowStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showCreateTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateTableStatement(CommandParser.ShowCreateTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showCreateTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateTableStatement(CommandParser.ShowCreateTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showSchemasStatament}.
	 * @param ctx the parse tree
	 */
	void enterShowSchemasStatament(CommandParser.ShowSchemasStatamentContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showSchemasStatament}.
	 * @param ctx the parse tree
	 */
	void exitShowSchemasStatament(CommandParser.ShowSchemasStatamentContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showPartitionStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowPartitionStatement(CommandParser.ShowPartitionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showPartitionStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowPartitionStatement(CommandParser.ShowPartitionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showInstanceStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowInstanceStatement(CommandParser.ShowInstanceStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showInstanceStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowInstanceStatement(CommandParser.ShowInstanceStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableStatement(CommandParser.ShowTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableStatement(CommandParser.ShowTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#bareDate}.
	 * @param ctx the parse tree
	 */
	void enterBareDate(CommandParser.BareDateContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#bareDate}.
	 * @param ctx the parse tree
	 */
	void exitBareDate(CommandParser.BareDateContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#showStmtIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterShowStmtIdentifier(CommandParser.ShowStmtIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#showStmtIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitShowStmtIdentifier(CommandParser.ShowStmtIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void enterRowFormat(CommandParser.RowFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void exitRowFormat(CommandParser.RowFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#recordReader}.
	 * @param ctx the parse tree
	 */
	void enterRecordReader(CommandParser.RecordReaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#recordReader}.
	 * @param ctx the parse tree
	 */
	void exitRecordReader(CommandParser.RecordReaderContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#recordWriter}.
	 * @param ctx the parse tree
	 */
	void enterRecordWriter(CommandParser.RecordWriterContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#recordWriter}.
	 * @param ctx the parse tree
	 */
	void exitRecordWriter(CommandParser.RecordWriterContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#rowFormatSerde}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatSerde(CommandParser.RowFormatSerdeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#rowFormatSerde}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatSerde(CommandParser.RowFormatSerdeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#rowFormatDelimited}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatDelimited(CommandParser.RowFormatDelimitedContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#rowFormatDelimited}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatDelimited(CommandParser.RowFormatDelimitedContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableRowFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormat(CommandParser.TableRowFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableRowFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormat(CommandParser.TableRowFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableProperties}.
	 * @param ctx the parse tree
	 */
	void enterTableProperties(CommandParser.TablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableProperties}.
	 * @param ctx the parse tree
	 */
	void exitTableProperties(CommandParser.TablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tablePropertiesList}.
	 * @param ctx the parse tree
	 */
	void enterTablePropertiesList(CommandParser.TablePropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tablePropertiesList}.
	 * @param ctx the parse tree
	 */
	void exitTablePropertiesList(CommandParser.TablePropertiesListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#keyValueProperty}.
	 * @param ctx the parse tree
	 */
	void enterKeyValueProperty(CommandParser.KeyValuePropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#keyValueProperty}.
	 * @param ctx the parse tree
	 */
	void exitKeyValueProperty(CommandParser.KeyValuePropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#userDefinedJoinPropertiesList}.
	 * @param ctx the parse tree
	 */
	void enterUserDefinedJoinPropertiesList(CommandParser.UserDefinedJoinPropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#userDefinedJoinPropertiesList}.
	 * @param ctx the parse tree
	 */
	void exitUserDefinedJoinPropertiesList(CommandParser.UserDefinedJoinPropertiesListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableRowFormatFieldIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatFieldIdentifier(CommandParser.TableRowFormatFieldIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableRowFormatFieldIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatFieldIdentifier(CommandParser.TableRowFormatFieldIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableRowFormatCollItemsIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatCollItemsIdentifier(CommandParser.TableRowFormatCollItemsIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableRowFormatCollItemsIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatCollItemsIdentifier(CommandParser.TableRowFormatCollItemsIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableRowFormatMapKeysIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatMapKeysIdentifier(CommandParser.TableRowFormatMapKeysIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableRowFormatMapKeysIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatMapKeysIdentifier(CommandParser.TableRowFormatMapKeysIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableRowFormatLinesIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatLinesIdentifier(CommandParser.TableRowFormatLinesIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableRowFormatLinesIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatLinesIdentifier(CommandParser.TableRowFormatLinesIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableRowNullFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableRowNullFormat(CommandParser.TableRowNullFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableRowNullFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableRowNullFormat(CommandParser.TableRowNullFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameTypeList(CommandParser.ColumnNameTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameTypeList(CommandParser.ColumnNameTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameColonTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameColonTypeList(CommandParser.ColumnNameColonTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameColonTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameColonTypeList(CommandParser.ColumnNameColonTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameCommentList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameCommentList(CommandParser.ColumnNameCommentListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameCommentList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameCommentList(CommandParser.ColumnNameCommentListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameComment}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameComment(CommandParser.ColumnNameCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameComment}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameComment(CommandParser.ColumnNameCommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnRefOrder}.
	 * @param ctx the parse tree
	 */
	void enterColumnRefOrder(CommandParser.ColumnRefOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnRefOrder}.
	 * @param ctx the parse tree
	 */
	void exitColumnRefOrder(CommandParser.ColumnRefOrderContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameType}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameType(CommandParser.ColumnNameTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameType}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameType(CommandParser.ColumnNameTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameColonType}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameColonType(CommandParser.ColumnNameColonTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameColonType}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameColonType(CommandParser.ColumnNameColonTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#colType}.
	 * @param ctx the parse tree
	 */
	void enterColType(CommandParser.ColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#colType}.
	 * @param ctx the parse tree
	 */
	void exitColType(CommandParser.ColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColTypeList(CommandParser.ColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColTypeList(CommandParser.ColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#anyType}.
	 * @param ctx the parse tree
	 */
	void enterAnyType(CommandParser.AnyTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#anyType}.
	 * @param ctx the parse tree
	 */
	void exitAnyType(CommandParser.AnyTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#type}.
	 * @param ctx the parse tree
	 */
	void enterType(CommandParser.TypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#type}.
	 * @param ctx the parse tree
	 */
	void exitType(CommandParser.TypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#primitiveType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveType(CommandParser.PrimitiveTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#primitiveType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveType(CommandParser.PrimitiveTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#builtinTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void enterBuiltinTypeOrUdt(CommandParser.BuiltinTypeOrUdtContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#builtinTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void exitBuiltinTypeOrUdt(CommandParser.BuiltinTypeOrUdtContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#primitiveTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveTypeOrUdt(CommandParser.PrimitiveTypeOrUdtContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#primitiveTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveTypeOrUdt(CommandParser.PrimitiveTypeOrUdtContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#listType}.
	 * @param ctx the parse tree
	 */
	void enterListType(CommandParser.ListTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#listType}.
	 * @param ctx the parse tree
	 */
	void exitListType(CommandParser.ListTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#structType}.
	 * @param ctx the parse tree
	 */
	void enterStructType(CommandParser.StructTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#structType}.
	 * @param ctx the parse tree
	 */
	void exitStructType(CommandParser.StructTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#mapType}.
	 * @param ctx the parse tree
	 */
	void enterMapType(CommandParser.MapTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#mapType}.
	 * @param ctx the parse tree
	 */
	void exitMapType(CommandParser.MapTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#unionType}.
	 * @param ctx the parse tree
	 */
	void enterUnionType(CommandParser.UnionTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#unionType}.
	 * @param ctx the parse tree
	 */
	void exitUnionType(CommandParser.UnionTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#setOperator}.
	 * @param ctx the parse tree
	 */
	void enterSetOperator(CommandParser.SetOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#setOperator}.
	 * @param ctx the parse tree
	 */
	void exitSetOperator(CommandParser.SetOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#withClause}.
	 * @param ctx the parse tree
	 */
	void enterWithClause(CommandParser.WithClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#withClause}.
	 * @param ctx the parse tree
	 */
	void exitWithClause(CommandParser.WithClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(CommandParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(CommandParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectList}.
	 * @param ctx the parse tree
	 */
	void enterSelectList(CommandParser.SelectListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectList}.
	 * @param ctx the parse tree
	 */
	void exitSelectList(CommandParser.SelectListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectTrfmClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectTrfmClause(CommandParser.SelectTrfmClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectTrfmClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectTrfmClause(CommandParser.SelectTrfmClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#hintClause}.
	 * @param ctx the parse tree
	 */
	void enterHintClause(CommandParser.HintClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#hintClause}.
	 * @param ctx the parse tree
	 */
	void exitHintClause(CommandParser.HintClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#hintList}.
	 * @param ctx the parse tree
	 */
	void enterHintList(CommandParser.HintListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#hintList}.
	 * @param ctx the parse tree
	 */
	void exitHintList(CommandParser.HintListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#hintItem}.
	 * @param ctx the parse tree
	 */
	void enterHintItem(CommandParser.HintItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#hintItem}.
	 * @param ctx the parse tree
	 */
	void exitHintItem(CommandParser.HintItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#dynamicfilterHint}.
	 * @param ctx the parse tree
	 */
	void enterDynamicfilterHint(CommandParser.DynamicfilterHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#dynamicfilterHint}.
	 * @param ctx the parse tree
	 */
	void exitDynamicfilterHint(CommandParser.DynamicfilterHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#mapJoinHint}.
	 * @param ctx the parse tree
	 */
	void enterMapJoinHint(CommandParser.MapJoinHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#mapJoinHint}.
	 * @param ctx the parse tree
	 */
	void exitMapJoinHint(CommandParser.MapJoinHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#skewJoinHint}.
	 * @param ctx the parse tree
	 */
	void enterSkewJoinHint(CommandParser.SkewJoinHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#skewJoinHint}.
	 * @param ctx the parse tree
	 */
	void exitSkewJoinHint(CommandParser.SkewJoinHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectivityHint}.
	 * @param ctx the parse tree
	 */
	void enterSelectivityHint(CommandParser.SelectivityHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectivityHint}.
	 * @param ctx the parse tree
	 */
	void exitSelectivityHint(CommandParser.SelectivityHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#multipleSkewHintArgs}.
	 * @param ctx the parse tree
	 */
	void enterMultipleSkewHintArgs(CommandParser.MultipleSkewHintArgsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#multipleSkewHintArgs}.
	 * @param ctx the parse tree
	 */
	void exitMultipleSkewHintArgs(CommandParser.MultipleSkewHintArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#skewJoinHintArgs}.
	 * @param ctx the parse tree
	 */
	void enterSkewJoinHintArgs(CommandParser.SkewJoinHintArgsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#skewJoinHintArgs}.
	 * @param ctx the parse tree
	 */
	void exitSkewJoinHintArgs(CommandParser.SkewJoinHintArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#skewColumns}.
	 * @param ctx the parse tree
	 */
	void enterSkewColumns(CommandParser.SkewColumnsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#skewColumns}.
	 * @param ctx the parse tree
	 */
	void exitSkewColumns(CommandParser.SkewColumnsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#skewJoinHintKeyValues}.
	 * @param ctx the parse tree
	 */
	void enterSkewJoinHintKeyValues(CommandParser.SkewJoinHintKeyValuesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#skewJoinHintKeyValues}.
	 * @param ctx the parse tree
	 */
	void exitSkewJoinHintKeyValues(CommandParser.SkewJoinHintKeyValuesContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#hintName}.
	 * @param ctx the parse tree
	 */
	void enterHintName(CommandParser.HintNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#hintName}.
	 * @param ctx the parse tree
	 */
	void exitHintName(CommandParser.HintNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#hintArgs}.
	 * @param ctx the parse tree
	 */
	void enterHintArgs(CommandParser.HintArgsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#hintArgs}.
	 * @param ctx the parse tree
	 */
	void exitHintArgs(CommandParser.HintArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#hintArgName}.
	 * @param ctx the parse tree
	 */
	void enterHintArgName(CommandParser.HintArgNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#hintArgName}.
	 * @param ctx the parse tree
	 */
	void exitHintArgName(CommandParser.HintArgNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void enterSelectItem(CommandParser.SelectItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void exitSelectItem(CommandParser.SelectItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#trfmClause}.
	 * @param ctx the parse tree
	 */
	void enterTrfmClause(CommandParser.TrfmClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#trfmClause}.
	 * @param ctx the parse tree
	 */
	void exitTrfmClause(CommandParser.TrfmClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectExpression}.
	 * @param ctx the parse tree
	 */
	void enterSelectExpression(CommandParser.SelectExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectExpression}.
	 * @param ctx the parse tree
	 */
	void exitSelectExpression(CommandParser.SelectExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#selectExpressionList}.
	 * @param ctx the parse tree
	 */
	void enterSelectExpressionList(CommandParser.SelectExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#selectExpressionList}.
	 * @param ctx the parse tree
	 */
	void exitSelectExpressionList(CommandParser.SelectExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#window_clause}.
	 * @param ctx the parse tree
	 */
	void enterWindow_clause(CommandParser.Window_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#window_clause}.
	 * @param ctx the parse tree
	 */
	void exitWindow_clause(CommandParser.Window_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#window_defn}.
	 * @param ctx the parse tree
	 */
	void enterWindow_defn(CommandParser.Window_defnContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#window_defn}.
	 * @param ctx the parse tree
	 */
	void exitWindow_defn(CommandParser.Window_defnContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#window_specification}.
	 * @param ctx the parse tree
	 */
	void enterWindow_specification(CommandParser.Window_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#window_specification}.
	 * @param ctx the parse tree
	 */
	void exitWindow_specification(CommandParser.Window_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#window_frame}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame(CommandParser.Window_frameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#window_frame}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame(CommandParser.Window_frameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#frame_exclusion}.
	 * @param ctx the parse tree
	 */
	void enterFrame_exclusion(CommandParser.Frame_exclusionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#frame_exclusion}.
	 * @param ctx the parse tree
	 */
	void exitFrame_exclusion(CommandParser.Frame_exclusionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#window_frame_boundary}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_boundary(CommandParser.Window_frame_boundaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#window_frame_boundary}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_boundary(CommandParser.Window_frame_boundaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableAllColumns}.
	 * @param ctx the parse tree
	 */
	void enterTableAllColumns(CommandParser.TableAllColumnsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableAllColumns}.
	 * @param ctx the parse tree
	 */
	void exitTableAllColumns(CommandParser.TableAllColumnsContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#expressionList}.
	 * @param ctx the parse tree
	 */
	void enterExpressionList(CommandParser.ExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#expressionList}.
	 * @param ctx the parse tree
	 */
	void exitExpressionList(CommandParser.ExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#aliasList}.
	 * @param ctx the parse tree
	 */
	void enterAliasList(CommandParser.AliasListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#aliasList}.
	 * @param ctx the parse tree
	 */
	void exitAliasList(CommandParser.AliasListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(CommandParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(CommandParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#joinSource}.
	 * @param ctx the parse tree
	 */
	void enterJoinSource(CommandParser.JoinSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#joinSource}.
	 * @param ctx the parse tree
	 */
	void exitJoinSource(CommandParser.JoinSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#joinRHS}.
	 * @param ctx the parse tree
	 */
	void enterJoinRHS(CommandParser.JoinRHSContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#joinRHS}.
	 * @param ctx the parse tree
	 */
	void exitJoinRHS(CommandParser.JoinRHSContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#uniqueJoinSource}.
	 * @param ctx the parse tree
	 */
	void enterUniqueJoinSource(CommandParser.UniqueJoinSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#uniqueJoinSource}.
	 * @param ctx the parse tree
	 */
	void exitUniqueJoinSource(CommandParser.UniqueJoinSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#uniqueJoinExpr}.
	 * @param ctx the parse tree
	 */
	void enterUniqueJoinExpr(CommandParser.UniqueJoinExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#uniqueJoinExpr}.
	 * @param ctx the parse tree
	 */
	void exitUniqueJoinExpr(CommandParser.UniqueJoinExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#uniqueJoinToken}.
	 * @param ctx the parse tree
	 */
	void enterUniqueJoinToken(CommandParser.UniqueJoinTokenContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#uniqueJoinToken}.
	 * @param ctx the parse tree
	 */
	void exitUniqueJoinToken(CommandParser.UniqueJoinTokenContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#joinToken}.
	 * @param ctx the parse tree
	 */
	void enterJoinToken(CommandParser.JoinTokenContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#joinToken}.
	 * @param ctx the parse tree
	 */
	void exitJoinToken(CommandParser.JoinTokenContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void enterLateralView(CommandParser.LateralViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void exitLateralView(CommandParser.LateralViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void enterTableAlias(CommandParser.TableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void exitTableAlias(CommandParser.TableAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableBucketSample}.
	 * @param ctx the parse tree
	 */
	void enterTableBucketSample(CommandParser.TableBucketSampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableBucketSample}.
	 * @param ctx the parse tree
	 */
	void exitTableBucketSample(CommandParser.TableBucketSampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#splitSample}.
	 * @param ctx the parse tree
	 */
	void enterSplitSample(CommandParser.SplitSampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#splitSample}.
	 * @param ctx the parse tree
	 */
	void exitSplitSample(CommandParser.SplitSampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableSample}.
	 * @param ctx the parse tree
	 */
	void enterTableSample(CommandParser.TableSampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableSample}.
	 * @param ctx the parse tree
	 */
	void exitTableSample(CommandParser.TableSampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableSource}.
	 * @param ctx the parse tree
	 */
	void enterTableSource(CommandParser.TableSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableSource}.
	 * @param ctx the parse tree
	 */
	void exitTableSource(CommandParser.TableSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#availableSql11KeywordsForOdpsTableAlias}.
	 * @param ctx the parse tree
	 */
	void enterAvailableSql11KeywordsForOdpsTableAlias(CommandParser.AvailableSql11KeywordsForOdpsTableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#availableSql11KeywordsForOdpsTableAlias}.
	 * @param ctx the parse tree
	 */
	void exitAvailableSql11KeywordsForOdpsTableAlias(CommandParser.AvailableSql11KeywordsForOdpsTableAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#schemaName}.
	 * @param ctx the parse tree
	 */
	void enterSchemaName(CommandParser.SchemaNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#schemaName}.
	 * @param ctx the parse tree
	 */
	void exitSchemaName(CommandParser.SchemaNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableName}.
	 * @param ctx the parse tree
	 */
	void enterTableName(CommandParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableName}.
	 * @param ctx the parse tree
	 */
	void exitTableName(CommandParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#partitioningSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitioningSpec(CommandParser.PartitioningSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#partitioningSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitioningSpec(CommandParser.PartitioningSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#partitionTableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void enterPartitionTableFunctionSource(CommandParser.PartitionTableFunctionSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#partitionTableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void exitPartitionTableFunctionSource(CommandParser.PartitionTableFunctionSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#partitionedTableFunction}.
	 * @param ctx the parse tree
	 */
	void enterPartitionedTableFunction(CommandParser.PartitionedTableFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#partitionedTableFunction}.
	 * @param ctx the parse tree
	 */
	void exitPartitionedTableFunction(CommandParser.PartitionedTableFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(CommandParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(CommandParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#valueRowConstructor}.
	 * @param ctx the parse tree
	 */
	void enterValueRowConstructor(CommandParser.ValueRowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#valueRowConstructor}.
	 * @param ctx the parse tree
	 */
	void exitValueRowConstructor(CommandParser.ValueRowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#valuesTableConstructor}.
	 * @param ctx the parse tree
	 */
	void enterValuesTableConstructor(CommandParser.ValuesTableConstructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#valuesTableConstructor}.
	 * @param ctx the parse tree
	 */
	void exitValuesTableConstructor(CommandParser.ValuesTableConstructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#valuesClause}.
	 * @param ctx the parse tree
	 */
	void enterValuesClause(CommandParser.ValuesClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#valuesClause}.
	 * @param ctx the parse tree
	 */
	void exitValuesClause(CommandParser.ValuesClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#virtualTableSource}.
	 * @param ctx the parse tree
	 */
	void enterVirtualTableSource(CommandParser.VirtualTableSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#virtualTableSource}.
	 * @param ctx the parse tree
	 */
	void exitVirtualTableSource(CommandParser.VirtualTableSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#tableNameColList}.
	 * @param ctx the parse tree
	 */
	void enterTableNameColList(CommandParser.TableNameColListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#tableNameColList}.
	 * @param ctx the parse tree
	 */
	void exitTableNameColList(CommandParser.TableNameColListContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#functionTypeCubeOrRollup}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTypeCubeOrRollup(CommandParser.FunctionTypeCubeOrRollupContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#functionTypeCubeOrRollup}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTypeCubeOrRollup(CommandParser.FunctionTypeCubeOrRollupContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupingSetsItem}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetsItem(CommandParser.GroupingSetsItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupingSetsItem}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetsItem(CommandParser.GroupingSetsItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupingSetsClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetsClause(CommandParser.GroupingSetsClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupingSetsClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetsClause(CommandParser.GroupingSetsClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupByKey}.
	 * @param ctx the parse tree
	 */
	void enterGroupByKey(CommandParser.GroupByKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupByKey}.
	 * @param ctx the parse tree
	 */
	void exitGroupByKey(CommandParser.GroupByKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupByClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupByClause(CommandParser.GroupByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupByClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupByClause(CommandParser.GroupByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupingSetExpression}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetExpression(CommandParser.GroupingSetExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupingSetExpression}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetExpression(CommandParser.GroupingSetExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupingSetExpressionMultiple}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetExpressionMultiple(CommandParser.GroupingSetExpressionMultipleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupingSetExpressionMultiple}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetExpressionMultiple(CommandParser.GroupingSetExpressionMultipleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#groupingExpressionSingle}.
	 * @param ctx the parse tree
	 */
	void enterGroupingExpressionSingle(CommandParser.GroupingExpressionSingleContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#groupingExpressionSingle}.
	 * @param ctx the parse tree
	 */
	void exitGroupingExpressionSingle(CommandParser.GroupingExpressionSingleContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(CommandParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(CommandParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#havingCondition}.
	 * @param ctx the parse tree
	 */
	void enterHavingCondition(CommandParser.HavingConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#havingCondition}.
	 * @param ctx the parse tree
	 */
	void exitHavingCondition(CommandParser.HavingConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#expressionsInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterExpressionsInParenthese(CommandParser.ExpressionsInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#expressionsInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitExpressionsInParenthese(CommandParser.ExpressionsInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#expressionsNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterExpressionsNotInParenthese(CommandParser.ExpressionsNotInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#expressionsNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitExpressionsNotInParenthese(CommandParser.ExpressionsNotInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnRefOrderInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnRefOrderInParenthese(CommandParser.ColumnRefOrderInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnRefOrderInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnRefOrderInParenthese(CommandParser.ColumnRefOrderInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnRefOrderNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnRefOrderNotInParenthese(CommandParser.ColumnRefOrderNotInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnRefOrderNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnRefOrderNotInParenthese(CommandParser.ColumnRefOrderNotInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void enterOrderByClause(CommandParser.OrderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void exitOrderByClause(CommandParser.OrderByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameOrIndexInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrIndexInParenthese(CommandParser.ColumnNameOrIndexInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameOrIndexInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrIndexInParenthese(CommandParser.ColumnNameOrIndexInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameOrIndexNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrIndexNotInParenthese(CommandParser.ColumnNameOrIndexNotInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameOrIndexNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrIndexNotInParenthese(CommandParser.ColumnNameOrIndexNotInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#columnNameOrIndex}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrIndex(CommandParser.ColumnNameOrIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#columnNameOrIndex}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrIndex(CommandParser.ColumnNameOrIndexContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#zorderByClause}.
	 * @param ctx the parse tree
	 */
	void enterZorderByClause(CommandParser.ZorderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#zorderByClause}.
	 * @param ctx the parse tree
	 */
	void exitZorderByClause(CommandParser.ZorderByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#clusterByClause}.
	 * @param ctx the parse tree
	 */
	void enterClusterByClause(CommandParser.ClusterByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#clusterByClause}.
	 * @param ctx the parse tree
	 */
	void exitClusterByClause(CommandParser.ClusterByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#partitionByClause}.
	 * @param ctx the parse tree
	 */
	void enterPartitionByClause(CommandParser.PartitionByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#partitionByClause}.
	 * @param ctx the parse tree
	 */
	void exitPartitionByClause(CommandParser.PartitionByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#distributeByClause}.
	 * @param ctx the parse tree
	 */
	void enterDistributeByClause(CommandParser.DistributeByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#distributeByClause}.
	 * @param ctx the parse tree
	 */
	void exitDistributeByClause(CommandParser.DistributeByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#sortByClause}.
	 * @param ctx the parse tree
	 */
	void enterSortByClause(CommandParser.SortByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#sortByClause}.
	 * @param ctx the parse tree
	 */
	void exitSortByClause(CommandParser.SortByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#function}.
	 * @param ctx the parse tree
	 */
	void enterFunction(CommandParser.FunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#function}.
	 * @param ctx the parse tree
	 */
	void exitFunction(CommandParser.FunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionArgument(CommandParser.FunctionArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionArgument(CommandParser.FunctionArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#builtinFunctionStructure}.
	 * @param ctx the parse tree
	 */
	void enterBuiltinFunctionStructure(CommandParser.BuiltinFunctionStructureContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#builtinFunctionStructure}.
	 * @param ctx the parse tree
	 */
	void exitBuiltinFunctionStructure(CommandParser.BuiltinFunctionStructureContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#functionName}.
	 * @param ctx the parse tree
	 */
	void enterFunctionName(CommandParser.FunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#functionName}.
	 * @param ctx the parse tree
	 */
	void exitFunctionName(CommandParser.FunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#castExpression}.
	 * @param ctx the parse tree
	 */
	void enterCastExpression(CommandParser.CastExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#castExpression}.
	 * @param ctx the parse tree
	 */
	void exitCastExpression(CommandParser.CastExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#caseExpression}.
	 * @param ctx the parse tree
	 */
	void enterCaseExpression(CommandParser.CaseExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#caseExpression}.
	 * @param ctx the parse tree
	 */
	void exitCaseExpression(CommandParser.CaseExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#whenExpression}.
	 * @param ctx the parse tree
	 */
	void enterWhenExpression(CommandParser.WhenExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#whenExpression}.
	 * @param ctx the parse tree
	 */
	void exitWhenExpression(CommandParser.WhenExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterConstant(CommandParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitConstant(CommandParser.ConstantContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#simpleStringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterSimpleStringLiteral(CommandParser.SimpleStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#simpleStringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitSimpleStringLiteral(CommandParser.SimpleStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(CommandParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(CommandParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#doubleQuoteStringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDoubleQuoteStringLiteral(CommandParser.DoubleQuoteStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#doubleQuoteStringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDoubleQuoteStringLiteral(CommandParser.DoubleQuoteStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#charSetStringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterCharSetStringLiteral(CommandParser.CharSetStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#charSetStringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitCharSetStringLiteral(CommandParser.CharSetStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#dateLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDateLiteral(CommandParser.DateLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#dateLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDateLiteral(CommandParser.DateLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#dateTimeLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDateTimeLiteral(CommandParser.DateTimeLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#dateTimeLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDateTimeLiteral(CommandParser.DateTimeLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#timestampLiteral}.
	 * @param ctx the parse tree
	 */
	void enterTimestampLiteral(CommandParser.TimestampLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#timestampLiteral}.
	 * @param ctx the parse tree
	 */
	void exitTimestampLiteral(CommandParser.TimestampLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#intervalLiteral}.
	 * @param ctx the parse tree
	 */
	void enterIntervalLiteral(CommandParser.IntervalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#intervalLiteral}.
	 * @param ctx the parse tree
	 */
	void exitIntervalLiteral(CommandParser.IntervalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#intervalQualifiers}.
	 * @param ctx the parse tree
	 */
	void enterIntervalQualifiers(CommandParser.IntervalQualifiersContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#intervalQualifiers}.
	 * @param ctx the parse tree
	 */
	void exitIntervalQualifiers(CommandParser.IntervalQualifiersContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#intervalQualifiersUnit}.
	 * @param ctx the parse tree
	 */
	void enterIntervalQualifiersUnit(CommandParser.IntervalQualifiersUnitContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#intervalQualifiersUnit}.
	 * @param ctx the parse tree
	 */
	void exitIntervalQualifiersUnit(CommandParser.IntervalQualifiersUnitContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#intervalQualifierPrecision}.
	 * @param ctx the parse tree
	 */
	void enterIntervalQualifierPrecision(CommandParser.IntervalQualifierPrecisionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#intervalQualifierPrecision}.
	 * @param ctx the parse tree
	 */
	void exitIntervalQualifierPrecision(CommandParser.IntervalQualifierPrecisionContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(CommandParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(CommandParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpec(CommandParser.PartitionSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpec(CommandParser.PartitionSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void enterPartitionVal(CommandParser.PartitionValContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void exitPartitionVal(CommandParser.PartitionValContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#dateWithoutQuote}.
	 * @param ctx the parse tree
	 */
	void enterDateWithoutQuote(CommandParser.DateWithoutQuoteContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#dateWithoutQuote}.
	 * @param ctx the parse tree
	 */
	void exitDateWithoutQuote(CommandParser.DateWithoutQuoteContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterFunctionIdentifier(CommandParser.FunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitFunctionIdentifier(CommandParser.FunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(CommandParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(CommandParser.NonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#sql11ReservedKeywordsUsedAsCastFunctionName}.
	 * @param ctx the parse tree
	 */
	void enterSql11ReservedKeywordsUsedAsCastFunctionName(CommandParser.Sql11ReservedKeywordsUsedAsCastFunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#sql11ReservedKeywordsUsedAsCastFunctionName}.
	 * @param ctx the parse tree
	 */
	void exitSql11ReservedKeywordsUsedAsCastFunctionName(CommandParser.Sql11ReservedKeywordsUsedAsCastFunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#sql11ReservedKeywordsUsedAsIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSql11ReservedKeywordsUsedAsIdentifier(CommandParser.Sql11ReservedKeywordsUsedAsIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#sql11ReservedKeywordsUsedAsIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSql11ReservedKeywordsUsedAsIdentifier(CommandParser.Sql11ReservedKeywordsUsedAsIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link CommandParser#reserved}.
	 * @param ctx the parse tree
	 */
	void enterReserved(CommandParser.ReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link CommandParser#reserved}.
	 * @param ctx the parse tree
	 */
	void exitReserved(CommandParser.ReservedContext ctx);
}