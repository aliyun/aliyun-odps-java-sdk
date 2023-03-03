// Generated from java-escape by ANTLR 4.11.1
package com.aliyun.odps.sqa.commandapi.antlr.sql;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link OdpsParser}.
 */
public interface OdpsParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link OdpsParser#script}.
	 * @param ctx the parse tree
	 */
	void enterScript(OdpsParser.ScriptContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#script}.
	 * @param ctx the parse tree
	 */
	void exitScript(OdpsParser.ScriptContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#userCodeBlock}.
	 * @param ctx the parse tree
	 */
	void enterUserCodeBlock(OdpsParser.UserCodeBlockContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#userCodeBlock}.
	 * @param ctx the parse tree
	 */
	void exitUserCodeBlock(OdpsParser.UserCodeBlockContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(OdpsParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(OdpsParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#compoundStatement}.
	 * @param ctx the parse tree
	 */
	void enterCompoundStatement(OdpsParser.CompoundStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#compoundStatement}.
	 * @param ctx the parse tree
	 */
	void exitCompoundStatement(OdpsParser.CompoundStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#emptyStatement}.
	 * @param ctx the parse tree
	 */
	void enterEmptyStatement(OdpsParser.EmptyStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#emptyStatement}.
	 * @param ctx the parse tree
	 */
	void exitEmptyStatement(OdpsParser.EmptyStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#execStatement}.
	 * @param ctx the parse tree
	 */
	void enterExecStatement(OdpsParser.ExecStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#execStatement}.
	 * @param ctx the parse tree
	 */
	void exitExecStatement(OdpsParser.ExecStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#cteStatement}.
	 * @param ctx the parse tree
	 */
	void enterCteStatement(OdpsParser.CteStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#cteStatement}.
	 * @param ctx the parse tree
	 */
	void exitCteStatement(OdpsParser.CteStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableAliasWithCols}.
	 * @param ctx the parse tree
	 */
	void enterTableAliasWithCols(OdpsParser.TableAliasWithColsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableAliasWithCols}.
	 * @param ctx the parse tree
	 */
	void exitTableAliasWithCols(OdpsParser.TableAliasWithColsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#subQuerySource}.
	 * @param ctx the parse tree
	 */
	void enterSubQuerySource(OdpsParser.SubQuerySourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#subQuerySource}.
	 * @param ctx the parse tree
	 */
	void exitSubQuerySource(OdpsParser.SubQuerySourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#explainStatement}.
	 * @param ctx the parse tree
	 */
	void enterExplainStatement(OdpsParser.ExplainStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#explainStatement}.
	 * @param ctx the parse tree
	 */
	void exitExplainStatement(OdpsParser.ExplainStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#ifStatement}.
	 * @param ctx the parse tree
	 */
	void enterIfStatement(OdpsParser.IfStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#ifStatement}.
	 * @param ctx the parse tree
	 */
	void exitIfStatement(OdpsParser.IfStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#loopStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoopStatement(OdpsParser.LoopStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#loopStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoopStatement(OdpsParser.LoopStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionDefinition}.
	 * @param ctx the parse tree
	 */
	void enterFunctionDefinition(OdpsParser.FunctionDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionDefinition}.
	 * @param ctx the parse tree
	 */
	void exitFunctionDefinition(OdpsParser.FunctionDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionParameters}.
	 * @param ctx the parse tree
	 */
	void enterFunctionParameters(OdpsParser.FunctionParametersContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionParameters}.
	 * @param ctx the parse tree
	 */
	void exitFunctionParameters(OdpsParser.FunctionParametersContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#parameterDefinition}.
	 * @param ctx the parse tree
	 */
	void enterParameterDefinition(OdpsParser.ParameterDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#parameterDefinition}.
	 * @param ctx the parse tree
	 */
	void exitParameterDefinition(OdpsParser.ParameterDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#typeDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterTypeDeclaration(OdpsParser.TypeDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#typeDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitTypeDeclaration(OdpsParser.TypeDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#parameterTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterParameterTypeDeclaration(OdpsParser.ParameterTypeDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#parameterTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitParameterTypeDeclaration(OdpsParser.ParameterTypeDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTypeDeclaration(OdpsParser.FunctionTypeDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionTypeDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTypeDeclaration(OdpsParser.FunctionTypeDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#parameterTypeDeclarationList}.
	 * @param ctx the parse tree
	 */
	void enterParameterTypeDeclarationList(OdpsParser.ParameterTypeDeclarationListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#parameterTypeDeclarationList}.
	 * @param ctx the parse tree
	 */
	void exitParameterTypeDeclarationList(OdpsParser.ParameterTypeDeclarationListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#parameterColumnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void enterParameterColumnNameTypeList(OdpsParser.ParameterColumnNameTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#parameterColumnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void exitParameterColumnNameTypeList(OdpsParser.ParameterColumnNameTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#parameterColumnNameType}.
	 * @param ctx the parse tree
	 */
	void enterParameterColumnNameType(OdpsParser.ParameterColumnNameTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#parameterColumnNameType}.
	 * @param ctx the parse tree
	 */
	void exitParameterColumnNameType(OdpsParser.ParameterColumnNameTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#varSizeParam}.
	 * @param ctx the parse tree
	 */
	void enterVarSizeParam(OdpsParser.VarSizeParamContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#varSizeParam}.
	 * @param ctx the parse tree
	 */
	void exitVarSizeParam(OdpsParser.VarSizeParamContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#assignStatement}.
	 * @param ctx the parse tree
	 */
	void enterAssignStatement(OdpsParser.AssignStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#assignStatement}.
	 * @param ctx the parse tree
	 */
	void exitAssignStatement(OdpsParser.AssignStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#preSelectClauses}.
	 * @param ctx the parse tree
	 */
	void enterPreSelectClauses(OdpsParser.PreSelectClausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#preSelectClauses}.
	 * @param ctx the parse tree
	 */
	void exitPreSelectClauses(OdpsParser.PreSelectClausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#postSelectClauses}.
	 * @param ctx the parse tree
	 */
	void enterPostSelectClauses(OdpsParser.PostSelectClausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#postSelectClauses}.
	 * @param ctx the parse tree
	 */
	void exitPostSelectClauses(OdpsParser.PostSelectClausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectRest}.
	 * @param ctx the parse tree
	 */
	void enterSelectRest(OdpsParser.SelectRestContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectRest}.
	 * @param ctx the parse tree
	 */
	void exitSelectRest(OdpsParser.SelectRestContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multiInsertFromRest}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertFromRest(OdpsParser.MultiInsertFromRestContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multiInsertFromRest}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertFromRest(OdpsParser.MultiInsertFromRestContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fromRest}.
	 * @param ctx the parse tree
	 */
	void enterFromRest(OdpsParser.FromRestContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fromRest}.
	 * @param ctx the parse tree
	 */
	void exitFromRest(OdpsParser.FromRestContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#simpleQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleQueryExpression(OdpsParser.SimpleQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#simpleQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleQueryExpression(OdpsParser.SimpleQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSelectQueryExpression(OdpsParser.SelectQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSelectQueryExpression(OdpsParser.SelectQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fromQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFromQueryExpression(OdpsParser.FromQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fromQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFromQueryExpression(OdpsParser.FromQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setOperationFactor}.
	 * @param ctx the parse tree
	 */
	void enterSetOperationFactor(OdpsParser.SetOperationFactorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setOperationFactor}.
	 * @param ctx the parse tree
	 */
	void exitSetOperationFactor(OdpsParser.SetOperationFactorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#queryExpression}.
	 * @param ctx the parse tree
	 */
	void enterQueryExpression(OdpsParser.QueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#queryExpression}.
	 * @param ctx the parse tree
	 */
	void exitQueryExpression(OdpsParser.QueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#queryExpressionWithCTE}.
	 * @param ctx the parse tree
	 */
	void enterQueryExpressionWithCTE(OdpsParser.QueryExpressionWithCTEContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#queryExpressionWithCTE}.
	 * @param ctx the parse tree
	 */
	void exitQueryExpressionWithCTE(OdpsParser.QueryExpressionWithCTEContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setRHS}.
	 * @param ctx the parse tree
	 */
	void enterSetRHS(OdpsParser.SetRHSContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setRHS}.
	 * @param ctx the parse tree
	 */
	void exitSetRHS(OdpsParser.SetRHSContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multiInsertSetOperationFactor}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertSetOperationFactor(OdpsParser.MultiInsertSetOperationFactorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multiInsertSetOperationFactor}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertSetOperationFactor(OdpsParser.MultiInsertSetOperationFactorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multiInsertSelect}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertSelect(OdpsParser.MultiInsertSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multiInsertSelect}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertSelect(OdpsParser.MultiInsertSelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multiInsertSetRHS}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertSetRHS(OdpsParser.MultiInsertSetRHSContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multiInsertSetRHS}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertSetRHS(OdpsParser.MultiInsertSetRHSContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multiInsertBranch}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertBranch(OdpsParser.MultiInsertBranchContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multiInsertBranch}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertBranch(OdpsParser.MultiInsertBranchContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fromStatement}.
	 * @param ctx the parse tree
	 */
	void enterFromStatement(OdpsParser.FromStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fromStatement}.
	 * @param ctx the parse tree
	 */
	void exitFromStatement(OdpsParser.FromStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#insertStatement}.
	 * @param ctx the parse tree
	 */
	void enterInsertStatement(OdpsParser.InsertStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#insertStatement}.
	 * @param ctx the parse tree
	 */
	void exitInsertStatement(OdpsParser.InsertStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectQueryStatement}.
	 * @param ctx the parse tree
	 */
	void enterSelectQueryStatement(OdpsParser.SelectQueryStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectQueryStatement}.
	 * @param ctx the parse tree
	 */
	void exitSelectQueryStatement(OdpsParser.SelectQueryStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void enterQueryStatement(OdpsParser.QueryStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	void exitQueryStatement(OdpsParser.QueryStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#insertStatementWithCTE}.
	 * @param ctx the parse tree
	 */
	void enterInsertStatementWithCTE(OdpsParser.InsertStatementWithCTEContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#insertStatementWithCTE}.
	 * @param ctx the parse tree
	 */
	void exitInsertStatementWithCTE(OdpsParser.InsertStatementWithCTEContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#subQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubQueryExpression(OdpsParser.SubQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#subQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubQueryExpression(OdpsParser.SubQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void enterLimitClause(OdpsParser.LimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void exitLimitClause(OdpsParser.LimitClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fromSource}.
	 * @param ctx the parse tree
	 */
	void enterFromSource(OdpsParser.FromSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fromSource}.
	 * @param ctx the parse tree
	 */
	void exitFromSource(OdpsParser.FromSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableVariableSource}.
	 * @param ctx the parse tree
	 */
	void enterTableVariableSource(OdpsParser.TableVariableSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableVariableSource}.
	 * @param ctx the parse tree
	 */
	void exitTableVariableSource(OdpsParser.TableVariableSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void enterTableFunctionSource(OdpsParser.TableFunctionSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void exitTableFunctionSource(OdpsParser.TableFunctionSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createMachineLearningModelStatment}.
	 * @param ctx the parse tree
	 */
	void enterCreateMachineLearningModelStatment(OdpsParser.CreateMachineLearningModelStatmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createMachineLearningModelStatment}.
	 * @param ctx the parse tree
	 */
	void exitCreateMachineLearningModelStatment(OdpsParser.CreateMachineLearningModelStatmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#variableName}.
	 * @param ctx the parse tree
	 */
	void enterVariableName(OdpsParser.VariableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#variableName}.
	 * @param ctx the parse tree
	 */
	void exitVariableName(OdpsParser.VariableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#atomExpression}.
	 * @param ctx the parse tree
	 */
	void enterAtomExpression(OdpsParser.AtomExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#atomExpression}.
	 * @param ctx the parse tree
	 */
	void exitAtomExpression(OdpsParser.AtomExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#variableRef}.
	 * @param ctx the parse tree
	 */
	void enterVariableRef(OdpsParser.VariableRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#variableRef}.
	 * @param ctx the parse tree
	 */
	void exitVariableRef(OdpsParser.VariableRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#variableCall}.
	 * @param ctx the parse tree
	 */
	void enterVariableCall(OdpsParser.VariableCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#variableCall}.
	 * @param ctx the parse tree
	 */
	void exitVariableCall(OdpsParser.VariableCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#funNameRef}.
	 * @param ctx the parse tree
	 */
	void enterFunNameRef(OdpsParser.FunNameRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#funNameRef}.
	 * @param ctx the parse tree
	 */
	void exitFunNameRef(OdpsParser.FunNameRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#lambdaExpression}.
	 * @param ctx the parse tree
	 */
	void enterLambdaExpression(OdpsParser.LambdaExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#lambdaExpression}.
	 * @param ctx the parse tree
	 */
	void exitLambdaExpression(OdpsParser.LambdaExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#lambdaParameter}.
	 * @param ctx the parse tree
	 */
	void enterLambdaParameter(OdpsParser.LambdaParameterContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#lambdaParameter}.
	 * @param ctx the parse tree
	 */
	void exitLambdaParameter(OdpsParser.LambdaParameterContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableOrColumnRef}.
	 * @param ctx the parse tree
	 */
	void enterTableOrColumnRef(OdpsParser.TableOrColumnRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableOrColumnRef}.
	 * @param ctx the parse tree
	 */
	void exitTableOrColumnRef(OdpsParser.TableOrColumnRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#newExpression}.
	 * @param ctx the parse tree
	 */
	void enterNewExpression(OdpsParser.NewExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#newExpression}.
	 * @param ctx the parse tree
	 */
	void exitNewExpression(OdpsParser.NewExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#existsExpression}.
	 * @param ctx the parse tree
	 */
	void enterExistsExpression(OdpsParser.ExistsExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#existsExpression}.
	 * @param ctx the parse tree
	 */
	void exitExistsExpression(OdpsParser.ExistsExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#scalarSubQueryExpression}.
	 * @param ctx the parse tree
	 */
	void enterScalarSubQueryExpression(OdpsParser.ScalarSubQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#scalarSubQueryExpression}.
	 * @param ctx the parse tree
	 */
	void exitScalarSubQueryExpression(OdpsParser.ScalarSubQueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#classNameWithPackage}.
	 * @param ctx the parse tree
	 */
	void enterClassNameWithPackage(OdpsParser.ClassNameWithPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#classNameWithPackage}.
	 * @param ctx the parse tree
	 */
	void exitClassNameWithPackage(OdpsParser.ClassNameWithPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#classNameOrArrayDecl}.
	 * @param ctx the parse tree
	 */
	void enterClassNameOrArrayDecl(OdpsParser.ClassNameOrArrayDeclContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#classNameOrArrayDecl}.
	 * @param ctx the parse tree
	 */
	void exitClassNameOrArrayDecl(OdpsParser.ClassNameOrArrayDeclContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#classNameList}.
	 * @param ctx the parse tree
	 */
	void enterClassNameList(OdpsParser.ClassNameListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#classNameList}.
	 * @param ctx the parse tree
	 */
	void exitClassNameList(OdpsParser.ClassNameListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#odpsqlNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterOdpsqlNonReserved(OdpsParser.OdpsqlNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#odpsqlNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitOdpsqlNonReserved(OdpsParser.OdpsqlNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#relaxedKeywords}.
	 * @param ctx the parse tree
	 */
	void enterRelaxedKeywords(OdpsParser.RelaxedKeywordsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#relaxedKeywords}.
	 * @param ctx the parse tree
	 */
	void exitRelaxedKeywords(OdpsParser.RelaxedKeywordsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#allIdentifiers}.
	 * @param ctx the parse tree
	 */
	void enterAllIdentifiers(OdpsParser.AllIdentifiersContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#allIdentifiers}.
	 * @param ctx the parse tree
	 */
	void exitAllIdentifiers(OdpsParser.AllIdentifiersContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(OdpsParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(OdpsParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#aliasIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterAliasIdentifier(OdpsParser.AliasIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#aliasIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitAliasIdentifier(OdpsParser.AliasIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#identifierWithoutSql11}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierWithoutSql11(OdpsParser.IdentifierWithoutSql11Context ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#identifierWithoutSql11}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierWithoutSql11(OdpsParser.IdentifierWithoutSql11Context ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTableChangeOwner}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableChangeOwner(OdpsParser.AlterTableChangeOwnerContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTableChangeOwner}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableChangeOwner(OdpsParser.AlterTableChangeOwnerContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterViewChangeOwner}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewChangeOwner(OdpsParser.AlterViewChangeOwnerContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterViewChangeOwner}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewChangeOwner(OdpsParser.AlterViewChangeOwnerContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTableEnableHubTable}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableEnableHubTable(OdpsParser.AlterTableEnableHubTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTableEnableHubTable}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableEnableHubTable(OdpsParser.AlterTableEnableHubTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableLifecycle}.
	 * @param ctx the parse tree
	 */
	void enterTableLifecycle(OdpsParser.TableLifecycleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableLifecycle}.
	 * @param ctx the parse tree
	 */
	void exitTableLifecycle(OdpsParser.TableLifecycleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetStatement(OdpsParser.SetStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetStatement(OdpsParser.SetStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#anythingButEqualOrSemi}.
	 * @param ctx the parse tree
	 */
	void enterAnythingButEqualOrSemi(OdpsParser.AnythingButEqualOrSemiContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#anythingButEqualOrSemi}.
	 * @param ctx the parse tree
	 */
	void exitAnythingButEqualOrSemi(OdpsParser.AnythingButEqualOrSemiContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#anythingButSemi}.
	 * @param ctx the parse tree
	 */
	void enterAnythingButSemi(OdpsParser.AnythingButSemiContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#anythingButSemi}.
	 * @param ctx the parse tree
	 */
	void exitAnythingButSemi(OdpsParser.AnythingButSemiContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setProjectStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetProjectStatement(OdpsParser.SetProjectStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setProjectStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetProjectStatement(OdpsParser.SetProjectStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#label}.
	 * @param ctx the parse tree
	 */
	void enterLabel(OdpsParser.LabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#label}.
	 * @param ctx the parse tree
	 */
	void exitLabel(OdpsParser.LabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewInfoVal}.
	 * @param ctx the parse tree
	 */
	void enterSkewInfoVal(OdpsParser.SkewInfoValContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewInfoVal}.
	 * @param ctx the parse tree
	 */
	void exitSkewInfoVal(OdpsParser.SkewInfoValContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#memberAccessOperator}.
	 * @param ctx the parse tree
	 */
	void enterMemberAccessOperator(OdpsParser.MemberAccessOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#memberAccessOperator}.
	 * @param ctx the parse tree
	 */
	void exitMemberAccessOperator(OdpsParser.MemberAccessOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#methodAccessOperator}.
	 * @param ctx the parse tree
	 */
	void enterMethodAccessOperator(OdpsParser.MethodAccessOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#methodAccessOperator}.
	 * @param ctx the parse tree
	 */
	void exitMethodAccessOperator(OdpsParser.MethodAccessOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#isNullOperator}.
	 * @param ctx the parse tree
	 */
	void enterIsNullOperator(OdpsParser.IsNullOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#isNullOperator}.
	 * @param ctx the parse tree
	 */
	void exitIsNullOperator(OdpsParser.IsNullOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#inOperator}.
	 * @param ctx the parse tree
	 */
	void enterInOperator(OdpsParser.InOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#inOperator}.
	 * @param ctx the parse tree
	 */
	void exitInOperator(OdpsParser.InOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#betweenOperator}.
	 * @param ctx the parse tree
	 */
	void enterBetweenOperator(OdpsParser.BetweenOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#betweenOperator}.
	 * @param ctx the parse tree
	 */
	void exitBetweenOperator(OdpsParser.BetweenOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mathExpression}.
	 * @param ctx the parse tree
	 */
	void enterMathExpression(OdpsParser.MathExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mathExpression}.
	 * @param ctx the parse tree
	 */
	void exitMathExpression(OdpsParser.MathExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#unarySuffixExpression}.
	 * @param ctx the parse tree
	 */
	void enterUnarySuffixExpression(OdpsParser.UnarySuffixExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#unarySuffixExpression}.
	 * @param ctx the parse tree
	 */
	void exitUnarySuffixExpression(OdpsParser.UnarySuffixExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#unaryPrefixExpression}.
	 * @param ctx the parse tree
	 */
	void enterUnaryPrefixExpression(OdpsParser.UnaryPrefixExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#unaryPrefixExpression}.
	 * @param ctx the parse tree
	 */
	void exitUnaryPrefixExpression(OdpsParser.UnaryPrefixExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fieldExpression}.
	 * @param ctx the parse tree
	 */
	void enterFieldExpression(OdpsParser.FieldExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fieldExpression}.
	 * @param ctx the parse tree
	 */
	void exitFieldExpression(OdpsParser.FieldExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalExpression(OdpsParser.LogicalExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalExpression(OdpsParser.LogicalExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#notExpression}.
	 * @param ctx the parse tree
	 */
	void enterNotExpression(OdpsParser.NotExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#notExpression}.
	 * @param ctx the parse tree
	 */
	void exitNotExpression(OdpsParser.NotExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#equalExpression}.
	 * @param ctx the parse tree
	 */
	void enterEqualExpression(OdpsParser.EqualExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#equalExpression}.
	 * @param ctx the parse tree
	 */
	void exitEqualExpression(OdpsParser.EqualExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mathExpressionListInParentheses}.
	 * @param ctx the parse tree
	 */
	void enterMathExpressionListInParentheses(OdpsParser.MathExpressionListInParenthesesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mathExpressionListInParentheses}.
	 * @param ctx the parse tree
	 */
	void exitMathExpressionListInParentheses(OdpsParser.MathExpressionListInParenthesesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mathExpressionList}.
	 * @param ctx the parse tree
	 */
	void enterMathExpressionList(OdpsParser.MathExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mathExpressionList}.
	 * @param ctx the parse tree
	 */
	void exitMathExpressionList(OdpsParser.MathExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(OdpsParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(OdpsParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#statisticStatement}.
	 * @param ctx the parse tree
	 */
	void enterStatisticStatement(OdpsParser.StatisticStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#statisticStatement}.
	 * @param ctx the parse tree
	 */
	void exitStatisticStatement(OdpsParser.StatisticStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addRemoveStatisticStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddRemoveStatisticStatement(OdpsParser.AddRemoveStatisticStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addRemoveStatisticStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddRemoveStatisticStatement(OdpsParser.AddRemoveStatisticStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#statisticInfo}.
	 * @param ctx the parse tree
	 */
	void enterStatisticInfo(OdpsParser.StatisticInfoContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#statisticInfo}.
	 * @param ctx the parse tree
	 */
	void exitStatisticInfo(OdpsParser.StatisticInfoContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showStatisticStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStatisticStatement(OdpsParser.ShowStatisticStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showStatisticStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStatisticStatement(OdpsParser.ShowStatisticStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showStatisticListStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStatisticListStatement(OdpsParser.ShowStatisticListStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showStatisticListStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStatisticListStatement(OdpsParser.ShowStatisticListStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#countTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterCountTableStatement(OdpsParser.CountTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#countTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitCountTableStatement(OdpsParser.CountTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#statisticName}.
	 * @param ctx the parse tree
	 */
	void enterStatisticName(OdpsParser.StatisticNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#statisticName}.
	 * @param ctx the parse tree
	 */
	void exitStatisticName(OdpsParser.StatisticNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#instanceManagement}.
	 * @param ctx the parse tree
	 */
	void enterInstanceManagement(OdpsParser.InstanceManagementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#instanceManagement}.
	 * @param ctx the parse tree
	 */
	void exitInstanceManagement(OdpsParser.InstanceManagementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#instanceStatus}.
	 * @param ctx the parse tree
	 */
	void enterInstanceStatus(OdpsParser.InstanceStatusContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#instanceStatus}.
	 * @param ctx the parse tree
	 */
	void exitInstanceStatus(OdpsParser.InstanceStatusContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#killInstance}.
	 * @param ctx the parse tree
	 */
	void enterKillInstance(OdpsParser.KillInstanceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#killInstance}.
	 * @param ctx the parse tree
	 */
	void exitKillInstance(OdpsParser.KillInstanceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#instanceId}.
	 * @param ctx the parse tree
	 */
	void enterInstanceId(OdpsParser.InstanceIdContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#instanceId}.
	 * @param ctx the parse tree
	 */
	void exitInstanceId(OdpsParser.InstanceIdContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#resourceManagement}.
	 * @param ctx the parse tree
	 */
	void enterResourceManagement(OdpsParser.ResourceManagementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#resourceManagement}.
	 * @param ctx the parse tree
	 */
	void exitResourceManagement(OdpsParser.ResourceManagementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addResource}.
	 * @param ctx the parse tree
	 */
	void enterAddResource(OdpsParser.AddResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addResource}.
	 * @param ctx the parse tree
	 */
	void exitAddResource(OdpsParser.AddResourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropResource}.
	 * @param ctx the parse tree
	 */
	void enterDropResource(OdpsParser.DropResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropResource}.
	 * @param ctx the parse tree
	 */
	void exitDropResource(OdpsParser.DropResourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#resourceId}.
	 * @param ctx the parse tree
	 */
	void enterResourceId(OdpsParser.ResourceIdContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#resourceId}.
	 * @param ctx the parse tree
	 */
	void exitResourceId(OdpsParser.ResourceIdContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropOfflineModel}.
	 * @param ctx the parse tree
	 */
	void enterDropOfflineModel(OdpsParser.DropOfflineModelContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropOfflineModel}.
	 * @param ctx the parse tree
	 */
	void exitDropOfflineModel(OdpsParser.DropOfflineModelContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#getResource}.
	 * @param ctx the parse tree
	 */
	void enterGetResource(OdpsParser.GetResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#getResource}.
	 * @param ctx the parse tree
	 */
	void exitGetResource(OdpsParser.GetResourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#options}.
	 * @param ctx the parse tree
	 */
	void enterOptions(OdpsParser.OptionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#options}.
	 * @param ctx the parse tree
	 */
	void exitOptions(OdpsParser.OptionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#authorizationStatement}.
	 * @param ctx the parse tree
	 */
	void enterAuthorizationStatement(OdpsParser.AuthorizationStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#authorizationStatement}.
	 * @param ctx the parse tree
	 */
	void exitAuthorizationStatement(OdpsParser.AuthorizationStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#listUsers}.
	 * @param ctx the parse tree
	 */
	void enterListUsers(OdpsParser.ListUsersContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#listUsers}.
	 * @param ctx the parse tree
	 */
	void exitListUsers(OdpsParser.ListUsersContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#listGroups}.
	 * @param ctx the parse tree
	 */
	void enterListGroups(OdpsParser.ListGroupsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#listGroups}.
	 * @param ctx the parse tree
	 */
	void exitListGroups(OdpsParser.ListGroupsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addUserStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddUserStatement(OdpsParser.AddUserStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addUserStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddUserStatement(OdpsParser.AddUserStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addGroupStatement}.
	 * @param ctx the parse tree
	 */
	void enterAddGroupStatement(OdpsParser.AddGroupStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addGroupStatement}.
	 * @param ctx the parse tree
	 */
	void exitAddGroupStatement(OdpsParser.AddGroupStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#removeUserStatement}.
	 * @param ctx the parse tree
	 */
	void enterRemoveUserStatement(OdpsParser.RemoveUserStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#removeUserStatement}.
	 * @param ctx the parse tree
	 */
	void exitRemoveUserStatement(OdpsParser.RemoveUserStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#removeGroupStatement}.
	 * @param ctx the parse tree
	 */
	void enterRemoveGroupStatement(OdpsParser.RemoveGroupStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#removeGroupStatement}.
	 * @param ctx the parse tree
	 */
	void exitRemoveGroupStatement(OdpsParser.RemoveGroupStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addAccountProvider}.
	 * @param ctx the parse tree
	 */
	void enterAddAccountProvider(OdpsParser.AddAccountProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addAccountProvider}.
	 * @param ctx the parse tree
	 */
	void exitAddAccountProvider(OdpsParser.AddAccountProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#removeAccountProvider}.
	 * @param ctx the parse tree
	 */
	void enterRemoveAccountProvider(OdpsParser.RemoveAccountProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#removeAccountProvider}.
	 * @param ctx the parse tree
	 */
	void exitRemoveAccountProvider(OdpsParser.RemoveAccountProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showAcl}.
	 * @param ctx the parse tree
	 */
	void enterShowAcl(OdpsParser.ShowAclContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showAcl}.
	 * @param ctx the parse tree
	 */
	void exitShowAcl(OdpsParser.ShowAclContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#listRoles}.
	 * @param ctx the parse tree
	 */
	void enterListRoles(OdpsParser.ListRolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#listRoles}.
	 * @param ctx the parse tree
	 */
	void exitListRoles(OdpsParser.ListRolesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#whoami}.
	 * @param ctx the parse tree
	 */
	void enterWhoami(OdpsParser.WhoamiContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#whoami}.
	 * @param ctx the parse tree
	 */
	void exitWhoami(OdpsParser.WhoamiContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#listTrustedProjects}.
	 * @param ctx the parse tree
	 */
	void enterListTrustedProjects(OdpsParser.ListTrustedProjectsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#listTrustedProjects}.
	 * @param ctx the parse tree
	 */
	void exitListTrustedProjects(OdpsParser.ListTrustedProjectsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addTrustedProject}.
	 * @param ctx the parse tree
	 */
	void enterAddTrustedProject(OdpsParser.AddTrustedProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addTrustedProject}.
	 * @param ctx the parse tree
	 */
	void exitAddTrustedProject(OdpsParser.AddTrustedProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#removeTrustedProject}.
	 * @param ctx the parse tree
	 */
	void enterRemoveTrustedProject(OdpsParser.RemoveTrustedProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#removeTrustedProject}.
	 * @param ctx the parse tree
	 */
	void exitRemoveTrustedProject(OdpsParser.RemoveTrustedProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showSecurityConfiguration}.
	 * @param ctx the parse tree
	 */
	void enterShowSecurityConfiguration(OdpsParser.ShowSecurityConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showSecurityConfiguration}.
	 * @param ctx the parse tree
	 */
	void exitShowSecurityConfiguration(OdpsParser.ShowSecurityConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showPackages}.
	 * @param ctx the parse tree
	 */
	void enterShowPackages(OdpsParser.ShowPackagesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showPackages}.
	 * @param ctx the parse tree
	 */
	void exitShowPackages(OdpsParser.ShowPackagesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showItems}.
	 * @param ctx the parse tree
	 */
	void enterShowItems(OdpsParser.ShowItemsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showItems}.
	 * @param ctx the parse tree
	 */
	void exitShowItems(OdpsParser.ShowItemsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#installPackage}.
	 * @param ctx the parse tree
	 */
	void enterInstallPackage(OdpsParser.InstallPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#installPackage}.
	 * @param ctx the parse tree
	 */
	void exitInstallPackage(OdpsParser.InstallPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#uninstallPackage}.
	 * @param ctx the parse tree
	 */
	void enterUninstallPackage(OdpsParser.UninstallPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#uninstallPackage}.
	 * @param ctx the parse tree
	 */
	void exitUninstallPackage(OdpsParser.UninstallPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createPackage}.
	 * @param ctx the parse tree
	 */
	void enterCreatePackage(OdpsParser.CreatePackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createPackage}.
	 * @param ctx the parse tree
	 */
	void exitCreatePackage(OdpsParser.CreatePackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#deletePackage}.
	 * @param ctx the parse tree
	 */
	void enterDeletePackage(OdpsParser.DeletePackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#deletePackage}.
	 * @param ctx the parse tree
	 */
	void exitDeletePackage(OdpsParser.DeletePackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addToPackage}.
	 * @param ctx the parse tree
	 */
	void enterAddToPackage(OdpsParser.AddToPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addToPackage}.
	 * @param ctx the parse tree
	 */
	void exitAddToPackage(OdpsParser.AddToPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#removeFromPackage}.
	 * @param ctx the parse tree
	 */
	void enterRemoveFromPackage(OdpsParser.RemoveFromPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#removeFromPackage}.
	 * @param ctx the parse tree
	 */
	void exitRemoveFromPackage(OdpsParser.RemoveFromPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#allowPackage}.
	 * @param ctx the parse tree
	 */
	void enterAllowPackage(OdpsParser.AllowPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#allowPackage}.
	 * @param ctx the parse tree
	 */
	void exitAllowPackage(OdpsParser.AllowPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#disallowPackage}.
	 * @param ctx the parse tree
	 */
	void enterDisallowPackage(OdpsParser.DisallowPackageContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#disallowPackage}.
	 * @param ctx the parse tree
	 */
	void exitDisallowPackage(OdpsParser.DisallowPackageContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#putPolicy}.
	 * @param ctx the parse tree
	 */
	void enterPutPolicy(OdpsParser.PutPolicyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#putPolicy}.
	 * @param ctx the parse tree
	 */
	void exitPutPolicy(OdpsParser.PutPolicyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#getPolicy}.
	 * @param ctx the parse tree
	 */
	void enterGetPolicy(OdpsParser.GetPolicyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#getPolicy}.
	 * @param ctx the parse tree
	 */
	void exitGetPolicy(OdpsParser.GetPolicyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#clearExpiredGrants}.
	 * @param ctx the parse tree
	 */
	void enterClearExpiredGrants(OdpsParser.ClearExpiredGrantsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#clearExpiredGrants}.
	 * @param ctx the parse tree
	 */
	void exitClearExpiredGrants(OdpsParser.ClearExpiredGrantsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#grantLabel}.
	 * @param ctx the parse tree
	 */
	void enterGrantLabel(OdpsParser.GrantLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#grantLabel}.
	 * @param ctx the parse tree
	 */
	void exitGrantLabel(OdpsParser.GrantLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#revokeLabel}.
	 * @param ctx the parse tree
	 */
	void enterRevokeLabel(OdpsParser.RevokeLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#revokeLabel}.
	 * @param ctx the parse tree
	 */
	void exitRevokeLabel(OdpsParser.RevokeLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showLabel}.
	 * @param ctx the parse tree
	 */
	void enterShowLabel(OdpsParser.ShowLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showLabel}.
	 * @param ctx the parse tree
	 */
	void exitShowLabel(OdpsParser.ShowLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#grantSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void enterGrantSuperPrivilege(OdpsParser.GrantSuperPrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#grantSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void exitGrantSuperPrivilege(OdpsParser.GrantSuperPrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#revokeSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void enterRevokeSuperPrivilege(OdpsParser.RevokeSuperPrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#revokeSuperPrivilege}.
	 * @param ctx the parse tree
	 */
	void exitRevokeSuperPrivilege(OdpsParser.RevokeSuperPrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createRoleStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateRoleStatement(OdpsParser.CreateRoleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createRoleStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateRoleStatement(OdpsParser.CreateRoleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropRoleStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropRoleStatement(OdpsParser.DropRoleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropRoleStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropRoleStatement(OdpsParser.DropRoleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#addRoleToProject}.
	 * @param ctx the parse tree
	 */
	void enterAddRoleToProject(OdpsParser.AddRoleToProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#addRoleToProject}.
	 * @param ctx the parse tree
	 */
	void exitAddRoleToProject(OdpsParser.AddRoleToProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#removeRoleFromProject}.
	 * @param ctx the parse tree
	 */
	void enterRemoveRoleFromProject(OdpsParser.RemoveRoleFromProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#removeRoleFromProject}.
	 * @param ctx the parse tree
	 */
	void exitRemoveRoleFromProject(OdpsParser.RemoveRoleFromProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#grantRole}.
	 * @param ctx the parse tree
	 */
	void enterGrantRole(OdpsParser.GrantRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#grantRole}.
	 * @param ctx the parse tree
	 */
	void exitGrantRole(OdpsParser.GrantRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#revokeRole}.
	 * @param ctx the parse tree
	 */
	void enterRevokeRole(OdpsParser.RevokeRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#revokeRole}.
	 * @param ctx the parse tree
	 */
	void exitRevokeRole(OdpsParser.RevokeRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#grantPrivileges}.
	 * @param ctx the parse tree
	 */
	void enterGrantPrivileges(OdpsParser.GrantPrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#grantPrivileges}.
	 * @param ctx the parse tree
	 */
	void exitGrantPrivileges(OdpsParser.GrantPrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilegeProperties}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeProperties(OdpsParser.PrivilegePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilegeProperties}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeProperties(OdpsParser.PrivilegePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilegePropertieKeys}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegePropertieKeys(OdpsParser.PrivilegePropertieKeysContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilegePropertieKeys}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegePropertieKeys(OdpsParser.PrivilegePropertieKeysContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#revokePrivileges}.
	 * @param ctx the parse tree
	 */
	void enterRevokePrivileges(OdpsParser.RevokePrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#revokePrivileges}.
	 * @param ctx the parse tree
	 */
	void exitRevokePrivileges(OdpsParser.RevokePrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#purgePrivileges}.
	 * @param ctx the parse tree
	 */
	void enterPurgePrivileges(OdpsParser.PurgePrivilegesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#purgePrivileges}.
	 * @param ctx the parse tree
	 */
	void exitPurgePrivileges(OdpsParser.PurgePrivilegesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showGrants}.
	 * @param ctx the parse tree
	 */
	void enterShowGrants(OdpsParser.ShowGrantsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showGrants}.
	 * @param ctx the parse tree
	 */
	void exitShowGrants(OdpsParser.ShowGrantsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showRoleGrants}.
	 * @param ctx the parse tree
	 */
	void enterShowRoleGrants(OdpsParser.ShowRoleGrantsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showRoleGrants}.
	 * @param ctx the parse tree
	 */
	void exitShowRoleGrants(OdpsParser.ShowRoleGrantsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showRoles}.
	 * @param ctx the parse tree
	 */
	void enterShowRoles(OdpsParser.ShowRolesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showRoles}.
	 * @param ctx the parse tree
	 */
	void exitShowRoles(OdpsParser.ShowRolesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showRolePrincipals}.
	 * @param ctx the parse tree
	 */
	void enterShowRolePrincipals(OdpsParser.ShowRolePrincipalsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showRolePrincipals}.
	 * @param ctx the parse tree
	 */
	void exitShowRolePrincipals(OdpsParser.ShowRolePrincipalsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#user}.
	 * @param ctx the parse tree
	 */
	void enterUser(OdpsParser.UserContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#user}.
	 * @param ctx the parse tree
	 */
	void exitUser(OdpsParser.UserContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#userRoleComments}.
	 * @param ctx the parse tree
	 */
	void enterUserRoleComments(OdpsParser.UserRoleCommentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#userRoleComments}.
	 * @param ctx the parse tree
	 */
	void exitUserRoleComments(OdpsParser.UserRoleCommentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#accountProvider}.
	 * @param ctx the parse tree
	 */
	void enterAccountProvider(OdpsParser.AccountProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#accountProvider}.
	 * @param ctx the parse tree
	 */
	void exitAccountProvider(OdpsParser.AccountProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#projectName}.
	 * @param ctx the parse tree
	 */
	void enterProjectName(OdpsParser.ProjectNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#projectName}.
	 * @param ctx the parse tree
	 */
	void exitProjectName(OdpsParser.ProjectNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilegeObjectName}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeObjectName(OdpsParser.PrivilegeObjectNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilegeObjectName}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeObjectName(OdpsParser.PrivilegeObjectNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilegeObjectType}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeObjectType(OdpsParser.PrivilegeObjectTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilegeObjectType}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeObjectType(OdpsParser.PrivilegeObjectTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#roleName}.
	 * @param ctx the parse tree
	 */
	void enterRoleName(OdpsParser.RoleNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#roleName}.
	 * @param ctx the parse tree
	 */
	void exitRoleName(OdpsParser.RoleNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#packageName}.
	 * @param ctx the parse tree
	 */
	void enterPackageName(OdpsParser.PackageNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#packageName}.
	 * @param ctx the parse tree
	 */
	void exitPackageName(OdpsParser.PackageNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#packageNameWithProject}.
	 * @param ctx the parse tree
	 */
	void enterPackageNameWithProject(OdpsParser.PackageNameWithProjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#packageNameWithProject}.
	 * @param ctx the parse tree
	 */
	void exitPackageNameWithProject(OdpsParser.PackageNameWithProjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#principalSpecification}.
	 * @param ctx the parse tree
	 */
	void enterPrincipalSpecification(OdpsParser.PrincipalSpecificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#principalSpecification}.
	 * @param ctx the parse tree
	 */
	void exitPrincipalSpecification(OdpsParser.PrincipalSpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#principalName}.
	 * @param ctx the parse tree
	 */
	void enterPrincipalName(OdpsParser.PrincipalNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#principalName}.
	 * @param ctx the parse tree
	 */
	void exitPrincipalName(OdpsParser.PrincipalNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#principalIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterPrincipalIdentifier(OdpsParser.PrincipalIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#principalIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitPrincipalIdentifier(OdpsParser.PrincipalIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilege}.
	 * @param ctx the parse tree
	 */
	void enterPrivilege(OdpsParser.PrivilegeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilege}.
	 * @param ctx the parse tree
	 */
	void exitPrivilege(OdpsParser.PrivilegeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilegeType}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeType(OdpsParser.PrivilegeTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilegeType}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeType(OdpsParser.PrivilegeTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#privilegeObject}.
	 * @param ctx the parse tree
	 */
	void enterPrivilegeObject(OdpsParser.PrivilegeObjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#privilegeObject}.
	 * @param ctx the parse tree
	 */
	void exitPrivilegeObject(OdpsParser.PrivilegeObjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#filePath}.
	 * @param ctx the parse tree
	 */
	void enterFilePath(OdpsParser.FilePathContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#filePath}.
	 * @param ctx the parse tree
	 */
	void exitFilePath(OdpsParser.FilePathContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#policyCondition}.
	 * @param ctx the parse tree
	 */
	void enterPolicyCondition(OdpsParser.PolicyConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#policyCondition}.
	 * @param ctx the parse tree
	 */
	void exitPolicyCondition(OdpsParser.PolicyConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#policyConditionOp}.
	 * @param ctx the parse tree
	 */
	void enterPolicyConditionOp(OdpsParser.PolicyConditionOpContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#policyConditionOp}.
	 * @param ctx the parse tree
	 */
	void exitPolicyConditionOp(OdpsParser.PolicyConditionOpContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#policyKey}.
	 * @param ctx the parse tree
	 */
	void enterPolicyKey(OdpsParser.PolicyKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#policyKey}.
	 * @param ctx the parse tree
	 */
	void exitPolicyKey(OdpsParser.PolicyKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#policyValue}.
	 * @param ctx the parse tree
	 */
	void enterPolicyValue(OdpsParser.PolicyValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#policyValue}.
	 * @param ctx the parse tree
	 */
	void exitPolicyValue(OdpsParser.PolicyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showCurrentRole}.
	 * @param ctx the parse tree
	 */
	void enterShowCurrentRole(OdpsParser.ShowCurrentRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showCurrentRole}.
	 * @param ctx the parse tree
	 */
	void exitShowCurrentRole(OdpsParser.ShowCurrentRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setRole}.
	 * @param ctx the parse tree
	 */
	void enterSetRole(OdpsParser.SetRoleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setRole}.
	 * @param ctx the parse tree
	 */
	void exitSetRole(OdpsParser.SetRoleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#adminOptionFor}.
	 * @param ctx the parse tree
	 */
	void enterAdminOptionFor(OdpsParser.AdminOptionForContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#adminOptionFor}.
	 * @param ctx the parse tree
	 */
	void exitAdminOptionFor(OdpsParser.AdminOptionForContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#withAdminOption}.
	 * @param ctx the parse tree
	 */
	void enterWithAdminOption(OdpsParser.WithAdminOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#withAdminOption}.
	 * @param ctx the parse tree
	 */
	void exitWithAdminOption(OdpsParser.WithAdminOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#withGrantOption}.
	 * @param ctx the parse tree
	 */
	void enterWithGrantOption(OdpsParser.WithGrantOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#withGrantOption}.
	 * @param ctx the parse tree
	 */
	void exitWithGrantOption(OdpsParser.WithGrantOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#grantOptionFor}.
	 * @param ctx the parse tree
	 */
	void enterGrantOptionFor(OdpsParser.GrantOptionForContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#grantOptionFor}.
	 * @param ctx the parse tree
	 */
	void exitGrantOptionFor(OdpsParser.GrantOptionForContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#explainOption}.
	 * @param ctx the parse tree
	 */
	void enterExplainOption(OdpsParser.ExplainOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#explainOption}.
	 * @param ctx the parse tree
	 */
	void exitExplainOption(OdpsParser.ExplainOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#loadStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoadStatement(OdpsParser.LoadStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#loadStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoadStatement(OdpsParser.LoadStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#replicationClause}.
	 * @param ctx the parse tree
	 */
	void enterReplicationClause(OdpsParser.ReplicationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#replicationClause}.
	 * @param ctx the parse tree
	 */
	void exitReplicationClause(OdpsParser.ReplicationClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#exportStatement}.
	 * @param ctx the parse tree
	 */
	void enterExportStatement(OdpsParser.ExportStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#exportStatement}.
	 * @param ctx the parse tree
	 */
	void exitExportStatement(OdpsParser.ExportStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#importStatement}.
	 * @param ctx the parse tree
	 */
	void enterImportStatement(OdpsParser.ImportStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#importStatement}.
	 * @param ctx the parse tree
	 */
	void exitImportStatement(OdpsParser.ImportStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#readStatement}.
	 * @param ctx the parse tree
	 */
	void enterReadStatement(OdpsParser.ReadStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#readStatement}.
	 * @param ctx the parse tree
	 */
	void exitReadStatement(OdpsParser.ReadStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#undoStatement}.
	 * @param ctx the parse tree
	 */
	void enterUndoStatement(OdpsParser.UndoStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#undoStatement}.
	 * @param ctx the parse tree
	 */
	void exitUndoStatement(OdpsParser.UndoStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#redoStatement}.
	 * @param ctx the parse tree
	 */
	void enterRedoStatement(OdpsParser.RedoStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#redoStatement}.
	 * @param ctx the parse tree
	 */
	void exitRedoStatement(OdpsParser.RedoStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#purgeStatement}.
	 * @param ctx the parse tree
	 */
	void enterPurgeStatement(OdpsParser.PurgeStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#purgeStatement}.
	 * @param ctx the parse tree
	 */
	void exitPurgeStatement(OdpsParser.PurgeStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropTableVairableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropTableVairableStatement(OdpsParser.DropTableVairableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropTableVairableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropTableVairableStatement(OdpsParser.DropTableVairableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#msckRepairTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterMsckRepairTableStatement(OdpsParser.MsckRepairTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#msckRepairTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitMsckRepairTableStatement(OdpsParser.MsckRepairTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#ddlStatement}.
	 * @param ctx the parse tree
	 */
	void enterDdlStatement(OdpsParser.DdlStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#ddlStatement}.
	 * @param ctx the parse tree
	 */
	void exitDdlStatement(OdpsParser.DdlStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionSpecOrPartitionId}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpecOrPartitionId(OdpsParser.PartitionSpecOrPartitionIdContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionSpecOrPartitionId}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpecOrPartitionId(OdpsParser.PartitionSpecOrPartitionIdContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableOrTableId}.
	 * @param ctx the parse tree
	 */
	void enterTableOrTableId(OdpsParser.TableOrTableIdContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableOrTableId}.
	 * @param ctx the parse tree
	 */
	void exitTableOrTableId(OdpsParser.TableOrTableIdContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableHistoryStatement}.
	 * @param ctx the parse tree
	 */
	void enterTableHistoryStatement(OdpsParser.TableHistoryStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableHistoryStatement}.
	 * @param ctx the parse tree
	 */
	void exitTableHistoryStatement(OdpsParser.TableHistoryStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setExstore}.
	 * @param ctx the parse tree
	 */
	void enterSetExstore(OdpsParser.SetExstoreContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setExstore}.
	 * @param ctx the parse tree
	 */
	void exitSetExstore(OdpsParser.SetExstoreContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#ifExists}.
	 * @param ctx the parse tree
	 */
	void enterIfExists(OdpsParser.IfExistsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#ifExists}.
	 * @param ctx the parse tree
	 */
	void exitIfExists(OdpsParser.IfExistsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#restrictOrCascade}.
	 * @param ctx the parse tree
	 */
	void enterRestrictOrCascade(OdpsParser.RestrictOrCascadeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#restrictOrCascade}.
	 * @param ctx the parse tree
	 */
	void exitRestrictOrCascade(OdpsParser.RestrictOrCascadeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#ifNotExists}.
	 * @param ctx the parse tree
	 */
	void enterIfNotExists(OdpsParser.IfNotExistsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#ifNotExists}.
	 * @param ctx the parse tree
	 */
	void exitIfNotExists(OdpsParser.IfNotExistsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#rewriteEnabled}.
	 * @param ctx the parse tree
	 */
	void enterRewriteEnabled(OdpsParser.RewriteEnabledContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#rewriteEnabled}.
	 * @param ctx the parse tree
	 */
	void exitRewriteEnabled(OdpsParser.RewriteEnabledContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#rewriteDisabled}.
	 * @param ctx the parse tree
	 */
	void enterRewriteDisabled(OdpsParser.RewriteDisabledContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#rewriteDisabled}.
	 * @param ctx the parse tree
	 */
	void exitRewriteDisabled(OdpsParser.RewriteDisabledContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#storedAsDirs}.
	 * @param ctx the parse tree
	 */
	void enterStoredAsDirs(OdpsParser.StoredAsDirsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#storedAsDirs}.
	 * @param ctx the parse tree
	 */
	void exitStoredAsDirs(OdpsParser.StoredAsDirsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#orReplace}.
	 * @param ctx the parse tree
	 */
	void enterOrReplace(OdpsParser.OrReplaceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#orReplace}.
	 * @param ctx the parse tree
	 */
	void exitOrReplace(OdpsParser.OrReplaceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#ignoreProtection}.
	 * @param ctx the parse tree
	 */
	void enterIgnoreProtection(OdpsParser.IgnoreProtectionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#ignoreProtection}.
	 * @param ctx the parse tree
	 */
	void exitIgnoreProtection(OdpsParser.IgnoreProtectionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateDatabaseStatement(OdpsParser.CreateDatabaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateDatabaseStatement(OdpsParser.CreateDatabaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#schemaName}.
	 * @param ctx the parse tree
	 */
	void enterSchemaName(OdpsParser.SchemaNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#schemaName}.
	 * @param ctx the parse tree
	 */
	void exitSchemaName(OdpsParser.SchemaNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateSchemaStatement(OdpsParser.CreateSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateSchemaStatement(OdpsParser.CreateSchemaStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dbLocation}.
	 * @param ctx the parse tree
	 */
	void enterDbLocation(OdpsParser.DbLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dbLocation}.
	 * @param ctx the parse tree
	 */
	void exitDbLocation(OdpsParser.DbLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dbProperties}.
	 * @param ctx the parse tree
	 */
	void enterDbProperties(OdpsParser.DbPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dbProperties}.
	 * @param ctx the parse tree
	 */
	void exitDbProperties(OdpsParser.DbPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dbPropertiesList}.
	 * @param ctx the parse tree
	 */
	void enterDbPropertiesList(OdpsParser.DbPropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dbPropertiesList}.
	 * @param ctx the parse tree
	 */
	void exitDbPropertiesList(OdpsParser.DbPropertiesListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#switchDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void enterSwitchDatabaseStatement(OdpsParser.SwitchDatabaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#switchDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void exitSwitchDatabaseStatement(OdpsParser.SwitchDatabaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropDatabaseStatement(OdpsParser.DropDatabaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropDatabaseStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropDatabaseStatement(OdpsParser.DropDatabaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropSchemaStatement(OdpsParser.DropSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropSchemaStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropSchemaStatement(OdpsParser.DropSchemaStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#databaseComment}.
	 * @param ctx the parse tree
	 */
	void enterDatabaseComment(OdpsParser.DatabaseCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#databaseComment}.
	 * @param ctx the parse tree
	 */
	void exitDatabaseComment(OdpsParser.DatabaseCommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dataFormatDesc}.
	 * @param ctx the parse tree
	 */
	void enterDataFormatDesc(OdpsParser.DataFormatDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dataFormatDesc}.
	 * @param ctx the parse tree
	 */
	void exitDataFormatDesc(OdpsParser.DataFormatDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableStatement(OdpsParser.CreateTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableStatement(OdpsParser.CreateTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#truncateTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterTruncateTableStatement(OdpsParser.TruncateTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#truncateTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitTruncateTableStatement(OdpsParser.TruncateTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createIndexStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateIndexStatement(OdpsParser.CreateIndexStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createIndexStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateIndexStatement(OdpsParser.CreateIndexStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#indexComment}.
	 * @param ctx the parse tree
	 */
	void enterIndexComment(OdpsParser.IndexCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#indexComment}.
	 * @param ctx the parse tree
	 */
	void exitIndexComment(OdpsParser.IndexCommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#autoRebuild}.
	 * @param ctx the parse tree
	 */
	void enterAutoRebuild(OdpsParser.AutoRebuildContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#autoRebuild}.
	 * @param ctx the parse tree
	 */
	void exitAutoRebuild(OdpsParser.AutoRebuildContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#indexTblName}.
	 * @param ctx the parse tree
	 */
	void enterIndexTblName(OdpsParser.IndexTblNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#indexTblName}.
	 * @param ctx the parse tree
	 */
	void exitIndexTblName(OdpsParser.IndexTblNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#indexPropertiesPrefixed}.
	 * @param ctx the parse tree
	 */
	void enterIndexPropertiesPrefixed(OdpsParser.IndexPropertiesPrefixedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#indexPropertiesPrefixed}.
	 * @param ctx the parse tree
	 */
	void exitIndexPropertiesPrefixed(OdpsParser.IndexPropertiesPrefixedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#indexProperties}.
	 * @param ctx the parse tree
	 */
	void enterIndexProperties(OdpsParser.IndexPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#indexProperties}.
	 * @param ctx the parse tree
	 */
	void exitIndexProperties(OdpsParser.IndexPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#indexPropertiesList}.
	 * @param ctx the parse tree
	 */
	void enterIndexPropertiesList(OdpsParser.IndexPropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#indexPropertiesList}.
	 * @param ctx the parse tree
	 */
	void exitIndexPropertiesList(OdpsParser.IndexPropertiesListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropIndexStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropIndexStatement(OdpsParser.DropIndexStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropIndexStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropIndexStatement(OdpsParser.DropIndexStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropTableStatement(OdpsParser.DropTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropTableStatement(OdpsParser.DropTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatement}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatement(OdpsParser.AlterStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatement}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatement(OdpsParser.AlterStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterSchemaStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterSchemaStatementSuffix(OdpsParser.AlterSchemaStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterSchemaStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterSchemaStatementSuffix(OdpsParser.AlterSchemaStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTableStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableStatementSuffix(OdpsParser.AlterTableStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTableStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableStatementSuffix(OdpsParser.AlterTableStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTableMergePartitionSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableMergePartitionSuffix(OdpsParser.AlterTableMergePartitionSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTableMergePartitionSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableMergePartitionSuffix(OdpsParser.AlterTableMergePartitionSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixAddConstraint}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixAddConstraint(OdpsParser.AlterStatementSuffixAddConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixAddConstraint}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixAddConstraint(OdpsParser.AlterStatementSuffixAddConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTblPartitionStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterTblPartitionStatementSuffix(OdpsParser.AlterTblPartitionStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTblPartitionStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterTblPartitionStatementSuffix(OdpsParser.AlterTblPartitionStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixPartitionLifecycle}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixPartitionLifecycle(OdpsParser.AlterStatementSuffixPartitionLifecycleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixPartitionLifecycle}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixPartitionLifecycle(OdpsParser.AlterStatementSuffixPartitionLifecycleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTblPartitionStatementSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void enterAlterTblPartitionStatementSuffixProperties(OdpsParser.AlterTblPartitionStatementSuffixPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTblPartitionStatementSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void exitAlterTblPartitionStatementSuffixProperties(OdpsParser.AlterTblPartitionStatementSuffixPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementPartitionKeyType}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementPartitionKeyType(OdpsParser.AlterStatementPartitionKeyTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementPartitionKeyType}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementPartitionKeyType(OdpsParser.AlterStatementPartitionKeyTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterViewStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewStatementSuffix(OdpsParser.AlterViewStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterViewStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewStatementSuffix(OdpsParser.AlterViewStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterMaterializedViewStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterMaterializedViewStatementSuffix(OdpsParser.AlterMaterializedViewStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterMaterializedViewStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterMaterializedViewStatementSuffix(OdpsParser.AlterMaterializedViewStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterMaterializedViewSuffixRewrite}.
	 * @param ctx the parse tree
	 */
	void enterAlterMaterializedViewSuffixRewrite(OdpsParser.AlterMaterializedViewSuffixRewriteContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterMaterializedViewSuffixRewrite}.
	 * @param ctx the parse tree
	 */
	void exitAlterMaterializedViewSuffixRewrite(OdpsParser.AlterMaterializedViewSuffixRewriteContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterMaterializedViewSuffixRebuild}.
	 * @param ctx the parse tree
	 */
	void enterAlterMaterializedViewSuffixRebuild(OdpsParser.AlterMaterializedViewSuffixRebuildContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterMaterializedViewSuffixRebuild}.
	 * @param ctx the parse tree
	 */
	void exitAlterMaterializedViewSuffixRebuild(OdpsParser.AlterMaterializedViewSuffixRebuildContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterIndexStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterIndexStatementSuffix(OdpsParser.AlterIndexStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterIndexStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterIndexStatementSuffix(OdpsParser.AlterIndexStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterDatabaseStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterDatabaseStatementSuffix(OdpsParser.AlterDatabaseStatementSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterDatabaseStatementSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterDatabaseStatementSuffix(OdpsParser.AlterDatabaseStatementSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterDatabaseSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void enterAlterDatabaseSuffixProperties(OdpsParser.AlterDatabaseSuffixPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterDatabaseSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void exitAlterDatabaseSuffixProperties(OdpsParser.AlterDatabaseSuffixPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterDatabaseSuffixSetOwner}.
	 * @param ctx the parse tree
	 */
	void enterAlterDatabaseSuffixSetOwner(OdpsParser.AlterDatabaseSuffixSetOwnerContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterDatabaseSuffixSetOwner}.
	 * @param ctx the parse tree
	 */
	void exitAlterDatabaseSuffixSetOwner(OdpsParser.AlterDatabaseSuffixSetOwnerContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixRename}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixRename(OdpsParser.AlterStatementSuffixRenameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixRename}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixRename(OdpsParser.AlterStatementSuffixRenameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixAddCol}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixAddCol(OdpsParser.AlterStatementSuffixAddColContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixAddCol}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixAddCol(OdpsParser.AlterStatementSuffixAddColContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixRenameCol}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixRenameCol(OdpsParser.AlterStatementSuffixRenameColContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixRenameCol}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixRenameCol(OdpsParser.AlterStatementSuffixRenameColContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixDropCol}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixDropCol(OdpsParser.AlterStatementSuffixDropColContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixDropCol}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixDropCol(OdpsParser.AlterStatementSuffixDropColContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixUpdateStatsCol}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixUpdateStatsCol(OdpsParser.AlterStatementSuffixUpdateStatsColContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixUpdateStatsCol}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixUpdateStatsCol(OdpsParser.AlterStatementSuffixUpdateStatsColContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementChangeColPosition}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementChangeColPosition(OdpsParser.AlterStatementChangeColPositionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementChangeColPosition}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementChangeColPosition(OdpsParser.AlterStatementChangeColPositionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixAddPartitions}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixAddPartitions(OdpsParser.AlterStatementSuffixAddPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixAddPartitions}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixAddPartitions(OdpsParser.AlterStatementSuffixAddPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixAddPartitionsElement}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixAddPartitionsElement(OdpsParser.AlterStatementSuffixAddPartitionsElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixAddPartitionsElement}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixAddPartitionsElement(OdpsParser.AlterStatementSuffixAddPartitionsElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixTouch}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixTouch(OdpsParser.AlterStatementSuffixTouchContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixTouch}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixTouch(OdpsParser.AlterStatementSuffixTouchContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixArchive}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixArchive(OdpsParser.AlterStatementSuffixArchiveContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixArchive}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixArchive(OdpsParser.AlterStatementSuffixArchiveContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixUnArchive}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixUnArchive(OdpsParser.AlterStatementSuffixUnArchiveContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixUnArchive}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixUnArchive(OdpsParser.AlterStatementSuffixUnArchiveContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixChangeOwner}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixChangeOwner(OdpsParser.AlterStatementSuffixChangeOwnerContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixChangeOwner}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixChangeOwner(OdpsParser.AlterStatementSuffixChangeOwnerContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionLocation}.
	 * @param ctx the parse tree
	 */
	void enterPartitionLocation(OdpsParser.PartitionLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionLocation}.
	 * @param ctx the parse tree
	 */
	void exitPartitionLocation(OdpsParser.PartitionLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixDropPartitions}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixDropPartitions(OdpsParser.AlterStatementSuffixDropPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixDropPartitions}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixDropPartitions(OdpsParser.AlterStatementSuffixDropPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixProperties(OdpsParser.AlterStatementSuffixPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixProperties(OdpsParser.AlterStatementSuffixPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterViewSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewSuffixProperties(OdpsParser.AlterViewSuffixPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterViewSuffixProperties}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewSuffixProperties(OdpsParser.AlterViewSuffixPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterViewColumnCommentSuffix}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewColumnCommentSuffix(OdpsParser.AlterViewColumnCommentSuffixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterViewColumnCommentSuffix}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewColumnCommentSuffix(OdpsParser.AlterViewColumnCommentSuffixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixSerdeProperties}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixSerdeProperties(OdpsParser.AlterStatementSuffixSerdePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixSerdeProperties}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixSerdeProperties(OdpsParser.AlterStatementSuffixSerdePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tablePartitionPrefix}.
	 * @param ctx the parse tree
	 */
	void enterTablePartitionPrefix(OdpsParser.TablePartitionPrefixContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tablePartitionPrefix}.
	 * @param ctx the parse tree
	 */
	void exitTablePartitionPrefix(OdpsParser.TablePartitionPrefixContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixFileFormat}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixFileFormat(OdpsParser.AlterStatementSuffixFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixFileFormat}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixFileFormat(OdpsParser.AlterStatementSuffixFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixClusterbySortby}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixClusterbySortby(OdpsParser.AlterStatementSuffixClusterbySortbyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixClusterbySortby}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixClusterbySortby(OdpsParser.AlterStatementSuffixClusterbySortbyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterTblPartitionStatementSuffixSkewedLocation}.
	 * @param ctx the parse tree
	 */
	void enterAlterTblPartitionStatementSuffixSkewedLocation(OdpsParser.AlterTblPartitionStatementSuffixSkewedLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterTblPartitionStatementSuffixSkewedLocation}.
	 * @param ctx the parse tree
	 */
	void exitAlterTblPartitionStatementSuffixSkewedLocation(OdpsParser.AlterTblPartitionStatementSuffixSkewedLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedLocations}.
	 * @param ctx the parse tree
	 */
	void enterSkewedLocations(OdpsParser.SkewedLocationsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedLocations}.
	 * @param ctx the parse tree
	 */
	void exitSkewedLocations(OdpsParser.SkewedLocationsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedLocationsList}.
	 * @param ctx the parse tree
	 */
	void enterSkewedLocationsList(OdpsParser.SkewedLocationsListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedLocationsList}.
	 * @param ctx the parse tree
	 */
	void exitSkewedLocationsList(OdpsParser.SkewedLocationsListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedLocationMap}.
	 * @param ctx the parse tree
	 */
	void enterSkewedLocationMap(OdpsParser.SkewedLocationMapContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedLocationMap}.
	 * @param ctx the parse tree
	 */
	void exitSkewedLocationMap(OdpsParser.SkewedLocationMapContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixLocation}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixLocation(OdpsParser.AlterStatementSuffixLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixLocation}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixLocation(OdpsParser.AlterStatementSuffixLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixSkewedby}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixSkewedby(OdpsParser.AlterStatementSuffixSkewedbyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixSkewedby}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixSkewedby(OdpsParser.AlterStatementSuffixSkewedbyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixExchangePartition}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixExchangePartition(OdpsParser.AlterStatementSuffixExchangePartitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixExchangePartition}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixExchangePartition(OdpsParser.AlterStatementSuffixExchangePartitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixProtectMode}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixProtectMode(OdpsParser.AlterStatementSuffixProtectModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixProtectMode}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixProtectMode(OdpsParser.AlterStatementSuffixProtectModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixRenamePart}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixRenamePart(OdpsParser.AlterStatementSuffixRenamePartContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixRenamePart}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixRenamePart(OdpsParser.AlterStatementSuffixRenamePartContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixStatsPart}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixStatsPart(OdpsParser.AlterStatementSuffixStatsPartContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixStatsPart}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixStatsPart(OdpsParser.AlterStatementSuffixStatsPartContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixMergeFiles}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixMergeFiles(OdpsParser.AlterStatementSuffixMergeFilesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixMergeFiles}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixMergeFiles(OdpsParser.AlterStatementSuffixMergeFilesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterProtectMode}.
	 * @param ctx the parse tree
	 */
	void enterAlterProtectMode(OdpsParser.AlterProtectModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterProtectMode}.
	 * @param ctx the parse tree
	 */
	void exitAlterProtectMode(OdpsParser.AlterProtectModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterProtectModeMode}.
	 * @param ctx the parse tree
	 */
	void enterAlterProtectModeMode(OdpsParser.AlterProtectModeModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterProtectModeMode}.
	 * @param ctx the parse tree
	 */
	void exitAlterProtectModeMode(OdpsParser.AlterProtectModeModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixBucketNum}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixBucketNum(OdpsParser.AlterStatementSuffixBucketNumContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixBucketNum}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixBucketNum(OdpsParser.AlterStatementSuffixBucketNumContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#alterStatementSuffixCompact}.
	 * @param ctx the parse tree
	 */
	void enterAlterStatementSuffixCompact(OdpsParser.AlterStatementSuffixCompactContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#alterStatementSuffixCompact}.
	 * @param ctx the parse tree
	 */
	void exitAlterStatementSuffixCompact(OdpsParser.AlterStatementSuffixCompactContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void enterFileFormat(OdpsParser.FileFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void exitFileFormat(OdpsParser.FileFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tabTypeExpr}.
	 * @param ctx the parse tree
	 */
	void enterTabTypeExpr(OdpsParser.TabTypeExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tabTypeExpr}.
	 * @param ctx the parse tree
	 */
	void exitTabTypeExpr(OdpsParser.TabTypeExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partTypeExpr}.
	 * @param ctx the parse tree
	 */
	void enterPartTypeExpr(OdpsParser.PartTypeExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partTypeExpr}.
	 * @param ctx the parse tree
	 */
	void exitPartTypeExpr(OdpsParser.PartTypeExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#descStatement}.
	 * @param ctx the parse tree
	 */
	void enterDescStatement(OdpsParser.DescStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#descStatement}.
	 * @param ctx the parse tree
	 */
	void exitDescStatement(OdpsParser.DescStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#analyzeStatement}.
	 * @param ctx the parse tree
	 */
	void enterAnalyzeStatement(OdpsParser.AnalyzeStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#analyzeStatement}.
	 * @param ctx the parse tree
	 */
	void exitAnalyzeStatement(OdpsParser.AnalyzeStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#forColumnsStatement}.
	 * @param ctx the parse tree
	 */
	void enterForColumnsStatement(OdpsParser.ForColumnsStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#forColumnsStatement}.
	 * @param ctx the parse tree
	 */
	void exitForColumnsStatement(OdpsParser.ForColumnsStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameOrList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrList(OdpsParser.ColumnNameOrListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameOrList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrList(OdpsParser.ColumnNameOrListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showStatement}.
	 * @param ctx the parse tree
	 */
	void enterShowStatement(OdpsParser.ShowStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showStatement}.
	 * @param ctx the parse tree
	 */
	void exitShowStatement(OdpsParser.ShowStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#listStatement}.
	 * @param ctx the parse tree
	 */
	void enterListStatement(OdpsParser.ListStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#listStatement}.
	 * @param ctx the parse tree
	 */
	void exitListStatement(OdpsParser.ListStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#bareDate}.
	 * @param ctx the parse tree
	 */
	void enterBareDate(OdpsParser.BareDateContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#bareDate}.
	 * @param ctx the parse tree
	 */
	void exitBareDate(OdpsParser.BareDateContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#lockStatement}.
	 * @param ctx the parse tree
	 */
	void enterLockStatement(OdpsParser.LockStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#lockStatement}.
	 * @param ctx the parse tree
	 */
	void exitLockStatement(OdpsParser.LockStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#lockDatabase}.
	 * @param ctx the parse tree
	 */
	void enterLockDatabase(OdpsParser.LockDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#lockDatabase}.
	 * @param ctx the parse tree
	 */
	void exitLockDatabase(OdpsParser.LockDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#lockMode}.
	 * @param ctx the parse tree
	 */
	void enterLockMode(OdpsParser.LockModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#lockMode}.
	 * @param ctx the parse tree
	 */
	void exitLockMode(OdpsParser.LockModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#unlockStatement}.
	 * @param ctx the parse tree
	 */
	void enterUnlockStatement(OdpsParser.UnlockStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#unlockStatement}.
	 * @param ctx the parse tree
	 */
	void exitUnlockStatement(OdpsParser.UnlockStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#unlockDatabase}.
	 * @param ctx the parse tree
	 */
	void enterUnlockDatabase(OdpsParser.UnlockDatabaseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#unlockDatabase}.
	 * @param ctx the parse tree
	 */
	void exitUnlockDatabase(OdpsParser.UnlockDatabaseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#resourceList}.
	 * @param ctx the parse tree
	 */
	void enterResourceList(OdpsParser.ResourceListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#resourceList}.
	 * @param ctx the parse tree
	 */
	void exitResourceList(OdpsParser.ResourceListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#resource}.
	 * @param ctx the parse tree
	 */
	void enterResource(OdpsParser.ResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#resource}.
	 * @param ctx the parse tree
	 */
	void exitResource(OdpsParser.ResourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#resourceType}.
	 * @param ctx the parse tree
	 */
	void enterResourceType(OdpsParser.ResourceTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#resourceType}.
	 * @param ctx the parse tree
	 */
	void exitResourceType(OdpsParser.ResourceTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateFunctionStatement(OdpsParser.CreateFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateFunctionStatement(OdpsParser.CreateFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropFunctionStatement(OdpsParser.DropFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropFunctionStatement(OdpsParser.DropFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#reloadFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterReloadFunctionStatement(OdpsParser.ReloadFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#reloadFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitReloadFunctionStatement(OdpsParser.ReloadFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createMacroStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateMacroStatement(OdpsParser.CreateMacroStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createMacroStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateMacroStatement(OdpsParser.CreateMacroStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropMacroStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropMacroStatement(OdpsParser.DropMacroStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropMacroStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropMacroStatement(OdpsParser.DropMacroStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createSqlFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateSqlFunctionStatement(OdpsParser.CreateSqlFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createSqlFunctionStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateSqlFunctionStatement(OdpsParser.CreateSqlFunctionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#cloneTableStatement}.
	 * @param ctx the parse tree
	 */
	void enterCloneTableStatement(OdpsParser.CloneTableStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#cloneTableStatement}.
	 * @param ctx the parse tree
	 */
	void exitCloneTableStatement(OdpsParser.CloneTableStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateViewStatement(OdpsParser.CreateViewStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateViewStatement(OdpsParser.CreateViewStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#viewPartition}.
	 * @param ctx the parse tree
	 */
	void enterViewPartition(OdpsParser.ViewPartitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#viewPartition}.
	 * @param ctx the parse tree
	 */
	void exitViewPartition(OdpsParser.ViewPartitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropViewStatement(OdpsParser.DropViewStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropViewStatement(OdpsParser.DropViewStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#createMaterializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterCreateMaterializedViewStatement(OdpsParser.CreateMaterializedViewStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#createMaterializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitCreateMaterializedViewStatement(OdpsParser.CreateMaterializedViewStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropMaterializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void enterDropMaterializedViewStatement(OdpsParser.DropMaterializedViewStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropMaterializedViewStatement}.
	 * @param ctx the parse tree
	 */
	void exitDropMaterializedViewStatement(OdpsParser.DropMaterializedViewStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showFunctionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterShowFunctionIdentifier(OdpsParser.ShowFunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showFunctionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitShowFunctionIdentifier(OdpsParser.ShowFunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#showStmtIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterShowStmtIdentifier(OdpsParser.ShowStmtIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#showStmtIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitShowStmtIdentifier(OdpsParser.ShowStmtIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableComment}.
	 * @param ctx the parse tree
	 */
	void enterTableComment(OdpsParser.TableCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableComment}.
	 * @param ctx the parse tree
	 */
	void exitTableComment(OdpsParser.TableCommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tablePartition}.
	 * @param ctx the parse tree
	 */
	void enterTablePartition(OdpsParser.TablePartitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tablePartition}.
	 * @param ctx the parse tree
	 */
	void exitTablePartition(OdpsParser.TablePartitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableBuckets}.
	 * @param ctx the parse tree
	 */
	void enterTableBuckets(OdpsParser.TableBucketsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableBuckets}.
	 * @param ctx the parse tree
	 */
	void exitTableBuckets(OdpsParser.TableBucketsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableShards}.
	 * @param ctx the parse tree
	 */
	void enterTableShards(OdpsParser.TableShardsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableShards}.
	 * @param ctx the parse tree
	 */
	void exitTableShards(OdpsParser.TableShardsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableSkewed}.
	 * @param ctx the parse tree
	 */
	void enterTableSkewed(OdpsParser.TableSkewedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableSkewed}.
	 * @param ctx the parse tree
	 */
	void exitTableSkewed(OdpsParser.TableSkewedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void enterRowFormat(OdpsParser.RowFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void exitRowFormat(OdpsParser.RowFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#recordReader}.
	 * @param ctx the parse tree
	 */
	void enterRecordReader(OdpsParser.RecordReaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#recordReader}.
	 * @param ctx the parse tree
	 */
	void exitRecordReader(OdpsParser.RecordReaderContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#recordWriter}.
	 * @param ctx the parse tree
	 */
	void enterRecordWriter(OdpsParser.RecordWriterContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#recordWriter}.
	 * @param ctx the parse tree
	 */
	void exitRecordWriter(OdpsParser.RecordWriterContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#rowFormatSerde}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatSerde(OdpsParser.RowFormatSerdeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#rowFormatSerde}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatSerde(OdpsParser.RowFormatSerdeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#rowFormatDelimited}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatDelimited(OdpsParser.RowFormatDelimitedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#rowFormatDelimited}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatDelimited(OdpsParser.RowFormatDelimitedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableRowFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormat(OdpsParser.TableRowFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableRowFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormat(OdpsParser.TableRowFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tablePropertiesPrefixed}.
	 * @param ctx the parse tree
	 */
	void enterTablePropertiesPrefixed(OdpsParser.TablePropertiesPrefixedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tablePropertiesPrefixed}.
	 * @param ctx the parse tree
	 */
	void exitTablePropertiesPrefixed(OdpsParser.TablePropertiesPrefixedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableProperties}.
	 * @param ctx the parse tree
	 */
	void enterTableProperties(OdpsParser.TablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableProperties}.
	 * @param ctx the parse tree
	 */
	void exitTableProperties(OdpsParser.TablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tablePropertiesList}.
	 * @param ctx the parse tree
	 */
	void enterTablePropertiesList(OdpsParser.TablePropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tablePropertiesList}.
	 * @param ctx the parse tree
	 */
	void exitTablePropertiesList(OdpsParser.TablePropertiesListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#keyValueProperty}.
	 * @param ctx the parse tree
	 */
	void enterKeyValueProperty(OdpsParser.KeyValuePropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#keyValueProperty}.
	 * @param ctx the parse tree
	 */
	void exitKeyValueProperty(OdpsParser.KeyValuePropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#userDefinedJoinPropertiesList}.
	 * @param ctx the parse tree
	 */
	void enterUserDefinedJoinPropertiesList(OdpsParser.UserDefinedJoinPropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#userDefinedJoinPropertiesList}.
	 * @param ctx the parse tree
	 */
	void exitUserDefinedJoinPropertiesList(OdpsParser.UserDefinedJoinPropertiesListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#keyPrivProperty}.
	 * @param ctx the parse tree
	 */
	void enterKeyPrivProperty(OdpsParser.KeyPrivPropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#keyPrivProperty}.
	 * @param ctx the parse tree
	 */
	void exitKeyPrivProperty(OdpsParser.KeyPrivPropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#keyProperty}.
	 * @param ctx the parse tree
	 */
	void enterKeyProperty(OdpsParser.KeyPropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#keyProperty}.
	 * @param ctx the parse tree
	 */
	void exitKeyProperty(OdpsParser.KeyPropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableRowFormatFieldIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatFieldIdentifier(OdpsParser.TableRowFormatFieldIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableRowFormatFieldIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatFieldIdentifier(OdpsParser.TableRowFormatFieldIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableRowFormatCollItemsIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatCollItemsIdentifier(OdpsParser.TableRowFormatCollItemsIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableRowFormatCollItemsIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatCollItemsIdentifier(OdpsParser.TableRowFormatCollItemsIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableRowFormatMapKeysIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatMapKeysIdentifier(OdpsParser.TableRowFormatMapKeysIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableRowFormatMapKeysIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatMapKeysIdentifier(OdpsParser.TableRowFormatMapKeysIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableRowFormatLinesIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableRowFormatLinesIdentifier(OdpsParser.TableRowFormatLinesIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableRowFormatLinesIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableRowFormatLinesIdentifier(OdpsParser.TableRowFormatLinesIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableRowNullFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableRowNullFormat(OdpsParser.TableRowNullFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableRowNullFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableRowNullFormat(OdpsParser.TableRowNullFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableFileFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableFileFormat(OdpsParser.TableFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableFileFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableFileFormat(OdpsParser.TableFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableLocation}.
	 * @param ctx the parse tree
	 */
	void enterTableLocation(OdpsParser.TableLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableLocation}.
	 * @param ctx the parse tree
	 */
	void exitTableLocation(OdpsParser.TableLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#externalTableResource}.
	 * @param ctx the parse tree
	 */
	void enterExternalTableResource(OdpsParser.ExternalTableResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#externalTableResource}.
	 * @param ctx the parse tree
	 */
	void exitExternalTableResource(OdpsParser.ExternalTableResourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#viewResource}.
	 * @param ctx the parse tree
	 */
	void enterViewResource(OdpsParser.ViewResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#viewResource}.
	 * @param ctx the parse tree
	 */
	void exitViewResource(OdpsParser.ViewResourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#outOfLineConstraints}.
	 * @param ctx the parse tree
	 */
	void enterOutOfLineConstraints(OdpsParser.OutOfLineConstraintsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#outOfLineConstraints}.
	 * @param ctx the parse tree
	 */
	void exitOutOfLineConstraints(OdpsParser.OutOfLineConstraintsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#enableSpec}.
	 * @param ctx the parse tree
	 */
	void enterEnableSpec(OdpsParser.EnableSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#enableSpec}.
	 * @param ctx the parse tree
	 */
	void exitEnableSpec(OdpsParser.EnableSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#validateSpec}.
	 * @param ctx the parse tree
	 */
	void enterValidateSpec(OdpsParser.ValidateSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#validateSpec}.
	 * @param ctx the parse tree
	 */
	void exitValidateSpec(OdpsParser.ValidateSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#relySpec}.
	 * @param ctx the parse tree
	 */
	void enterRelySpec(OdpsParser.RelySpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#relySpec}.
	 * @param ctx the parse tree
	 */
	void exitRelySpec(OdpsParser.RelySpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameTypeConstraintList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameTypeConstraintList(OdpsParser.ColumnNameTypeConstraintListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameTypeConstraintList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameTypeConstraintList(OdpsParser.ColumnNameTypeConstraintListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameTypeList(OdpsParser.ColumnNameTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameTypeList(OdpsParser.ColumnNameTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionColumnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void enterPartitionColumnNameTypeList(OdpsParser.PartitionColumnNameTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionColumnNameTypeList}.
	 * @param ctx the parse tree
	 */
	void exitPartitionColumnNameTypeList(OdpsParser.PartitionColumnNameTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameTypeConstraintWithPosList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameTypeConstraintWithPosList(OdpsParser.ColumnNameTypeConstraintWithPosListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameTypeConstraintWithPosList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameTypeConstraintWithPosList(OdpsParser.ColumnNameTypeConstraintWithPosListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameColonTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameColonTypeList(OdpsParser.ColumnNameColonTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameColonTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameColonTypeList(OdpsParser.ColumnNameColonTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameList(OdpsParser.ColumnNameListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameList(OdpsParser.ColumnNameListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameListInParentheses}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameListInParentheses(OdpsParser.ColumnNameListInParenthesesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameListInParentheses}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameListInParentheses(OdpsParser.ColumnNameListInParenthesesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnName}.
	 * @param ctx the parse tree
	 */
	void enterColumnName(OdpsParser.ColumnNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnName}.
	 * @param ctx the parse tree
	 */
	void exitColumnName(OdpsParser.ColumnNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameOrderList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrderList(OdpsParser.ColumnNameOrderListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameOrderList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrderList(OdpsParser.ColumnNameOrderListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#clusterColumnNameOrderList}.
	 * @param ctx the parse tree
	 */
	void enterClusterColumnNameOrderList(OdpsParser.ClusterColumnNameOrderListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#clusterColumnNameOrderList}.
	 * @param ctx the parse tree
	 */
	void exitClusterColumnNameOrderList(OdpsParser.ClusterColumnNameOrderListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedValueElement}.
	 * @param ctx the parse tree
	 */
	void enterSkewedValueElement(OdpsParser.SkewedValueElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedValueElement}.
	 * @param ctx the parse tree
	 */
	void exitSkewedValueElement(OdpsParser.SkewedValueElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedColumnValuePairList}.
	 * @param ctx the parse tree
	 */
	void enterSkewedColumnValuePairList(OdpsParser.SkewedColumnValuePairListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedColumnValuePairList}.
	 * @param ctx the parse tree
	 */
	void exitSkewedColumnValuePairList(OdpsParser.SkewedColumnValuePairListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedColumnValuePair}.
	 * @param ctx the parse tree
	 */
	void enterSkewedColumnValuePair(OdpsParser.SkewedColumnValuePairContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedColumnValuePair}.
	 * @param ctx the parse tree
	 */
	void exitSkewedColumnValuePair(OdpsParser.SkewedColumnValuePairContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedColumnValues}.
	 * @param ctx the parse tree
	 */
	void enterSkewedColumnValues(OdpsParser.SkewedColumnValuesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedColumnValues}.
	 * @param ctx the parse tree
	 */
	void exitSkewedColumnValues(OdpsParser.SkewedColumnValuesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedColumnValue}.
	 * @param ctx the parse tree
	 */
	void enterSkewedColumnValue(OdpsParser.SkewedColumnValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedColumnValue}.
	 * @param ctx the parse tree
	 */
	void exitSkewedColumnValue(OdpsParser.SkewedColumnValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewedValueLocationElement}.
	 * @param ctx the parse tree
	 */
	void enterSkewedValueLocationElement(OdpsParser.SkewedValueLocationElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewedValueLocationElement}.
	 * @param ctx the parse tree
	 */
	void exitSkewedValueLocationElement(OdpsParser.SkewedValueLocationElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameOrder}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrder(OdpsParser.ColumnNameOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameOrder}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrder(OdpsParser.ColumnNameOrderContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameCommentList}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameCommentList(OdpsParser.ColumnNameCommentListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameCommentList}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameCommentList(OdpsParser.ColumnNameCommentListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameComment}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameComment(OdpsParser.ColumnNameCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameComment}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameComment(OdpsParser.ColumnNameCommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnRefOrder}.
	 * @param ctx the parse tree
	 */
	void enterColumnRefOrder(OdpsParser.ColumnRefOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnRefOrder}.
	 * @param ctx the parse tree
	 */
	void exitColumnRefOrder(OdpsParser.ColumnRefOrderContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameTypeConstraint}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameTypeConstraint(OdpsParser.ColumnNameTypeConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameTypeConstraint}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameTypeConstraint(OdpsParser.ColumnNameTypeConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameType}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameType(OdpsParser.ColumnNameTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameType}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameType(OdpsParser.ColumnNameTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionColumnNameType}.
	 * @param ctx the parse tree
	 */
	void enterPartitionColumnNameType(OdpsParser.PartitionColumnNameTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionColumnNameType}.
	 * @param ctx the parse tree
	 */
	void exitPartitionColumnNameType(OdpsParser.PartitionColumnNameTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifier(OdpsParser.MultipartIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifier(OdpsParser.MultipartIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameTypeConstraintWithPos}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameTypeConstraintWithPos(OdpsParser.ColumnNameTypeConstraintWithPosContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameTypeConstraintWithPos}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameTypeConstraintWithPos(OdpsParser.ColumnNameTypeConstraintWithPosContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#constraints}.
	 * @param ctx the parse tree
	 */
	void enterConstraints(OdpsParser.ConstraintsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#constraints}.
	 * @param ctx the parse tree
	 */
	void exitConstraints(OdpsParser.ConstraintsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#primaryKey}.
	 * @param ctx the parse tree
	 */
	void enterPrimaryKey(OdpsParser.PrimaryKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#primaryKey}.
	 * @param ctx the parse tree
	 */
	void exitPrimaryKey(OdpsParser.PrimaryKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#nullableSpec}.
	 * @param ctx the parse tree
	 */
	void enterNullableSpec(OdpsParser.NullableSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#nullableSpec}.
	 * @param ctx the parse tree
	 */
	void exitNullableSpec(OdpsParser.NullableSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#defaultValue}.
	 * @param ctx the parse tree
	 */
	void enterDefaultValue(OdpsParser.DefaultValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#defaultValue}.
	 * @param ctx the parse tree
	 */
	void exitDefaultValue(OdpsParser.DefaultValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameColonType}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameColonType(OdpsParser.ColumnNameColonTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameColonType}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameColonType(OdpsParser.ColumnNameColonTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#colType}.
	 * @param ctx the parse tree
	 */
	void enterColType(OdpsParser.ColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#colType}.
	 * @param ctx the parse tree
	 */
	void exitColType(OdpsParser.ColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColTypeList(OdpsParser.ColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColTypeList(OdpsParser.ColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#anyType}.
	 * @param ctx the parse tree
	 */
	void enterAnyType(OdpsParser.AnyTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#anyType}.
	 * @param ctx the parse tree
	 */
	void exitAnyType(OdpsParser.AnyTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#anyTypeList}.
	 * @param ctx the parse tree
	 */
	void enterAnyTypeList(OdpsParser.AnyTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#anyTypeList}.
	 * @param ctx the parse tree
	 */
	void exitAnyTypeList(OdpsParser.AnyTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableTypeInfo}.
	 * @param ctx the parse tree
	 */
	void enterTableTypeInfo(OdpsParser.TableTypeInfoContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableTypeInfo}.
	 * @param ctx the parse tree
	 */
	void exitTableTypeInfo(OdpsParser.TableTypeInfoContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#type}.
	 * @param ctx the parse tree
	 */
	void enterType(OdpsParser.TypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#type}.
	 * @param ctx the parse tree
	 */
	void exitType(OdpsParser.TypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#primitiveType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveType(OdpsParser.PrimitiveTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#primitiveType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveType(OdpsParser.PrimitiveTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#builtinTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void enterBuiltinTypeOrUdt(OdpsParser.BuiltinTypeOrUdtContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#builtinTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void exitBuiltinTypeOrUdt(OdpsParser.BuiltinTypeOrUdtContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#primitiveTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveTypeOrUdt(OdpsParser.PrimitiveTypeOrUdtContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#primitiveTypeOrUdt}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveTypeOrUdt(OdpsParser.PrimitiveTypeOrUdtContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#listType}.
	 * @param ctx the parse tree
	 */
	void enterListType(OdpsParser.ListTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#listType}.
	 * @param ctx the parse tree
	 */
	void exitListType(OdpsParser.ListTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#structType}.
	 * @param ctx the parse tree
	 */
	void enterStructType(OdpsParser.StructTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#structType}.
	 * @param ctx the parse tree
	 */
	void exitStructType(OdpsParser.StructTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mapType}.
	 * @param ctx the parse tree
	 */
	void enterMapType(OdpsParser.MapTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mapType}.
	 * @param ctx the parse tree
	 */
	void exitMapType(OdpsParser.MapTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#unionType}.
	 * @param ctx the parse tree
	 */
	void enterUnionType(OdpsParser.UnionTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#unionType}.
	 * @param ctx the parse tree
	 */
	void exitUnionType(OdpsParser.UnionTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setOperator}.
	 * @param ctx the parse tree
	 */
	void enterSetOperator(OdpsParser.SetOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setOperator}.
	 * @param ctx the parse tree
	 */
	void exitSetOperator(OdpsParser.SetOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#withClause}.
	 * @param ctx the parse tree
	 */
	void enterWithClause(OdpsParser.WithClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#withClause}.
	 * @param ctx the parse tree
	 */
	void exitWithClause(OdpsParser.WithClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#insertClause}.
	 * @param ctx the parse tree
	 */
	void enterInsertClause(OdpsParser.InsertClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#insertClause}.
	 * @param ctx the parse tree
	 */
	void exitInsertClause(OdpsParser.InsertClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#destination}.
	 * @param ctx the parse tree
	 */
	void enterDestination(OdpsParser.DestinationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#destination}.
	 * @param ctx the parse tree
	 */
	void exitDestination(OdpsParser.DestinationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#deleteStatement}.
	 * @param ctx the parse tree
	 */
	void enterDeleteStatement(OdpsParser.DeleteStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#deleteStatement}.
	 * @param ctx the parse tree
	 */
	void exitDeleteStatement(OdpsParser.DeleteStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnAssignmentClause}.
	 * @param ctx the parse tree
	 */
	void enterColumnAssignmentClause(OdpsParser.ColumnAssignmentClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnAssignmentClause}.
	 * @param ctx the parse tree
	 */
	void exitColumnAssignmentClause(OdpsParser.ColumnAssignmentClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#setColumnsClause}.
	 * @param ctx the parse tree
	 */
	void enterSetColumnsClause(OdpsParser.SetColumnsClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#setColumnsClause}.
	 * @param ctx the parse tree
	 */
	void exitSetColumnsClause(OdpsParser.SetColumnsClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#updateStatement}.
	 * @param ctx the parse tree
	 */
	void enterUpdateStatement(OdpsParser.UpdateStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#updateStatement}.
	 * @param ctx the parse tree
	 */
	void exitUpdateStatement(OdpsParser.UpdateStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeStatement}.
	 * @param ctx the parse tree
	 */
	void enterMergeStatement(OdpsParser.MergeStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeStatement}.
	 * @param ctx the parse tree
	 */
	void exitMergeStatement(OdpsParser.MergeStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeTargetTable}.
	 * @param ctx the parse tree
	 */
	void enterMergeTargetTable(OdpsParser.MergeTargetTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeTargetTable}.
	 * @param ctx the parse tree
	 */
	void exitMergeTargetTable(OdpsParser.MergeTargetTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeSourceTable}.
	 * @param ctx the parse tree
	 */
	void enterMergeSourceTable(OdpsParser.MergeSourceTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeSourceTable}.
	 * @param ctx the parse tree
	 */
	void exitMergeSourceTable(OdpsParser.MergeSourceTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeAction}.
	 * @param ctx the parse tree
	 */
	void enterMergeAction(OdpsParser.MergeActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeAction}.
	 * @param ctx the parse tree
	 */
	void exitMergeAction(OdpsParser.MergeActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeValuesCaluse}.
	 * @param ctx the parse tree
	 */
	void enterMergeValuesCaluse(OdpsParser.MergeValuesCaluseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeValuesCaluse}.
	 * @param ctx the parse tree
	 */
	void exitMergeValuesCaluse(OdpsParser.MergeValuesCaluseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeSetColumnsClause}.
	 * @param ctx the parse tree
	 */
	void enterMergeSetColumnsClause(OdpsParser.MergeSetColumnsClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeSetColumnsClause}.
	 * @param ctx the parse tree
	 */
	void exitMergeSetColumnsClause(OdpsParser.MergeSetColumnsClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mergeColumnAssignmentClause}.
	 * @param ctx the parse tree
	 */
	void enterMergeColumnAssignmentClause(OdpsParser.MergeColumnAssignmentClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mergeColumnAssignmentClause}.
	 * @param ctx the parse tree
	 */
	void exitMergeColumnAssignmentClause(OdpsParser.MergeColumnAssignmentClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(OdpsParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(OdpsParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectList}.
	 * @param ctx the parse tree
	 */
	void enterSelectList(OdpsParser.SelectListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectList}.
	 * @param ctx the parse tree
	 */
	void exitSelectList(OdpsParser.SelectListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectTrfmClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectTrfmClause(OdpsParser.SelectTrfmClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectTrfmClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectTrfmClause(OdpsParser.SelectTrfmClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#hintClause}.
	 * @param ctx the parse tree
	 */
	void enterHintClause(OdpsParser.HintClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#hintClause}.
	 * @param ctx the parse tree
	 */
	void exitHintClause(OdpsParser.HintClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#hintList}.
	 * @param ctx the parse tree
	 */
	void enterHintList(OdpsParser.HintListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#hintList}.
	 * @param ctx the parse tree
	 */
	void exitHintList(OdpsParser.HintListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#hintItem}.
	 * @param ctx the parse tree
	 */
	void enterHintItem(OdpsParser.HintItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#hintItem}.
	 * @param ctx the parse tree
	 */
	void exitHintItem(OdpsParser.HintItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dynamicfilterHint}.
	 * @param ctx the parse tree
	 */
	void enterDynamicfilterHint(OdpsParser.DynamicfilterHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dynamicfilterHint}.
	 * @param ctx the parse tree
	 */
	void exitDynamicfilterHint(OdpsParser.DynamicfilterHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#mapJoinHint}.
	 * @param ctx the parse tree
	 */
	void enterMapJoinHint(OdpsParser.MapJoinHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#mapJoinHint}.
	 * @param ctx the parse tree
	 */
	void exitMapJoinHint(OdpsParser.MapJoinHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewJoinHint}.
	 * @param ctx the parse tree
	 */
	void enterSkewJoinHint(OdpsParser.SkewJoinHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewJoinHint}.
	 * @param ctx the parse tree
	 */
	void exitSkewJoinHint(OdpsParser.SkewJoinHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectivityHint}.
	 * @param ctx the parse tree
	 */
	void enterSelectivityHint(OdpsParser.SelectivityHintContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectivityHint}.
	 * @param ctx the parse tree
	 */
	void exitSelectivityHint(OdpsParser.SelectivityHintContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#multipleSkewHintArgs}.
	 * @param ctx the parse tree
	 */
	void enterMultipleSkewHintArgs(OdpsParser.MultipleSkewHintArgsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#multipleSkewHintArgs}.
	 * @param ctx the parse tree
	 */
	void exitMultipleSkewHintArgs(OdpsParser.MultipleSkewHintArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewJoinHintArgs}.
	 * @param ctx the parse tree
	 */
	void enterSkewJoinHintArgs(OdpsParser.SkewJoinHintArgsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewJoinHintArgs}.
	 * @param ctx the parse tree
	 */
	void exitSkewJoinHintArgs(OdpsParser.SkewJoinHintArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewColumns}.
	 * @param ctx the parse tree
	 */
	void enterSkewColumns(OdpsParser.SkewColumnsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewColumns}.
	 * @param ctx the parse tree
	 */
	void exitSkewColumns(OdpsParser.SkewColumnsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#skewJoinHintKeyValues}.
	 * @param ctx the parse tree
	 */
	void enterSkewJoinHintKeyValues(OdpsParser.SkewJoinHintKeyValuesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#skewJoinHintKeyValues}.
	 * @param ctx the parse tree
	 */
	void exitSkewJoinHintKeyValues(OdpsParser.SkewJoinHintKeyValuesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#hintName}.
	 * @param ctx the parse tree
	 */
	void enterHintName(OdpsParser.HintNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#hintName}.
	 * @param ctx the parse tree
	 */
	void exitHintName(OdpsParser.HintNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#hintArgs}.
	 * @param ctx the parse tree
	 */
	void enterHintArgs(OdpsParser.HintArgsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#hintArgs}.
	 * @param ctx the parse tree
	 */
	void exitHintArgs(OdpsParser.HintArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#hintArgName}.
	 * @param ctx the parse tree
	 */
	void enterHintArgName(OdpsParser.HintArgNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#hintArgName}.
	 * @param ctx the parse tree
	 */
	void exitHintArgName(OdpsParser.HintArgNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void enterSelectItem(OdpsParser.SelectItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectItem}.
	 * @param ctx the parse tree
	 */
	void exitSelectItem(OdpsParser.SelectItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#trfmClause}.
	 * @param ctx the parse tree
	 */
	void enterTrfmClause(OdpsParser.TrfmClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#trfmClause}.
	 * @param ctx the parse tree
	 */
	void exitTrfmClause(OdpsParser.TrfmClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectExpression}.
	 * @param ctx the parse tree
	 */
	void enterSelectExpression(OdpsParser.SelectExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectExpression}.
	 * @param ctx the parse tree
	 */
	void exitSelectExpression(OdpsParser.SelectExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#selectExpressionList}.
	 * @param ctx the parse tree
	 */
	void enterSelectExpressionList(OdpsParser.SelectExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#selectExpressionList}.
	 * @param ctx the parse tree
	 */
	void exitSelectExpressionList(OdpsParser.SelectExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#window_clause}.
	 * @param ctx the parse tree
	 */
	void enterWindow_clause(OdpsParser.Window_clauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#window_clause}.
	 * @param ctx the parse tree
	 */
	void exitWindow_clause(OdpsParser.Window_clauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#window_defn}.
	 * @param ctx the parse tree
	 */
	void enterWindow_defn(OdpsParser.Window_defnContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#window_defn}.
	 * @param ctx the parse tree
	 */
	void exitWindow_defn(OdpsParser.Window_defnContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#window_specification}.
	 * @param ctx the parse tree
	 */
	void enterWindow_specification(OdpsParser.Window_specificationContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#window_specification}.
	 * @param ctx the parse tree
	 */
	void exitWindow_specification(OdpsParser.Window_specificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#window_frame}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame(OdpsParser.Window_frameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#window_frame}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame(OdpsParser.Window_frameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#frame_exclusion}.
	 * @param ctx the parse tree
	 */
	void enterFrame_exclusion(OdpsParser.Frame_exclusionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#frame_exclusion}.
	 * @param ctx the parse tree
	 */
	void exitFrame_exclusion(OdpsParser.Frame_exclusionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#window_frame_start_boundary}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_start_boundary(OdpsParser.Window_frame_start_boundaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#window_frame_start_boundary}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_start_boundary(OdpsParser.Window_frame_start_boundaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#window_frame_boundary}.
	 * @param ctx the parse tree
	 */
	void enterWindow_frame_boundary(OdpsParser.Window_frame_boundaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#window_frame_boundary}.
	 * @param ctx the parse tree
	 */
	void exitWindow_frame_boundary(OdpsParser.Window_frame_boundaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableAllColumns}.
	 * @param ctx the parse tree
	 */
	void enterTableAllColumns(OdpsParser.TableAllColumnsContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableAllColumns}.
	 * @param ctx the parse tree
	 */
	void exitTableAllColumns(OdpsParser.TableAllColumnsContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableOrColumn}.
	 * @param ctx the parse tree
	 */
	void enterTableOrColumn(OdpsParser.TableOrColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableOrColumn}.
	 * @param ctx the parse tree
	 */
	void exitTableOrColumn(OdpsParser.TableOrColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableAndColumnRef}.
	 * @param ctx the parse tree
	 */
	void enterTableAndColumnRef(OdpsParser.TableAndColumnRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableAndColumnRef}.
	 * @param ctx the parse tree
	 */
	void exitTableAndColumnRef(OdpsParser.TableAndColumnRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#expressionList}.
	 * @param ctx the parse tree
	 */
	void enterExpressionList(OdpsParser.ExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#expressionList}.
	 * @param ctx the parse tree
	 */
	void exitExpressionList(OdpsParser.ExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#aliasList}.
	 * @param ctx the parse tree
	 */
	void enterAliasList(OdpsParser.AliasListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#aliasList}.
	 * @param ctx the parse tree
	 */
	void exitAliasList(OdpsParser.AliasListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(OdpsParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(OdpsParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#joinSource}.
	 * @param ctx the parse tree
	 */
	void enterJoinSource(OdpsParser.JoinSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#joinSource}.
	 * @param ctx the parse tree
	 */
	void exitJoinSource(OdpsParser.JoinSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#joinRHS}.
	 * @param ctx the parse tree
	 */
	void enterJoinRHS(OdpsParser.JoinRHSContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#joinRHS}.
	 * @param ctx the parse tree
	 */
	void exitJoinRHS(OdpsParser.JoinRHSContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#uniqueJoinSource}.
	 * @param ctx the parse tree
	 */
	void enterUniqueJoinSource(OdpsParser.UniqueJoinSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#uniqueJoinSource}.
	 * @param ctx the parse tree
	 */
	void exitUniqueJoinSource(OdpsParser.UniqueJoinSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#uniqueJoinExpr}.
	 * @param ctx the parse tree
	 */
	void enterUniqueJoinExpr(OdpsParser.UniqueJoinExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#uniqueJoinExpr}.
	 * @param ctx the parse tree
	 */
	void exitUniqueJoinExpr(OdpsParser.UniqueJoinExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#uniqueJoinToken}.
	 * @param ctx the parse tree
	 */
	void enterUniqueJoinToken(OdpsParser.UniqueJoinTokenContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#uniqueJoinToken}.
	 * @param ctx the parse tree
	 */
	void exitUniqueJoinToken(OdpsParser.UniqueJoinTokenContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#joinToken}.
	 * @param ctx the parse tree
	 */
	void enterJoinToken(OdpsParser.JoinTokenContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#joinToken}.
	 * @param ctx the parse tree
	 */
	void exitJoinToken(OdpsParser.JoinTokenContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void enterLateralView(OdpsParser.LateralViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void exitLateralView(OdpsParser.LateralViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void enterTableAlias(OdpsParser.TableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void exitTableAlias(OdpsParser.TableAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableBucketSample}.
	 * @param ctx the parse tree
	 */
	void enterTableBucketSample(OdpsParser.TableBucketSampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableBucketSample}.
	 * @param ctx the parse tree
	 */
	void exitTableBucketSample(OdpsParser.TableBucketSampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#splitSample}.
	 * @param ctx the parse tree
	 */
	void enterSplitSample(OdpsParser.SplitSampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#splitSample}.
	 * @param ctx the parse tree
	 */
	void exitSplitSample(OdpsParser.SplitSampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableSample}.
	 * @param ctx the parse tree
	 */
	void enterTableSample(OdpsParser.TableSampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableSample}.
	 * @param ctx the parse tree
	 */
	void exitTableSample(OdpsParser.TableSampleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableSource}.
	 * @param ctx the parse tree
	 */
	void enterTableSource(OdpsParser.TableSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableSource}.
	 * @param ctx the parse tree
	 */
	void exitTableSource(OdpsParser.TableSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#availableSql11KeywordsForOdpsTableAlias}.
	 * @param ctx the parse tree
	 */
	void enterAvailableSql11KeywordsForOdpsTableAlias(OdpsParser.AvailableSql11KeywordsForOdpsTableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#availableSql11KeywordsForOdpsTableAlias}.
	 * @param ctx the parse tree
	 */
	void exitAvailableSql11KeywordsForOdpsTableAlias(OdpsParser.AvailableSql11KeywordsForOdpsTableAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableName}.
	 * @param ctx the parse tree
	 */
	void enterTableName(OdpsParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableName}.
	 * @param ctx the parse tree
	 */
	void exitTableName(OdpsParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitioningSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitioningSpec(OdpsParser.PartitioningSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitioningSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitioningSpec(OdpsParser.PartitioningSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionTableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void enterPartitionTableFunctionSource(OdpsParser.PartitionTableFunctionSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionTableFunctionSource}.
	 * @param ctx the parse tree
	 */
	void exitPartitionTableFunctionSource(OdpsParser.PartitionTableFunctionSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionedTableFunction}.
	 * @param ctx the parse tree
	 */
	void enterPartitionedTableFunction(OdpsParser.PartitionedTableFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionedTableFunction}.
	 * @param ctx the parse tree
	 */
	void exitPartitionedTableFunction(OdpsParser.PartitionedTableFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(OdpsParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(OdpsParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#valueRowConstructor}.
	 * @param ctx the parse tree
	 */
	void enterValueRowConstructor(OdpsParser.ValueRowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#valueRowConstructor}.
	 * @param ctx the parse tree
	 */
	void exitValueRowConstructor(OdpsParser.ValueRowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#valuesTableConstructor}.
	 * @param ctx the parse tree
	 */
	void enterValuesTableConstructor(OdpsParser.ValuesTableConstructorContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#valuesTableConstructor}.
	 * @param ctx the parse tree
	 */
	void exitValuesTableConstructor(OdpsParser.ValuesTableConstructorContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#valuesClause}.
	 * @param ctx the parse tree
	 */
	void enterValuesClause(OdpsParser.ValuesClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#valuesClause}.
	 * @param ctx the parse tree
	 */
	void exitValuesClause(OdpsParser.ValuesClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#virtualTableSource}.
	 * @param ctx the parse tree
	 */
	void enterVirtualTableSource(OdpsParser.VirtualTableSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#virtualTableSource}.
	 * @param ctx the parse tree
	 */
	void exitVirtualTableSource(OdpsParser.VirtualTableSourceContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableNameColList}.
	 * @param ctx the parse tree
	 */
	void enterTableNameColList(OdpsParser.TableNameColListContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableNameColList}.
	 * @param ctx the parse tree
	 */
	void exitTableNameColList(OdpsParser.TableNameColListContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionTypeCubeOrRollup}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTypeCubeOrRollup(OdpsParser.FunctionTypeCubeOrRollupContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionTypeCubeOrRollup}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTypeCubeOrRollup(OdpsParser.FunctionTypeCubeOrRollupContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupingSetsItem}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetsItem(OdpsParser.GroupingSetsItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupingSetsItem}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetsItem(OdpsParser.GroupingSetsItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupingSetsClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetsClause(OdpsParser.GroupingSetsClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupingSetsClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetsClause(OdpsParser.GroupingSetsClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupByKey}.
	 * @param ctx the parse tree
	 */
	void enterGroupByKey(OdpsParser.GroupByKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupByKey}.
	 * @param ctx the parse tree
	 */
	void exitGroupByKey(OdpsParser.GroupByKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupByClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupByClause(OdpsParser.GroupByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupByClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupByClause(OdpsParser.GroupByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupingSetExpression}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetExpression(OdpsParser.GroupingSetExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupingSetExpression}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetExpression(OdpsParser.GroupingSetExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupingSetExpressionMultiple}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSetExpressionMultiple(OdpsParser.GroupingSetExpressionMultipleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupingSetExpressionMultiple}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSetExpressionMultiple(OdpsParser.GroupingSetExpressionMultipleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#groupingExpressionSingle}.
	 * @param ctx the parse tree
	 */
	void enterGroupingExpressionSingle(OdpsParser.GroupingExpressionSingleContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#groupingExpressionSingle}.
	 * @param ctx the parse tree
	 */
	void exitGroupingExpressionSingle(OdpsParser.GroupingExpressionSingleContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(OdpsParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(OdpsParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#havingCondition}.
	 * @param ctx the parse tree
	 */
	void enterHavingCondition(OdpsParser.HavingConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#havingCondition}.
	 * @param ctx the parse tree
	 */
	void exitHavingCondition(OdpsParser.HavingConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#expressionsInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterExpressionsInParenthese(OdpsParser.ExpressionsInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#expressionsInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitExpressionsInParenthese(OdpsParser.ExpressionsInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#expressionsNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterExpressionsNotInParenthese(OdpsParser.ExpressionsNotInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#expressionsNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitExpressionsNotInParenthese(OdpsParser.ExpressionsNotInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnRefOrderInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnRefOrderInParenthese(OdpsParser.ColumnRefOrderInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnRefOrderInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnRefOrderInParenthese(OdpsParser.ColumnRefOrderInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnRefOrderNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnRefOrderNotInParenthese(OdpsParser.ColumnRefOrderNotInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnRefOrderNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnRefOrderNotInParenthese(OdpsParser.ColumnRefOrderNotInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void enterOrderByClause(OdpsParser.OrderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#orderByClause}.
	 * @param ctx the parse tree
	 */
	void exitOrderByClause(OdpsParser.OrderByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameOrIndexInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrIndexInParenthese(OdpsParser.ColumnNameOrIndexInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameOrIndexInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrIndexInParenthese(OdpsParser.ColumnNameOrIndexInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameOrIndexNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrIndexNotInParenthese(OdpsParser.ColumnNameOrIndexNotInParentheseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameOrIndexNotInParenthese}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrIndexNotInParenthese(OdpsParser.ColumnNameOrIndexNotInParentheseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#columnNameOrIndex}.
	 * @param ctx the parse tree
	 */
	void enterColumnNameOrIndex(OdpsParser.ColumnNameOrIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#columnNameOrIndex}.
	 * @param ctx the parse tree
	 */
	void exitColumnNameOrIndex(OdpsParser.ColumnNameOrIndexContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#zorderByClause}.
	 * @param ctx the parse tree
	 */
	void enterZorderByClause(OdpsParser.ZorderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#zorderByClause}.
	 * @param ctx the parse tree
	 */
	void exitZorderByClause(OdpsParser.ZorderByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#clusterByClause}.
	 * @param ctx the parse tree
	 */
	void enterClusterByClause(OdpsParser.ClusterByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#clusterByClause}.
	 * @param ctx the parse tree
	 */
	void exitClusterByClause(OdpsParser.ClusterByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionByClause}.
	 * @param ctx the parse tree
	 */
	void enterPartitionByClause(OdpsParser.PartitionByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionByClause}.
	 * @param ctx the parse tree
	 */
	void exitPartitionByClause(OdpsParser.PartitionByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#distributeByClause}.
	 * @param ctx the parse tree
	 */
	void enterDistributeByClause(OdpsParser.DistributeByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#distributeByClause}.
	 * @param ctx the parse tree
	 */
	void exitDistributeByClause(OdpsParser.DistributeByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#sortByClause}.
	 * @param ctx the parse tree
	 */
	void enterSortByClause(OdpsParser.SortByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#sortByClause}.
	 * @param ctx the parse tree
	 */
	void exitSortByClause(OdpsParser.SortByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#function}.
	 * @param ctx the parse tree
	 */
	void enterFunction(OdpsParser.FunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#function}.
	 * @param ctx the parse tree
	 */
	void exitFunction(OdpsParser.FunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionArgument(OdpsParser.FunctionArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionArgument(OdpsParser.FunctionArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#builtinFunctionStructure}.
	 * @param ctx the parse tree
	 */
	void enterBuiltinFunctionStructure(OdpsParser.BuiltinFunctionStructureContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#builtinFunctionStructure}.
	 * @param ctx the parse tree
	 */
	void exitBuiltinFunctionStructure(OdpsParser.BuiltinFunctionStructureContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionName}.
	 * @param ctx the parse tree
	 */
	void enterFunctionName(OdpsParser.FunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionName}.
	 * @param ctx the parse tree
	 */
	void exitFunctionName(OdpsParser.FunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#castExpression}.
	 * @param ctx the parse tree
	 */
	void enterCastExpression(OdpsParser.CastExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#castExpression}.
	 * @param ctx the parse tree
	 */
	void exitCastExpression(OdpsParser.CastExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#caseExpression}.
	 * @param ctx the parse tree
	 */
	void enterCaseExpression(OdpsParser.CaseExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#caseExpression}.
	 * @param ctx the parse tree
	 */
	void exitCaseExpression(OdpsParser.CaseExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#whenExpression}.
	 * @param ctx the parse tree
	 */
	void enterWhenExpression(OdpsParser.WhenExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#whenExpression}.
	 * @param ctx the parse tree
	 */
	void exitWhenExpression(OdpsParser.WhenExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterConstant(OdpsParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitConstant(OdpsParser.ConstantContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#simpleStringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterSimpleStringLiteral(OdpsParser.SimpleStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#simpleStringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitSimpleStringLiteral(OdpsParser.SimpleStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(OdpsParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(OdpsParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#doubleQuoteStringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDoubleQuoteStringLiteral(OdpsParser.DoubleQuoteStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#doubleQuoteStringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDoubleQuoteStringLiteral(OdpsParser.DoubleQuoteStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#charSetStringLiteral}.
	 * @param ctx the parse tree
	 */
	void enterCharSetStringLiteral(OdpsParser.CharSetStringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#charSetStringLiteral}.
	 * @param ctx the parse tree
	 */
	void exitCharSetStringLiteral(OdpsParser.CharSetStringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dateLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDateLiteral(OdpsParser.DateLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dateLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDateLiteral(OdpsParser.DateLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dateTimeLiteral}.
	 * @param ctx the parse tree
	 */
	void enterDateTimeLiteral(OdpsParser.DateTimeLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dateTimeLiteral}.
	 * @param ctx the parse tree
	 */
	void exitDateTimeLiteral(OdpsParser.DateTimeLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#timestampLiteral}.
	 * @param ctx the parse tree
	 */
	void enterTimestampLiteral(OdpsParser.TimestampLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#timestampLiteral}.
	 * @param ctx the parse tree
	 */
	void exitTimestampLiteral(OdpsParser.TimestampLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#intervalLiteral}.
	 * @param ctx the parse tree
	 */
	void enterIntervalLiteral(OdpsParser.IntervalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#intervalLiteral}.
	 * @param ctx the parse tree
	 */
	void exitIntervalLiteral(OdpsParser.IntervalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#intervalQualifiers}.
	 * @param ctx the parse tree
	 */
	void enterIntervalQualifiers(OdpsParser.IntervalQualifiersContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#intervalQualifiers}.
	 * @param ctx the parse tree
	 */
	void exitIntervalQualifiers(OdpsParser.IntervalQualifiersContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#intervalQualifiersUnit}.
	 * @param ctx the parse tree
	 */
	void enterIntervalQualifiersUnit(OdpsParser.IntervalQualifiersUnitContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#intervalQualifiersUnit}.
	 * @param ctx the parse tree
	 */
	void exitIntervalQualifiersUnit(OdpsParser.IntervalQualifiersUnitContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#intervalQualifierPrecision}.
	 * @param ctx the parse tree
	 */
	void enterIntervalQualifierPrecision(OdpsParser.IntervalQualifierPrecisionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#intervalQualifierPrecision}.
	 * @param ctx the parse tree
	 */
	void exitIntervalQualifierPrecision(OdpsParser.IntervalQualifierPrecisionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(OdpsParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(OdpsParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#tableOrPartition}.
	 * @param ctx the parse tree
	 */
	void enterTableOrPartition(OdpsParser.TableOrPartitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#tableOrPartition}.
	 * @param ctx the parse tree
	 */
	void exitTableOrPartition(OdpsParser.TableOrPartitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpec(OdpsParser.PartitionSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpec(OdpsParser.PartitionSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void enterPartitionVal(OdpsParser.PartitionValContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void exitPartitionVal(OdpsParser.PartitionValContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dateWithoutQuote}.
	 * @param ctx the parse tree
	 */
	void enterDateWithoutQuote(OdpsParser.DateWithoutQuoteContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dateWithoutQuote}.
	 * @param ctx the parse tree
	 */
	void exitDateWithoutQuote(OdpsParser.DateWithoutQuoteContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#dropPartitionSpec}.
	 * @param ctx the parse tree
	 */
	void enterDropPartitionSpec(OdpsParser.DropPartitionSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#dropPartitionSpec}.
	 * @param ctx the parse tree
	 */
	void exitDropPartitionSpec(OdpsParser.DropPartitionSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#sysFuncNames}.
	 * @param ctx the parse tree
	 */
	void enterSysFuncNames(OdpsParser.SysFuncNamesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#sysFuncNames}.
	 * @param ctx the parse tree
	 */
	void exitSysFuncNames(OdpsParser.SysFuncNamesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#descFuncNames}.
	 * @param ctx the parse tree
	 */
	void enterDescFuncNames(OdpsParser.DescFuncNamesContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#descFuncNames}.
	 * @param ctx the parse tree
	 */
	void exitDescFuncNames(OdpsParser.DescFuncNamesContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterFunctionIdentifier(OdpsParser.FunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitFunctionIdentifier(OdpsParser.FunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#reserved}.
	 * @param ctx the parse tree
	 */
	void enterReserved(OdpsParser.ReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#reserved}.
	 * @param ctx the parse tree
	 */
	void exitReserved(OdpsParser.ReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(OdpsParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(OdpsParser.NonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#sql11ReservedKeywordsUsedAsCastFunctionName}.
	 * @param ctx the parse tree
	 */
	void enterSql11ReservedKeywordsUsedAsCastFunctionName(OdpsParser.Sql11ReservedKeywordsUsedAsCastFunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#sql11ReservedKeywordsUsedAsCastFunctionName}.
	 * @param ctx the parse tree
	 */
	void exitSql11ReservedKeywordsUsedAsCastFunctionName(OdpsParser.Sql11ReservedKeywordsUsedAsCastFunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link OdpsParser#sql11ReservedKeywordsUsedAsIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSql11ReservedKeywordsUsedAsIdentifier(OdpsParser.Sql11ReservedKeywordsUsedAsIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link OdpsParser#sql11ReservedKeywordsUsedAsIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSql11ReservedKeywordsUsedAsIdentifier(OdpsParser.Sql11ReservedKeywordsUsedAsIdentifierContext ctx);
}