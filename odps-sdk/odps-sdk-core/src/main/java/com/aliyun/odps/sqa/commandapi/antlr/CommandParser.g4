parser grammar CommandParser;
options
{
    tokenVocab=CommandLexer;
}

command: statement? SEMICOLON EOF;

statement:
      alterStatement
    | authorizationStatement
    | sqlCostStatement
    | showStatement
    | descStatement
    | whoamiStatement
    ;

whoamiStatement
    : KW_WHOAMI
    ;

sqlCostStatement
    : KW_COST KW_SQL query=anythingButSemi
    ;

// compound statement defnition
compoundStatement
    : KW_BEGIN statement* KW_END
    ;

cteStatement
    :
    id = identifier
    ( LPAREN cols=columnNameCommentList RPAREN
    | param=functionParameters (KW_RETURNS retvar=variableName retType=parameterTypeDeclaration)?)?
    KW_AS (LPAREN (queryExp=queryExpression | exp=expression) RPAREN | cpd=compoundStatement)
    ;

tableAliasWithCols
    :
    KW_AS? table=identifier (LPAREN col+=identifier (COMMA col+=identifier)* RPAREN)?
    ;

subQuerySource
    :
    subQuery=subQueryExpression alias = tableAliasWithCols?
    ;

functionParameters:
    LPAREN (param+=parameterDefinition (COMMA param+=parameterDefinition)*)? RPAREN;

parameterDefinition:
    var=variableName decl=parameterTypeDeclaration (EQUAL init=expression)? (KW_COMMENT comment=stringLiteral)?;

parameterTypeDeclaration:
    KW_TABLE LPAREN (var=varSizeParam | columnsType=parameterColumnNameTypeList (COMMA var=varSizeParam)?) RPAREN
    |
    funType = functionTypeDeclaration
    |
    singleType=anyType
    ;

functionTypeDeclaration
    : KW_FUNCTION LPAREN argType = parameterTypeDeclarationList? RPAREN KW_RETURNS ret=parameterTypeDeclaration;

parameterTypeDeclarationList
    : parameterTypeDeclaration (COMMA parameterTypeDeclaration)*
    ;

parameterColumnNameTypeList
    : parameterColumnNameType (COMMA parameterColumnNameType)*
    ;

parameterColumnNameType
    : colName=identifier t = anyType (KW_COMMENT comment=stringLiteral)?
    ;

varSizeParam
    :
    STAR any=anyType
    ;

preSelectClauses
    :
    w = whereClause
    g = groupByClause?
    h = havingClause?
    win = window_clause?
    |
    g = groupByClause
    h = havingClause?
    win = window_clause?
    |
    h = havingClause
    win = window_clause?
    |
    win = window_clause
    ;

postSelectClauses
    :
    o=orderByClause?
    c=clusterByClause?
    d=distributeByClause?
    sort=sortByClause?
    zorder=zorderByClause?
    l=limitClause?
    ;

selectRest
    :
    (f=fromClause | lv=lateralView)? pre=preSelectClauses? post = postSelectClauses
    ;

multiInsertFromRest
    :
    s = selectClause lv = lateralView? pre = preSelectClauses? post = postSelectClauses
    |
    lv = lateralView? pre = preSelectClauses s = selectClause post = postSelectClauses
    ;

fromRest
    :
    s = selectClause pre = preSelectClauses? post = postSelectClauses
    |
    pre = preSelectClauses s = selectClause post = postSelectClauses
    ;

simpleQueryExpression
    :
    s = selectQueryExpression
    |
    f = fromQueryExpression
    ;

selectQueryExpression
    :
    s = selectClause rest = selectRest
    ;

fromQueryExpression
    :
    f = fromClause rest = fromRest
    ;

setOperationFactor
    :
    s = simpleQueryExpression
    |
    LPAREN q=queryExpression RPAREN
    ;

queryExpression
    :
    ( LPAREN q=queryExpression RPAREN rhs += setRHS
    | s = simpleQueryExpression
    ) (rhs += setRHS)*
    ;

queryExpressionWithCTE
    :
    (w = withClause)?
    exp = queryExpression
    ;

setRHS
    :
    op=setOperator operand=setOperationFactor
    ;

multiInsertSetOperationFactor
    :
    f=fromRest
    |
    LPAREN m=multiInsertSelect RPAREN
    ;

multiInsertSelect
    :
    ( LPAREN m=multiInsertSelect RPAREN rhs+=multiInsertSetRHS
    | f=multiInsertFromRest
    ) (rhs+=multiInsertSetRHS)*
    ;

multiInsertSetRHS
    :
    op=setOperator operand=multiInsertSetOperationFactor
    ;

subQueryExpression
    :
    LPAREN query=queryExpression RPAREN
    ;

limitClause
    : KW_LIMIT offset=mathExpression COMMA exp=mathExpression
    | KW_LIMIT exp=mathExpression (KW_OFFSET offset=mathExpression)?
    ;

// add Variable to from source
fromSource
    : pt=partitionedTableFunction
    | t=tableSource
    | sq=subQuerySource
    | vt=virtualTableSource
    | tv=tableVariableSource
    | tf=tableFunctionSource
    | LPAREN js=joinSource RPAREN;

tableVariableSource
    : var = variableName alias=tableAliasWithCols?
    ;

tableFunctionSource
    : fun = function alias=tableAliasWithCols?
    ;

variableName
    : Variable
    ;

// add Varaiable and native type instialization to atom expressions
atomExpression
    :
    KW_NULL
    | con = constant      // literals
    | castExp = castExpression
    | caseExp = caseExpression
    | whenExp = whenExpression
    | LPAREN exp = expression RPAREN
    | var = variableRef      // @xxx
    | varFun = variableCall      // @xxx
    | fun = function
    | col = tableOrColumnRef    // maybe dbname, tablename, colname, funcName, className, package...
    | newExp = newExpression    // new xxxclass()
    | exists = existsExpression
    | subQuery = scalarSubQueryExpression
    ;

variableRef
    :
    variableName
    ;

variableCall
    :
    variableName LPAREN expressionList? RPAREN
    ;

funNameRef
    :
    db=identifier COLON fn=identifier
    ;

lambdaExpression
    :
    KW_FUNCTION LPAREN (p+=lambdaParameter (COMMA p+=lambdaParameter)*)? RPAREN
    (KW_RETURNS retvar=variableName retType=parameterTypeDeclaration)?
    KW_AS (c=compoundStatement | q=queryExpressionWithCTE | e=expression)
    | (p+=lambdaParameter | LPAREN (p+=lambdaParameter (COMMA p+=lambdaParameter)*)? RPAREN) LAMBDA_IMPLEMENT e=expression
    ;

lambdaParameter
    :
    (v=variableName| i=identifier) p=parameterTypeDeclaration?
    ;

tableOrColumnRef
    :
    identifier
    ;

// support function from other languages
newExpression
    : KW_NEW classNameWithPackage
    ( LPAREN args=expressionList? RPAREN
    | (arr+=LSQUARE RSQUARE)+ init=LCURLY elem=expressionList? RCURLY
    | (arr+=LSQUARE len+=expression RSQUARE)+ (arr+=LSQUARE RSQUARE)*
    );

existsExpression:
    KW_EXISTS query=subQueryExpression
    ;

scalarSubQueryExpression:
    subQuery = subQueryExpression
    ;

classNameWithPackage:
    (packages += identifier DOT)* className=identifier (LESSTHAN types=classNameList GREATERTHAN)?;

classNameOrArrayDecl:
    classNameWithPackage (arr+=LSQUARE RSQUARE)*;

classNameList:
    cn+=classNameOrArrayDecl (COMMA cn+=classNameOrArrayDecl)*;


// We have add some keywords. But those keywords are identifyable with Identifier during parsing
// So we can use them as identifier without quote to keep compatible.
odpsqlNonReserved:
    KW_RETURNS | KW_BEGIN | KW_LOOP | KW_NEW | KW_REMOVE | KW_GRANTS | KW_ACL | KW_TYPE | KW_LIST | KW_USERS | KW_WHOAMI
    | KW_TRUSTEDPROJECTS | KW_TRUSTEDPROJECT | KW_SECURITYCONFIGURATION | KW_PACKAGE | KW_PACKAGES | KW_INSTALL | KW_UNINSTALL
    | KW_PRIVILEGES | KW_PROJECT | KW_PROJECTS | KW_LABEL | KW_ALLOW | KW_DISALLOW | KW_P | KW_JOB | KW_JOBS | KW_ACCOUNTPROVIDERS
    | KW_RESOURCES | KW_FLAGS | KW_STATISTIC_LIST | KW_STATISTIC | KW_COUNT | KW_GET | KW_PUT | KW_POLICY | KW_PROJECTPROTECTION
    | KW_EXCEPTION | KW_CLEAR | KW_EXPIRED | KW_EXP | KW_ACCOUNTPROVIDER | KW_SUPER | KW_VOLUMEFILE | KW_VOLUMEARCHIVE | KW_OFFLINEMODEL | KW_PY
    | KW_RESOURCE | KW_STATUS | KW_KILL | KW_HUBLIFECYCLE | KW_SHARDS | KW_SETPROJECT | KW_MERGE | KW_SMALLFILES | KW_PARTITIONPROPERTIES
    | KW_EXSTORE | KW_CHANGELOGS | KW_REDO | KW_HUBTABLE | KW_CHANGEOWNER | KW_RECYCLEBIN | KW_PRIVILEGEPROPERTIES | relaxedKeywords | KW_NULL_VALUE
    | KW_DISTINCT_VALUE | KW_TABLE_COUNT | KW_COLUMN_SUM | KW_COLUMN_MAX | KW_COLUMN_MIN | KW_EXPRESSION_CONDITION | KW_GROUPS
    | KW_CACHE | ByteLengthLiteral | KW_VARIABLES | KW_EXCEPT | KW_SELECTIVITY | KW_LOCALTIMESTAMP | KW_EXTRACT | KW_SUBSTRING | KW_LAST
    | KW_NULLS | KW_DEFAULT | KW_ANY | KW_OFFSET | KW_CLONE | KW_CONSTRAINT
    | KW_UNIQUEJOIN | KW_TABLESAMPLE |  KW_MACRO | KW_FILE | KW_DYNAMICFILTER | KW_DATABASE | KW_UDFPROPERTIES
    | KW_UNBOUNDED | KW_PRECEDING | KW_FOLLOWING | KW_MORE | KW_OVER | KW_PARTIALSCAN | KW_EXCHANGE| KW_CONF
    | KW_LIFECYCLE | KW_CACHEPROPERTIES
    | KW_TENANT
    ;
relaxedKeywords:
     KW_INTERVAL | KW_CONF | KW_EXCHANGE | KW_LESS | KW_MORE;


identifier
    :
    id = Identifier
    | nonReservedId = nonReserved
    | sq11KeywordAsId = sql11ReservedKeywordsUsedAsIdentifier
    | odpsNonReservedId = odpsqlNonReserved
    ;

aliasIdentifier
    :
    identifier
    | doubleQuoteStringLiteral
    ;

identifierWithoutSql11
    :
    id = Identifier
    | nonReservedId = nonReserved
    | odpsNonReservedId = odpsqlNonReserved
    ;

anythingButSemi:
    (~SEMICOLON)*;

memberAccessOperator
    :
    DOT field=identifier
    ;

methodAccessOperator
    :
    DOT (LESSTHAN types=classNameList GREATERTHAN)? field=identifier LPAREN arguments=expressionList? RPAREN
    ;

isNullOperator:
    KW_IS not=KW_NOT? KW_NULL
    ;

inOperator:
    not=KW_NOT? KW_IN (exp=expressionsInParenthese | subQuery=subQueryExpression)
    ;

betweenOperator:
    not=KW_NOT? KW_BETWEEN (min=mathExpression) KW_AND (max=mathExpression)
    ;

mathExpression
    :
    lhs=mathExpression op=BITWISEXOR rhs=mathExpression
    |lhs=mathExpression (op=STAR | op=DIVIDE | op=MOD | op=DIV) rhs=mathExpression
    |lhs=mathExpression (op=PLUS | op=MINUS) rhs=mathExpression
    |lhs=mathExpression op=CONCATENATE rhs=mathExpression
    |lhs=mathExpression op=AMPERSAND rhs=mathExpression
    |lhs=mathExpression op=BITWISEOR rhs=mathExpression
    |exp=unarySuffixExpression
    //|cast=nativeCast operand=mathExpression
    ;

unarySuffixExpression:
    operand=unarySuffixExpression op=isNullOperator
    |exp=unaryPrefixExpression
    ;

unaryPrefixExpression:
    (op=PLUS | op=MINUS | op=TILDE) operand=unaryPrefixExpression
    | exp=fieldExpression
    ;

fieldExpression:
    operand=fieldExpression (member=memberAccessOperator|ls=LSQUARE index=expression RSQUARE|method=methodAccessOperator)
    |exp=atomExpression
    ;

logicalExpression:
    lhs=logicalExpression op=KW_AND rhs=logicalExpression
    |lhs=logicalExpression op=KW_OR rhs=logicalExpression
    |exp=notExpression
    ;

notExpression:
    op=KW_NOT operand=notExpression
    |exp=equalExpression
    ;

equalExpression:
    lhs=equalExpression
            (
            not=KW_NOT? (op=KW_LIKE|op=KW_RLIKE|op=KW_REGEXP)
            |op=EQUAL
            |op=EQUAL_NS
            |KW_IS not=KW_NOT? op=KW_DISTINCT KW_FROM
            |op=NOTEQUAL
            |op=LESSTHANOREQUALTO
            |op=LESSTHAN
            |op=GREATERTHANOREQUALTO
            |op=GREATERTHAN
            )
            rhs=equalExpression
    |lhs=equalExpression (in=inOperator | between=betweenOperator)
    |exp=mathExpression
    |expIn=mathExpressionListInParentheses
    ;

mathExpressionListInParentheses
    : LPAREN mathExpressionList RPAREN
    ;

mathExpressionList
    : mathExpression (COMMA mathExpression)*
    ;

expression:
    logicalExpression
    ;

instanceId:
    Identifier;

authorizationStatement:
    // odps only
      addUserStatement
    | removeUserStatement
    | addGroupStatement
    | removeGroupStatement
    | addAccountProvider
    | removeAccountProvider
    | listUsers
    | listGroups
    | showAcl
    | describeRole
    | listRoles
    | listTrustedProjects
    | addTrustedProject
    | removeTrustedProject
    | showSecurityConfiguration
    | showPackages
    | showItems
    | describePackage
    | installPackage
    | uninstallPackage
    | createPackage
    | deletePackage
    | addToPackage
    | removeFromPackage
    | allowPackage
    | disallowPackage
    | putPolicy
    | getPolicy
    | clearExpiredGrants
    | grantLabel
    | revokeLabel
    | showLabel
    | grantSuperPrivilege
    | revokeSuperPrivilege
    | purgePrivileges

    // Odps privilege commands
    | createRoleStatement
    | dropRoleStatement
    | addRoleToProject
    | removeRoleFromProject
    | grantRole
    | revokeRole
    | grantPrivileges
    | revokePrivileges
    | showGrants        // odps: show grants
    | showRoleGrants    // odps show grants
    | showRoles     // odps: list roles
    | showRolePrincipals    // odps: describe role
    ;

listUsers:
    KW_LIST KW_TENANT? KW_USERS;

listGroups:
    KW_LIST KW_TENANT? KW_GROUPS (KW_FOR KW_USER user)?;

addUserStatement:
    KW_ADD KW_TENANT? KW_USER name=user comment=userRoleComments?;

addGroupStatement:
    KW_ADD KW_TENANT? KW_GROUP name=principalIdentifier comment=userRoleComments?;

removeUserStatement:
    KW_REMOVE KW_TENANT? KW_USER user;

removeGroupStatement:
    KW_REMOVE KW_TENANT? KW_GROUP principalIdentifier;

addAccountProvider:
    KW_ADD KW_ACCOUNTPROVIDER accountProvider;

removeAccountProvider:
    KW_REMOVE KW_ACCOUNTPROVIDER accountProvider;

showAcl:
    KW_SHOW KW_ACL KW_FOR privilegeObjectName (KW_ON KW_TYPE privilegeObjectType)?;

describeRole:
    (KW_DESCRIBE|KW_DESC) KW_TENANT? KW_ROLE roleName;

listRoles:
    KW_LIST KW_TENANT? KW_ROLES;

listTrustedProjects:
    KW_LIST KW_TRUSTEDPROJECTS;

addTrustedProject:
    KW_ADD KW_TRUSTEDPROJECT projectName;

removeTrustedProject:
    KW_REMOVE KW_TRUSTEDPROJECT projectName;

showSecurityConfiguration:
    KW_SHOW KW_SECURITYCONFIGURATION;

showPackages:
    KW_SHOW KW_PACKAGES (KW_WITH privilegeObject)? privilegeProperties?;

showItems:
    KW_SHOW KW_PACKAGE (pkg=packageName | KW_ITEMS (KW_FROM prj=projectName)? (KW_ON KW_TYPE tp=privilegeObjectType)?) privilegeProperties?;

installPackage:
    KW_INSTALL KW_PACKAGE pkg=packageNameWithProject privilegeProperties?;

uninstallPackage:
    KW_UNINSTALL KW_PACKAGE pkg=packageNameWithProject privilegeProperties?;

createPackage:
    KW_CREATE KW_PACKAGE packageName;

deletePackage:
    (KW_DELETE|KW_DROP) KW_PACKAGE packageName;

addToPackage:
    KW_ADD privilegeObject KW_TO KW_PACKAGE packageName (KW_WITH KW_PRIVILEGES privilege (COMMA privilege)*)? props=privilegeProperties?;

removeFromPackage:
    KW_REMOVE privilegeObject KW_FROM KW_PACKAGE packageName;

allowPackage:
    KW_ALLOW KW_PROJECT (pj=projectName|st=STAR) KW_TO KW_INSTALL KW_PACKAGE packageName
    (KW_WITH privilege (COMMA privilege)*)? (KW_USING KW_LABEL label)? (KW_EXP Number)?;

disallowPackage:
    KW_DISALLOW KW_PROJECT (pj=projectName|st=STAR) KW_TO KW_INSTALL KW_PACKAGE packageName;

putPolicy:
    KW_PUT KW_POLICY filePath (KW_ON KW_TENANT? KW_ROLE roleName)?;

getPolicy:
    KW_GET KW_POLICY (KW_ON KW_TENANT? KW_ROLE roleName)?;

clearExpiredGrants:
    KW_CLEAR KW_EXPIRED KW_GRANTS;

grantLabel:
    KW_GRANT KW_LABEL label KW_ON KW_TABLE tabName=privilegeObjectName (LPAREN columnNameList RPAREN)? KW_TO (p=principalName KW_WITH KW_EXP Number | p=principalName) props=privilegeProperties?;

revokeLabel:
    KW_REVOKE KW_LABEL KW_ON KW_TABLE tabName=privilegeObjectName (LPAREN columnNameList RPAREN)? KW_FROM p=principalName props=privilegeProperties?;

showLabel:
    KW_SHOW KW_LABEL label? KW_GRANTS
        ( KW_ON KW_TABLE tabName=privilegeObjectName (KW_FOR p=principalName)?
        | KW_FOR p=principalName
        | forTable=KW_FOR KW_TABLE tabName=privilegeObjectName)? props=privilegeProperties?;

grantSuperPrivilege:
    KW_GRANT KW_SUPER privilege (COMMA privilege)* KW_TO principalName;

revokeSuperPrivilege:
    KW_REVOKE KW_SUPER privilege (COMMA privilege)* KW_FROM principalName;

createRoleStatement
    : KW_CREATE KW_TENANT? KW_ROLE role=roleName comment=userRoleComments? props=privilegeProperties?;

dropRoleStatement
    : (KW_DELETE|KW_DROP) KW_TENANT? KW_ROLE roleName;

addRoleToProject
    : KW_ADD KW_TENANT KW_ROLE roleName KW_TO KW_PROJECT projectName;

removeRoleFromProject
    : KW_REMOVE KW_TENANT KW_ROLE roleName KW_FROM KW_PROJECT projectName;

grantRole
    : KW_GRANT (KW_TENANT? KW_ROLE)? roleName (COMMA roleName)* KW_TO principalSpecification withAdminOption?;

revokeRole
    : KW_REVOKE adminOptionFor? (KW_TENANT? KW_ROLE)? roleName (COMMA roleName)* KW_FROM principalSpecification;

grantPrivileges
    : KW_GRANT privilege (COMMA privilege)* KW_ON privilegeObject
    KW_TO principalSpecification withGrantOption? privilegeProperties?;

privilegeProperties:
    KW_PRIVILEGEPROPERTIES LPAREN keyValueProperty (COMMA keyValueProperty)* RPAREN;

revokePrivileges
    : KW_REVOKE grantOptionFor? privilege (COMMA privilege)* KW_ON privilegeObject KW_FROM principalSpecification privilegeProperties?;

purgePrivileges
    : KW_PURGE KW_PRIVILEGES KW_FROM principalSpecification
    ;

showGrants:
    KW_SHOW (KW_GRANT|KW_GRANTS) KW_FOR? principalName?
    (KW_ON KW_TYPE privilegeObjectType | KW_ON privilegeObject)?
    privilegeProperties?;

showRoleGrants:
    KW_SHOW KW_ROLE KW_GRANT principalName;

showRoles:
    KW_SHOW KW_ROLES;

showRolePrincipals:
    KW_SHOW KW_PRINCIPALS KW_FOR? KW_TENANT? KW_ROLE? roleName;

user:
    (allIdentifiers DOLLAR)?    // account provider
    (allIdentifiers | Number) ((DOT | MINUS | UNDERLINE) (allIdentifiers | Number))*  // main body
    ((Variable |  AT (allIdentifiers | Number)) ((DOT | MINUS | UNDERLINE) (allIdentifiers | Number))*)?
    (COLON (identifier|Number|DOT|MINUS|AT)+)?;

userRoleComments:
    (KW_COMMENT | options) stringLiteral;

accountProvider:
    identifier;


privilegeObjectName:
    ((STAR? identifier) | STAR) ((Number|DOT|MINUS|AT|SHARP|DOLLAR|STAR) identifier?)* STAR? | STAR;

privilegeObjectType:
    identifier | KW_FUNCTION | KW_DATABASE;

roleName:
    identifier;

packageName:
    identifier;

packageNameWithProject:
    (proj=identifier DOT)? name=identifier;

principalSpecification:
    principalName (COMMA principalName)*;

principalName:
    t=KW_USER? (principalIdentifier | user)
    | KW_TENANT? t=KW_USER (principalIdentifier | user)
    | KW_TENANT? t=KW_GROUP principalIdentifier
    | KW_TENANT? t=KW_ROLE identifier
    ;

describePackage:
    (KW_DESCRIBE|KW_DESC)
    (
    | KW_PACKAGE (proj=projectName DOT)? pkgName=packageName privilegeProperties?
    | KW_PACKAGE KW_ITEMS obj=privilegeObject (KW_FROM proj=projectName)?)
    ;

principalIdentifier
    : id = identifier
    ;

privilege:
    privilegeType (LPAREN cols=columnNameList RPAREN)?;

privilegeType:
    identifier | KW_SELECT;

// database or table type. Type is optional, default type is table
privilegeObject:
    KW_TABLE? tableName (LPAREN cols=columnNameList RPAREN)? parts=partitionSpec?
    | privilegeObjectType privilegeObjectName;

filePath:
    stringLiteral | DIVIDE? identifier ((COLON | DIVIDE | ESCAPE | DOT | MINUS)+ identifier)*;

adminOptionFor:
    KW_ADMIN KW_OPTION KW_FOR;

withAdminOption:
    KW_WITH KW_ADMIN KW_OPTION;

withGrantOption:
    KW_WITH KW_GRANT KW_OPTION;

grantOptionFor:
    KW_GRANT KW_OPTION KW_FOR;


label:
    Number;

columnNameList
    : columnName (COMMA columnName)*
    ;

columnName
    : identifier
    ;

allIdentifiers
    :
    id = Identifier
    | nonReservedId = nonReserved
    | sq11KeywordAsId = sql11ReservedKeywordsUsedAsIdentifier
    | odpsNonReservedId = odpsqlNonReserved
    | reservedId = reserved
    ;

options:
    MINUS identifier;

projectName:
    identifier;

alterStatement
    : KW_ALTER KW_TABLE tableNamee=tableName partition=partitionSpec? tableSuffix=alterTableStatementSuffix
    ;

alterTableStatementSuffix
    : archive=alterStatementSuffixArchive
    | merge=alterStatementSuffixMergeFiles
    | compact=alterStatementSuffixCompact
    | freeze=alterStatementSuffixFreeze
    | restore=alterStatementSuffixRestore
    ;

alterStatementSuffixArchive
    : KW_ARCHIVE
    ;

alterStatementSuffixMergeFiles
    : KW_MERGE KW_SMALLFILES
    ;

alterStatementSuffixCompact
    : KW_COMPACT compactTypee=compactType
    ;

compactType
    : KW_MAJOR | KW_MINOR
    ;

alterStatementSuffixFreeze
    : KW_FREEZE
    ;

alterStatementSuffixRestore
    : KW_RESTORE
    ;

descStatement
    : descTableStatement
    | descTableExtendedStatement
    | descProjectStatement
    | descInstanceStatement
    | descSchemaStatement
    ;

descSchemaStatement
    : (KW_DESCRIBE | KW_DESC) KW_SCHEMA schema_Name=schemaName
    ;

descTableStatement
    : (KW_DESCRIBE | KW_DESC) table_Name=tableName partition_Spec=partitionSpec?
    ;

descTableExtendedStatement
    : (KW_DESCRIBE | KW_DESC) KW_EXTENDED table_Name=tableName partition_Spec=partitionSpec?
    ;

descProjectStatement
    : (KW_DESCRIBE | KW_DESC) KW_PROJECT KW_EXTENDED? project_Name=projectName
    ;

descInstanceStatement
    : (KW_DESCRIBE | KW_DESC) KW_INSTANCE instance_Id=instanceId
    ;

showStatement
    : showTableStatement
    | showPartitionStatement
    | showInstanceStatement
    | showSchemasStatament
    | showCreateTableStatement
    ;

showCreateTableStatement
    : KW_SHOW KW_CREATE KW_TABLE table_name=tableName
    ;

showSchemasStatament
    : KW_SHOW KW_SCHEMAS ((KW_IN | KW_FROM) project_Name=identifier)? (KW_LIKE prefix_name=showStmtIdentifier)?
    ;

showPartitionStatement
    : KW_SHOW KW_PARTITIONS table_Name=tableName partition_Spec=partitionSpec?
    ;

// 神坑，三个地方都不要使用Number，否则有可能没办法解析到EOF，导致解析失败。
showInstanceStatement
    : KW_SHOW KW_INSTANCES (KW_FROM from_date=bareDate)? (KW_TO to_date=bareDate)? (Num)?
    ;

showTableStatement
    : KW_SHOW KW_TABLES ((KW_IN | KW_FROM) project_Name=identifier (DOT schema_Name=identifier)?)? (KW_LIKE prefix_name=showStmtIdentifier)?
    ;

bareDate:
    stringLiteral | dateWithoutQuote;

showStmtIdentifier
    : identifier
    | simpleStringLiteral
    ;

rowFormat
    : rowFormatSerde
    | rowFormatDelimited
    |
    ;

recordReader
    : KW_RECORDREADER reader=stringLiteral
    |
    ;

recordWriter
    : KW_RECORDWRITER writer=stringLiteral
    |
    ;

rowFormatSerde
    : KW_ROW KW_FORMAT KW_SERDE name=stringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    ;

rowFormatDelimited
    :
      KW_ROW KW_FORMAT KW_DELIMITED fd=tableRowFormatFieldIdentifier? cd=tableRowFormatCollItemsIdentifier? md=tableRowFormatMapKeysIdentifier? ld=tableRowFormatLinesIdentifier? nul=tableRowNullFormat?
    ;

tableRowFormat
    :
      rfd=rowFormatDelimited
    | rfs=rowFormatSerde
    ;


tableProperties
    :
      LPAREN tablePropertiesList RPAREN
    ;

tablePropertiesList
    :
      kv+=keyValueProperty (COMMA kv+=keyValueProperty)*
    ;

keyValueProperty
    :
      key=simpleStringLiteral EQUAL value=simpleStringLiteral
    ;

userDefinedJoinPropertiesList
    :
    kv+=keyValueProperty (COMMA kv+=keyValueProperty)*
    ;


tableRowFormatFieldIdentifier
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=stringLiteral (KW_ESCAPED KW_BY fldEscape=stringLiteral)?
    ;

tableRowFormatCollItemsIdentifier
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=stringLiteral
    ;

tableRowFormatMapKeysIdentifier
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=stringLiteral
    ;

tableRowFormatLinesIdentifier
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=stringLiteral
    ;

tableRowNullFormat
    :
      KW_NULL KW_DEFINED KW_AS nullIdnt=stringLiteral
    ;

columnNameTypeList
    : columnNameType (COMMA columnNameType)*
    ;

columnNameColonTypeList
    : t+=columnNameColonType (COMMA t+=columnNameColonType)*
    ;

columnNameCommentList
    : columnNameComment (COMMA columnNameComment)*
    ;

columnNameComment
    : colName=identifier (KW_COMMENT comment=stringLiteral)?
    ;

columnRefOrder
    : expression (asc=KW_ASC | desc=KW_DESC)? (KW_NULLS (first=KW_FIRST | last=KW_LAST))?
    ;


columnNameType
    : colName=identifier t = colType (KW_COMMENT comment=stringLiteral)?
    ;


columnNameColonType
    : n=identifier COLON t=builtinTypeOrUdt (KW_COMMENT c=stringLiteral)?
    ;

colType
    : t=type
    ;

colTypeList
    : colType (COMMA colType)*
    ;

anyType
    : t=type | KW_ANY
    ;

type
    : pt=primitiveType
    | lt=listType
    | st=structType
    | mt=mapType
    | ut=unionType;

primitiveType
    : KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_BOOLEAN
    | KW_FLOAT
    | KW_DOUBLE
    | KW_DATE
    | KW_DATETIME
    | KW_TIMESTAMP
    | KW_STRING
    | KW_BINARY
    | KW_DECIMAL (LPAREN prec=Number (COMMA scale=Number)? RPAREN)?
    | KW_VARCHAR LPAREN length=Number RPAREN
    | KW_CHAR LPAREN length=Number RPAREN
    ;

builtinTypeOrUdt
    : t=type | cn=classNameOrArrayDecl
    ;

primitiveTypeOrUdt
    : t=primitiveType | cn=classNameOrArrayDecl
    ;

listType
    : KW_ARRAY LESSTHAN elemType=builtinTypeOrUdt GREATERTHAN
    ;

structType
    : KW_STRUCT LESSTHAN tl=columnNameColonTypeList GREATERTHAN
    ;

mapType
    : KW_MAP LESSTHAN left=primitiveTypeOrUdt COMMA right=builtinTypeOrUdt GREATERTHAN
    ;

unionType
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN
    ;

setOperator
    : union = KW_UNION all = KW_ALL
    | union = KW_UNION KW_DISTINCT?
    | intersect = KW_INTERSECT all = KW_ALL
    | intersect = KW_INTERSECT KW_DISTINCT?
    | minus = (KW_MINUS|KW_EXCEPT) all = KW_ALL
    | minus = (KW_MINUS|KW_EXCEPT) KW_DISTINCT?
    ;

withClause
  :
  KW_WITH branches+=cteStatement (COMMA branches+=cteStatement)*
;


selectClause
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM trfm=selectTrfmClause))
    | trfmc=trfmClause
    ;

selectList
    :
    selectItem ( COMMA  selectItem )*
    ;

selectTrfmClause
    :
    LPAREN exprs=selectExpressionList RPAREN
    inSerde=tableRowFormat? inRec=recordWriter
    KW_USING using=stringLiteral (KW_RESOURCES res+=stringLiteral (COMMA res+=stringLiteral)*)?
    ( KW_AS ((LPAREN (aliases=aliasList | cols=columnNameTypeList) RPAREN) | (aliases=aliasList | cols=columnNameTypeList)))?
    outSerde=tableRowFormat? outRec=recordReader
    ;

hintClause
    :
    HintStart hintList STAR DIVIDE
    ;

hintList
    :
    hintItem (COMMA hintItem)*
    ;

hintItem
    :
    mapjoin=mapJoinHint
    |
    skewjoin=skewJoinHint
    |
    selectivity=selectivityHint
    |
    dynamicfilter=dynamicfilterHint
    |
    hintName (LPAREN hintArgs RPAREN)?
    ;

dynamicfilterHint:
    KW_DYNAMICFILTER (LPAREN hintArgs RPAREN);

mapJoinHint:
    KW_MAPJOIN (LPAREN hintArgs RPAREN)?;

skewJoinHint:
    KW_SKEWJOIN (LPAREN multipleSkewHintArgs RPAREN)?;

selectivityHint:
    name=KW_SELECTIVITY (LPAREN num=Number RPAREN)?;

//tableName1((123, 456), (789, 56)), tableName2((22, 33), (55, 66)),
multipleSkewHintArgs
    :
    multipleSkewHints+=skewJoinHintArgs ( COMMA multipleSkewHints+=skewJoinHintArgs )*;

skewJoinHintArgs
    :
    table=identifier ( columns=skewColumns ( LPAREN keyValues+=skewJoinHintKeyValues ( COMMA keyValues+=skewJoinHintKeyValues )* RPAREN )* )?;

// (c1, c2)
skewColumns
    : LPAREN skewKeys+=identifier ( COMMA skewKeys+=identifier )* RPAREN;

// (123, 456)
skewJoinHintKeyValues
    : LPAREN keyValue+=constant ( COMMA keyValue+=constant )* RPAREN;

hintName
    :
    KW_STREAMTABLE
    | KW_HOLD_DDLTIME
    | id=identifier
    ;

hintArgs
    :
    hintArgName (COMMA hintArgName)*
    ;

hintArgName
    :
    identifier
    ;

selectItem
    :
    tableAllColumns
    |
    ( expression
      ((KW_AS? alias+=aliasIdentifier) | (KW_AS LPAREN alias+=aliasIdentifier (COMMA alias+=aliasIdentifier)* RPAREN))?
    )
    ;

trfmClause
    :
    (   KW_MAP    exprs=selectExpressionList
      | KW_REDUCE exprs=selectExpressionList )
    inSerde=rowFormat inRec=recordWriter
    KW_USING using=stringLiteral (KW_RESOURCES res+=stringLiteral (COMMA res+=stringLiteral)*)?
    ( KW_AS ((LPAREN (aliases=aliasList | cols=columnNameTypeList) RPAREN) | (aliases=aliasList | cols=columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    ;

selectExpression
    :
    wildcardCol=tableAllColumns
    |
    exp=expression
    ;

selectExpressionList
    :
    selectExpression (COMMA selectExpression)*
    ;

//---------------------- Rules for windowing clauses -------------------------------
window_clause
:
  KW_WINDOW winDef+=window_defn (COMMA winDef+=window_defn)*
;

window_defn
:
  name=identifier KW_AS spec=window_specification
;

window_specification
:
  id=identifier |
  LPAREN (
    id=identifier? p=partitioningSpec? w=window_frame?
  ) RPAREN
;

window_frame :
  frameType=(KW_ROWS | KW_RANGE | KW_GROUPS) b=window_frame_boundary ex=frame_exclusion?
  |
  frameType=(KW_ROWS | KW_RANGE | KW_GROUPS) KW_BETWEEN
  s=window_frame_boundary KW_AND end=window_frame_boundary ex=frame_exclusion?
;

frame_exclusion :
  KW_EXCLUDE KW_CURRENT KW_ROW
  | KW_EXCLUDE KW_GROUP
  | KW_EXCLUDE KW_TIES
  | KW_EXCLUDE KW_NO KW_OTHERS
  ;

window_frame_boundary
:
  KW_UNBOUNDED (d=KW_PRECEDING|d=KW_FOLLOWING) |
  KW_CURRENT KW_ROW  |
//  Number (d=KW_PRECEDING | d=KW_FOLLOWING )
  value=mathExpression/*precedenceBitwiseOrExpression*/ (d=KW_PRECEDING | d=KW_FOLLOWING )
;

tableAllColumns
    : STAR
    | table=identifier DOT STAR
    ;

expressionList
    :
    expression (COMMA expression)*
    ;

aliasList
    :
    ids+=aliasIdentifier (COMMA ids+=aliasIdentifier)*
    ;

//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
    :
    KW_FROM hintClause? joinSource
    ;

joinSource
    : lhs=fromSource ( rhs += joinRHS )*
    | uniqueJoinToken uniqueJoinSource (COMMA uniqueJoinSource)+
    ;

joinRHS
    : joinType=joinToken joinTable=fromSource
      ( KW_USING LPAREN commonCols+=identifier ( COMMA commonCols+=identifier)* RPAREN
      | ( KW_ON joinOn+=expression )*
        ( KW_USING cbn=functionIdentifier
          (LPAREN exprs=selectExpressionList RPAREN)?
          (tableAlias)?
          ( (KW_AS? alias+=aliasIdentifier)
          | (KW_AS LPAREN alias+=aliasIdentifier (COMMA alias+=aliasIdentifier)* RPAREN)
          )?
          (KW_WITH KW_UDFPROPERTIES LPAREN userDefinedJoinPropertiesList RPAREN)?
          (sort=sortByClause)?
        )?
      )
    | natural=KW_NATURAL joinType=joinToken joinTable=fromSource
    | lv=lateralView (KW_ON joinOn+=expression)?
    ;

uniqueJoinSource
    : KW_PRESERVE? fromSource uniqueJoinExpr
    ;

uniqueJoinExpr
    : LPAREN e1+=expression (COMMA e1+=expression)* RPAREN
    ;

uniqueJoinToken
    : KW_UNIQUEJOIN;

joinToken
    :
      KW_JOIN
    | KW_INNER KW_JOIN
    | COMMA
    | KW_CROSS KW_JOIN
    | KW_LEFT  (outer = KW_OUTER)? KW_JOIN
    | KW_RIGHT (outer = KW_OUTER)? KW_JOIN
    | KW_FULL  (outer = KW_OUTER)? KW_JOIN
    | KW_LEFT semi = KW_SEMI KW_JOIN
    | KW_LEFT anti = KW_ANTI KW_JOIN
    ;

lateralView
	:
	KW_LATERAL KW_VIEW outer=KW_OUTER function tableAlias (KW_AS identifier (COMMA identifier)*)?
	|
	KW_LATERAL KW_VIEW function tableAlias KW_AS identifier (COMMA identifier)*
	;

tableAlias
    :
    aliasIdentifier
    ;


tableBucketSample
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN
    ;

splitSample
    :
    KW_TABLESAMPLE LPAREN  (numerator=Number) (percent=KW_PERCENT|KW_ROWS) RPAREN
    |
    KW_TABLESAMPLE LPAREN  (numerator=ByteLengthLiteral) RPAREN
    ;

tableSample
    :
    tableBucketSample |
    splitSample
    ;

tableSource
    : tabname=tableName
    (props=tableProperties)?
    (ts=tableSample)?
    (KW_AS? (alias=identifierWithoutSql11 | extra=availableSql11KeywordsForOdpsTableAlias | l=doubleQuoteStringLiteral)
        (LPAREN col+=identifier (COMMA col+=identifier)* RPAREN)?)?
    ;

// for compatibility, 'inner' and 'user' is avaliable as a table alias
availableSql11KeywordsForOdpsTableAlias
    :
    KW_USER
    | KW_INNER
    ;

schemaName
    :
    db=identifier DOT sch=identifier
    |
    sch=identifier
    ;

tableName
    :
    db=identifier DOT tab=identifier
    |
    tab=identifier
    |
    db=identifier DOT sch=identifier DOT tab=identifier
    ;

//---------------------- Rules for parsing PTF clauses -----------------------------
partitioningSpec
   :
   p = partitionByClause o = orderByClause? |
   o = orderByClause |
   d = distributeByClause s = sortByClause? |
   s = sortByClause |
   c = clusterByClause
   ;

partitionTableFunctionSource
   :
   subQuerySource |
   tableSource |
   partitionedTableFunction
   ;

partitionedTableFunction
   :
   name=Identifier LPAREN KW_ON
   (ptfsrc=partitionTableFunctionSource spec=partitioningSpec?)
   (Identifier LPAREN expression RPAREN ( COMMA Identifier LPAREN expression RPAREN)*)?
   RPAREN alias=Identifier?
   ;

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
    :
    KW_WHERE expression
    ;

valueRowConstructor
    :
    //LPAREN precedenceUnaryPrefixExpression (COMMA precedenceUnaryPrefixExpression)* RPAREN
    LPAREN cols+=mathExpression (COMMA cols+=mathExpression)* RPAREN
    ;

valuesTableConstructor
    :
    rows+=valueRowConstructor (COMMA rows+=valueRowConstructor)*
    ;

/*
VALUES(1),(2) means 2 rows, 1 column each.
VALUES(1,2),(3,4) means 2 rows, 2 columns each.
VALUES(1,2,3) means 1 row, 3 columns
*/
valuesClause
    :
    KW_VALUES values=valuesTableConstructor
    ;

/*
This represents a clause like this:
(VALUES(1,2),(2,3)) as VirtTable(col1,col2)
*/
virtualTableSource
   	:
   	LPAREN values=valuesClause RPAREN tableDecl=tableNameColList
   	|
   	values=valuesClause tableDecl=tableNameColList
   	;
/*
e.g. as VirtTable(col1,col2)
Note that we only want literals as column names
*/
tableNameColList
    :
    KW_AS? table=identifier LPAREN col+=identifier (COMMA col+=identifier)* RPAREN
    ;

functionTypeCubeOrRollup
    : (c=KW_CUBE | r=KW_ROLLUP) LPAREN gs+=groupingSetExpression ( COMMA gs+=groupingSetExpression)*  RPAREN
    ;

groupingSetsItem
    :
    functionTypeCubeOrRollup | groupingSetExpression
    ;

groupingSetsClause
    :
    sets = KW_GROUPING KW_SETS
    LPAREN groupingSetsItem ( COMMA groupingSetsItem)*  RPAREN
    ;


groupByKey
    :
    cr=functionTypeCubeOrRollup | gp=groupingSetsClause | exp=expression
    ;
// group by a,b
groupByClause
    :
    KW_GROUP KW_BY
    groupByKey (COMMA groupByKey)*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE) | (groupingset=groupingSetsClause)) ?
    ;

groupingSetExpression
   :
   groupingSetExpressionMultiple
   |
   groupingExpressionSingle
   ;

groupingSetExpressionMultiple
   :
   LPAREN
   expression? (COMMA expression)*
   RPAREN
   ;

groupingExpressionSingle
    :
    expression
    ;

havingClause
    :
    KW_HAVING havingCondition
    ;

havingCondition
    :
    expression
    ;

expressionsInParenthese
    :
    LPAREN expressionsNotInParenthese RPAREN
    ;

expressionsNotInParenthese
    :
    expression (COMMA expression)*
    ;

columnRefOrderInParenthese
    :
    LPAREN columnRefOrderNotInParenthese RPAREN
    ;

columnRefOrderNotInParenthese
    :
    columnRefOrder (COMMA columnRefOrder)*
    ;

// order by a,b
orderByClause
    :
    KW_ORDER KW_BY exp+=columnRefOrder ( COMMA exp+=columnRefOrder)*
    ;

columnNameOrIndexInParenthese
    :
    LPAREN columnNameOrIndexNotInParenthese RPAREN
    ;

columnNameOrIndexNotInParenthese
    :
    col += columnNameOrIndex (COMMA col += columnNameOrIndex)*
    ;

columnNameOrIndex
    :
    col = tableOrColumnRef
    | index = constant
    ;

// zorder by a,b
zorderByClause
    :
    KW_ZORDER KW_BY
    (
    expsParen = columnNameOrIndexInParenthese
    |
    expsNoParen = columnNameOrIndexNotInParenthese
    )
    ;

clusterByClause
    :
    KW_CLUSTER KW_BY
    (
    expsParen = expressionsInParenthese
    |
    expsNoParen = expressionsNotInParenthese
    )
    ;

partitionByClause
    :
    KW_PARTITION KW_BY
    (
    expsParen=expressionsInParenthese
    |
    expsNoParen=expressionsNotInParenthese
    )
    ;

distributeByClause
    :
    KW_DISTRIBUTE KW_BY
    (
    expsParen = expressionsInParenthese
    |
    expsNoParen = expressionsNotInParenthese
    )
    ;

sortByClause
    :
    KW_SORT KW_BY
    (
    expsParen=columnRefOrderInParenthese
    |
    expsNoParen=columnRefOrderNotInParenthese
    )
    ;

// fun(par1, par2, par3)
function
    :
    name=functionName
    lp = LPAREN
      (
        star = STAR
        | distinct = KW_DISTINCT? (arg+=functionArgument (COMMA arg+=functionArgument)*)?
      )
    RPAREN
    (
        (KW_WITHIN KW_GROUP LPAREN  obc = orderByClause RPAREN)?
        (KW_FILTER LPAREN wc = whereClause RPAREN)?
        |
        (KW_FILTER LPAREN wc = whereClause RPAREN)?
        (KW_OVER ws=window_specification)?
    )
    |
    bfs=builtinFunctionStructure
    ;

functionArgument
    :
    s=selectExpression
    |
    f=funNameRef
    |
    l=lambdaExpression
    ;

builtinFunctionStructure
    : KW_EXTRACT LPAREN u=intervalQualifiersUnit KW_FROM arg=expression RPAREN
    | KW_SUBSTRING LPAREN arg=expression KW_FROM st=mathExpression (KW_FOR end=mathExpression)? RPAREN
    ;

functionName
    : // Keyword IF is also a function name
    kwIf = KW_IF
    | kwArray = KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | id = functionIdentifier
    | sql11ReservedId = sql11ReservedKeywordsUsedAsCastFunctionName
    ;

castExpression
    :
    KW_CAST
    LPAREN
          exp = expression
          KW_AS typeDecl=builtinTypeOrUdt
    RPAREN
    ;

caseExpression
    :
    KW_CASE caseExp = expression
    (KW_WHEN whenExp += expression KW_THEN thenExp += expression)+
    (KW_ELSE elseExp = expression)?
    KW_END
    ;

whenExpression
    :
    KW_CASE
     ( KW_WHEN whenExp += expression KW_THEN thenExp += expression)+
    (KW_ELSE elseExp = expression)?
    KW_END
    ;

constant
    :
    n=Number
    | date=dateLiteral
    | timestamp=timestampLiteral
    | datetime=dateTimeLiteral
    | i=intervalLiteral
    | s=stringLiteral
    | bi=BigintLiteral
    | si=SmallintLiteral
    | ti=TinyintLiteral
    | df=DecimalLiteral
    | cs=charSetStringLiteral
    | b=booleanValue
    ;

simpleStringLiteral
    :
    (StringLiteral | DoubleQuoteStringLiteral)
    ;

stringLiteral
    :
    simpleStringLiteral+
    ;

doubleQuoteStringLiteral
    :
    DoubleQuoteStringLiteral
    ;



charSetStringLiteral
    :
    CharSetStringLiteral
    ;

dateLiteral
    :
    KW_DATE s=simpleStringLiteral
    |
    KW_CURRENT_DATE   // for compatible, CURRENT_DATE should be recognized as identifier
    ;

dateTimeLiteral
    :
    KW_DATETIME s = simpleStringLiteral
    ;

timestampLiteral
    :
    KW_TIMESTAMP s=simpleStringLiteral
    |
    KW_CURRENT_TIMESTAMP
    |
    KW_LOCALTIMESTAMP
    ;

intervalLiteral
    :
    KW_INTERVAL v=stringLiteral q=intervalQualifiers
    | KW_INTERVAL e=mathExpression u=intervalQualifiersUnit
    ;

intervalQualifiers
    : y2m=KW_YEAR intervalQualifierPrecision? KW_TO KW_MONTH intervalQualifierPrecision?
    | d2h=KW_DAY intervalQualifierPrecision? KW_TO KW_HOUR intervalQualifierPrecision?
    | d2m=KW_DAY intervalQualifierPrecision? KW_TO KW_MINUTE intervalQualifierPrecision?
    | d2s=KW_DAY intervalQualifierPrecision? KW_TO KW_SECOND intervalQualifierPrecision?
    | h2m=KW_HOUR intervalQualifierPrecision? KW_TO KW_MINUTE intervalQualifierPrecision?
    | h2s=KW_HOUR intervalQualifierPrecision? KW_TO KW_SECOND intervalQualifierPrecision?
    | m2s=KW_MINUTE intervalQualifierPrecision? KW_TO KW_SECOND intervalQualifierPrecision?
    | u=intervalQualifiersUnit intervalQualifierPrecision?
    ;

intervalQualifiersUnit
    : y=KW_YEAR | y=KW_YEARS
    | M=KW_MONTH | M=KW_MONTHS
    | d=KW_DAY | d=KW_DAYS
    | h=KW_HOUR | h=KW_HOURS
    | m=KW_MINUTE | m=KW_MINUTES
    | s=KW_SECOND | s=KW_SECONDS
    ;

intervalQualifierPrecision
    : LPAREN Number RPAREN;



booleanValue
    :
    KW_TRUE | KW_FALSE
    ;

partitionSpec
    :
    KW_PARTITION
     LPAREN partitions+=partitionVal (COMMA partitions+=partitionVal)* RPAREN
    ;

partitionVal
    :
    identifier EQUAL (constant | variableRef)
    ;

// for compatibility
dateWithoutQuote:
    Num MINUS Num MINUS Num;


functionIdentifier
    : db=identifier (DOT|COLON) fn=identifier
    | builtin=COLON COLON fn=identifier
    | fn = identifier
    ;


//the new version of nonReserved + sql11ReservedKeywordsUsedAsIdentifier = old version of nonReserved
nonReserved
    :
    KW_ADD | KW_ADMIN | KW_AFTER | KW_ANALYZE | KW_ARCHIVE | KW_ASC | KW_BEFORE | KW_BUCKET | KW_BUCKETS
    | KW_CASCADE | KW_CHANGE | KW_CLUSTER | KW_CLUSTERED | KW_CLUSTERSTATUS | KW_COLLECTION | KW_COLUMNS
    | KW_COMMENT | KW_COMPACT | KW_COMPACTIONS | KW_COMPUTE | KW_CONCATENATE | KW_CONTINUE | KW_DATA | KW_DAY
    | KW_DATABASES | KW_DATETIME | KW_DBPROPERTIES | KW_DEFERRED | KW_DEFINED | KW_DELIMITED | KW_DEPENDENCY
    | KW_DESC | KW_DIRECTORIES | KW_DIRECTORY | KW_DISABLE | KW_DISTRIBUTE | KW_ELEM_TYPE
    | KW_ENABLE | KW_ESCAPED | KW_EXCLUSIVE | KW_EXPLAIN | KW_EXPORT | KW_FIELDS | KW_FILE | KW_FILEFORMAT
    | KW_FIRST | KW_FORMAT | KW_FORMATTED | KW_FUNCTIONS | KW_HOLD_DDLTIME | KW_HOUR | KW_IDXPROPERTIES | KW_IGNORE
    | KW_INDEX | KW_INDEXES | KW_INPATH | KW_INPUTDRIVER | KW_INPUTFORMAT | KW_ITEMS | KW_JAR
    | KW_KEYS | KW_KEY_TYPE | KW_LIMIT | KW_LINES | KW_LOAD | KW_LOCATION | KW_LOCK | KW_LOCKS | KW_LOGICAL | KW_LONG
    | KW_MAPJOIN |  KW_SKEWJOIN | KW_MATERIALIZED | KW_METADATA | KW_MINUS | KW_MINUTE | KW_MONTH | KW_MSCK | KW_NOSCAN | KW_NO_DROP | KW_OFFLINE
    | KW_OPTION | KW_OUTPUTDRIVER | KW_OUTPUTFORMAT | KW_OVERWRITE | KW_OWNER | KW_PARTITIONED | KW_PARTITIONS | KW_PLUS | KW_PRETTY
    | KW_PRINCIPALS | KW_PROTECTION | KW_PURGE | KW_READ | KW_READONLY | KW_REBUILD | KW_RECORDREADER | KW_RECORDWRITER
    | KW_REGEXP | KW_RELOAD | KW_RENAME | KW_REPAIR | KW_REPLACE | KW_REPLICATION | KW_RESTRICT | KW_REWRITE | KW_RLIKE
    | KW_ROLE | KW_ROLES | KW_SCHEMA | KW_SCHEMAS | KW_SECOND | KW_SEMI | KW_SERDE | KW_SERDEPROPERTIES | KW_SERVER | KW_SETS | KW_SHARED
    | KW_SHOW | KW_SHOW_DATABASE | KW_SKEWED | KW_SORT | KW_SORTED | KW_SSL | KW_STATISTICS | KW_STORED
    | KW_STREAMTABLE | KW_STRING | KW_STRUCT | KW_TABLES | KW_TBLPROPERTIES | KW_TEMPORARY | KW_TERMINATED
    | KW_TINYINT | KW_TOUCH | KW_TRANSACTIONS | KW_UNARCHIVE | KW_UNDO | KW_UNIONTYPE | KW_UNLOCK | KW_UNSET
    | KW_UNSIGNED | KW_URI | KW_USE | KW_UTC | KW_UTCTIMESTAMP | KW_VALUE_TYPE | KW_VIEW | KW_WHILE | KW_YEAR
    | KW_MAP | KW_REDUCE | KW_SEMI | KW_ANTI | KW_CUBE | KW_PRIMARY | KW_KEY | KW_VALIDATE | KW_NOVALIDATE | KW_RELY | KW_NORELY
    | KW_OUTPUT | KW_YEARS | KW_MONTHS | KW_DAYS | KW_HOURS | KW_MINUTES | KW_SECONDS | KW_MATCHED
    | KW_CODE_BEGIN | KW_CODE_END
    | KW_HISTORY | KW_RESTORE | KW_LSN
    | KW_ZORDER
    | KW_SQL
    | KW_MODEL | KW_PROPERTIES
    | KW_UNLOAD
    | KW_WITHIN | KW_FILTER
    | KW_EXCLUDE | KW_TIES | KW_NO | KW_OTHERS
    | KW_TRANSFORM
    ;

//The following SQL2011 reserved keywords are used as cast function name only, it is a subset of the sql11ReservedKeywordsUsedAsIdentifier.
sql11ReservedKeywordsUsedAsCastFunctionName
    :
    KW_BIGINT | KW_BINARY | KW_BOOLEAN | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_DATE | KW_DOUBLE | KW_FLOAT | KW_INT | KW_SMALLINT | KW_TIMESTAMP
    ;

//The following SQL2011 reserved keywords are used as identifiers in many q tests, they may be added back due to backward compatibility.
sql11ReservedKeywordsUsedAsIdentifier
    :
    KW_ALL | KW_ALTER | KW_ARRAY | KW_AS | KW_AUTHORIZATION | KW_BETWEEN | KW_BIGINT | KW_BINARY | KW_BOOLEAN
    | KW_BOTH | KW_BY | KW_CREATE | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_CURSOR | KW_DATE | KW_DECIMAL | KW_DELETE | KW_DESCRIBE
    | KW_DOUBLE | KW_DROP | KW_EXISTS | KW_EXTERNAL | KW_FALSE | KW_FETCH | KW_FLOAT | KW_FOR | KW_FULL | KW_GRANT
    | KW_GROUP | KW_GROUPING | KW_IMPORT | KW_IN | KW_INNER | KW_INSERT | KW_INT | KW_INTERSECT | KW_INTO | KW_IS | KW_LATERAL
    | KW_LEFT | KW_LIKE | KW_LOCAL | KW_NONE | KW_NULL | KW_OF | KW_ORDER | KW_OUT | KW_OUTER | KW_PARTITION
    | KW_PERCENT | KW_PROCEDURE | KW_RANGE | KW_READS | KW_REVOKE | KW_RIGHT
    | KW_ROLLUP | KW_ROW | KW_ROWS | KW_SET | KW_SMALLINT | KW_TABLE | KW_TIMESTAMP | KW_TO | KW_TRIGGER | KW_TRUE
    | KW_TRUNCATE | KW_UNION | KW_UPDATE | KW_USER | KW_USING | KW_VALUES | KW_WITH | KW_WINDOW | KW_NATURAL
    ;

reserved
    :
    KW_AND | KW_OR | KW_NOT | KW_LIKE | KW_IF | KW_HAVING | KW_FROM | KW_SELECT | KW_DISTINCT | KW_UNIQUEJOIN |
    KW_PRESERVE | KW_JOIN | KW_ON | KW_COLUMN  | KW_CHAR | KW_VARCHAR | KW_TABLESAMPLE |
    KW_CAST | KW_MACRO | KW_EXTENDED | KW_CASE | KW_WHEN | KW_THEN | KW_ELSE | KW_END | KW_CROSS | KW_UNBOUNDED | KW_PRECEDING |
    KW_FOLLOWING | KW_CURRENT | KW_PARTIALSCAN | KW_OVER | KW_WHERE
    ;