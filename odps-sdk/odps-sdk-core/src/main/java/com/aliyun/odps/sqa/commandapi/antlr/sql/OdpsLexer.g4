lexer grammar OdpsLexer;

channels { WS_CHANNEL, COMMENT_CHANNEL }

// make keyword case insensitive
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

// Keywords

KW_TRUE : T R U E;
KW_FALSE : F A L S E;
KW_ALL : A L L;
KW_NONE: N O N E;
KW_AND : A N D;
KW_OR : O R;
KW_NOT : N O T | '!';
KW_LIKE : L I K E;

KW_IF : I F;
KW_EXISTS : E X I S T S;

KW_ASC : A S C;
KW_DESC : D E S C;
KW_ORDER : O R D E R;
KW_ZORDER : Z O R D E R;
KW_GROUP : G R O U P;
KW_GROUPS : G R O U P S;
KW_BY : B Y;
KW_HAVING : H A V I N G;
KW_WHERE : W H E R E;
KW_FROM : F R O M;
KW_AS : A S;
KW_SELECT : S E L E C T;
KW_DISTINCT : D I S T I N C T;
KW_INSERT : I N S E R T;
KW_OVERWRITE : O V E R W R I T E;
KW_OUTER : O U T E R;
KW_UNIQUEJOIN : U N I Q U E J O I N;
KW_PRESERVE : P R E S E R V E;
KW_JOIN : J O I N;
KW_LEFT : L E F T;
KW_RIGHT : R I G H T;
KW_FULL : F U L L;
KW_ON : O N;
KW_PARTITION : P A R T I T I O N;
KW_PARTITIONS : P A R T I T I O N S;
KW_TABLE: T A B L E;
KW_TABLES: T A B L E S;
KW_COLUMNS: C O L U M N S;
KW_INDEX: I N D E X;
KW_INDEXES: I N D E X E S;
KW_REBUILD: R E B U I L D;
KW_FUNCTIONS: F U N C T I O N S;
KW_SHOW: S H O W;
KW_MSCK: M S C K;
KW_REPAIR: R E P A I R;
KW_DIRECTORY: D I R E C T O R Y;
KW_LOCAL: L O C A L;
KW_TRANSFORM : T R A N S F O R M;
KW_USING: U S I N G;
KW_CLUSTER: C L U S T E R;
KW_DISTRIBUTE: D I S T R I B U T E;
KW_SORT: S O R T;
KW_UNION: U N I O N;
KW_LOAD: L O A D;
KW_UNLOAD: U N L O A D;
KW_EXPORT: E X P O R T;
KW_IMPORT: I M P O R T;
KW_REPLICATION: R E P L I C A T I O N;
KW_METADATA: M E T A D A T A;
KW_DATA: D A T A;
KW_INPATH: I N P A T H;
KW_IS: I S;
KW_NULL: N U L L;
KW_CREATE: C R E A T E;
KW_EXTERNAL: E X T E R N A L;
KW_ALTER: A L T E R;
KW_CHANGE: C H A N G E;
KW_COLUMN: C O L U M N;
KW_FIRST: F I R S T;
KW_LAST: L A S T;
KW_NULLS: N U L L S;
KW_AFTER: A F T E R;
KW_DESCRIBE: D E S C R I B E;
KW_DROP: D R O P;
KW_RENAME: R E N A M E;
KW_IGNORE: I G N O R E;
KW_PROTECTION: P R O T E C T I O N;
KW_TO: T O;
KW_COMMENT: C O M M E N T;
KW_BOOLEAN: B O O L E A N;
KW_TINYINT: T I N Y I N T;
KW_SMALLINT: S M A L L I N T;
KW_INT: I N T;
KW_BIGINT: B I G I N T;
KW_FLOAT: F L O A T;
KW_DOUBLE: D O U B L E;
KW_DATE: D A T E;
KW_DATETIME: D A T E T I M E;
KW_TIMESTAMP: T I M E S T A M P;
KW_INTERVAL: I N T E R V A L;
KW_DECIMAL: D E C I M A L;
KW_STRING: S T R I N G;
KW_CHAR: C H A R;
KW_VARCHAR: V A R C H A R;
KW_ARRAY: A R R A Y;
KW_STRUCT: S T R U C T;
KW_MAP: M A P;
KW_UNIONTYPE: U N I O N T Y P E;
KW_REDUCE: R E D U C E;
KW_PARTITIONED: P A R T I T I O N E D;
KW_CLUSTERED: C L U S T E R E D;
KW_SORTED: S O R T E D;
KW_INTO: I N T O;
KW_BUCKETS: B U C K E T S;
KW_ROW: R O W;
KW_ROWS: R O W S;
KW_FORMAT: F O R M A T;
KW_DELIMITED: D E L I M I T E D;
KW_FIELDS: F I E L D S;
KW_TERMINATED: T E R M I N A T E D;
KW_ESCAPED: E S C A P E D;
KW_COLLECTION: C O L L E C T I O N;
KW_ITEMS: I T E M S;
KW_KEYS: K E Y S;
KW_KEY_TYPE: '$' K E Y '$';
KW_LINES: L I N E S;
KW_STORED: S T O R E D;
KW_FILEFORMAT: F I L E F O R M A T;
KW_INPUTFORMAT: I N P U T F O R M A T;
KW_OUTPUTFORMAT: O U T P U T F O R M A T;
KW_INPUTDRIVER: I N P U T D R I V E R;
KW_OUTPUTDRIVER: O U T P U T D R I V E R;
KW_OFFLINE: O F F L I N E;
KW_ENABLE: E N A B L E;
KW_DISABLE: D I S A B L E;
KW_READONLY: R E A D O N L Y;
KW_NO_DROP: N O '_' D R O P;
KW_LOCATION: L O C A T I O N;
KW_TABLESAMPLE: T A B L E S A M P L E;
KW_BUCKET: B U C K E T;
KW_OUT: O U T;
KW_OF: O F;
KW_PERCENT: P E R C E N T;
KW_CAST: C A S T;
KW_ADD: A D D;
KW_REPLACE: R E P L A C E;
KW_RLIKE: R L I K E;
KW_REGEXP: R E G E X P;
KW_TEMPORARY: T E M P O R A R Y;
KW_FUNCTION: F U N C T I O N;
KW_MACRO: M A C R O;
KW_FILE: F I L E;
KW_JAR: J A R;
KW_EXPLAIN: E X P L A I N;
KW_EXTENDED: E X T E N D E D;
KW_FORMATTED: F O R M A T T E D;
KW_PRETTY: P R E T T Y;
KW_DEPENDENCY: D E P E N D E N C Y;
KW_LOGICAL: L O G I C A L;
KW_SERDE: S E R D E;
KW_WITH: W I T H;
KW_DEFERRED:  D E F E R R E D;
KW_SERDEPROPERTIES: S E R D E P R O P E R T I E S;
KW_DBPROPERTIES: D B P R O P E R T I E S;
KW_LIMIT: L I M I T;
KW_OFFSET: O F F S E T;
KW_SET: S E T;
KW_UNSET: U N S E T;
KW_TBLPROPERTIES: T B L P R O P E R T I E S;
KW_IDXPROPERTIES: I D X P R O P E R T I E S;
KW_VALUE_TYPE: '$' V A L U E '$';
KW_ELEM_TYPE: '$' E L E M '$';
KW_DEFINED: D E F I N E D;
KW_CASE: C A S E;
KW_WHEN: W H E N;
KW_THEN: T H E N;
KW_ELSE: E L S E;
KW_END: E N D;
KW_MAPJOIN: M A P J O I N;
KW_SKEWJOIN: S K E W J O I N;
KW_DYNAMICFILTER: D Y N A M I C F I L T E R;
KW_STREAMTABLE: S T R E A M T A B L E;
KW_HOLD_DDLTIME: H O L D '_' D D L T I M E;
KW_CLUSTERSTATUS: C L U S T E R S T A T U S;
KW_UTC: U T C;
KW_UTCTIMESTAMP: U T C '_' T M E S T A M P;
KW_LONG: L O N G;
KW_DELETE: D E L E T E;
KW_PLUS: P L U S;
KW_MINUS: M I N U S;
KW_FETCH: F E T C H;
KW_INTERSECT: I N T E R S E C T;
KW_VIEW: V I E W;
KW_IN:  I N;
KW_DATABASE: D A T A B A S E;
KW_DATABASES: D A T A B A S E S;
KW_MATERIALIZED: M A T E R I A L I Z E D;
KW_SCHEMA: S C H E M A;
KW_SCHEMAS: S C H E M A S;
KW_GRANT: G R A N T;
KW_REVOKE: R E V O K E;
KW_SSL: S S L;
KW_UNDO: U N D O;
KW_LOCK: L O C K;
KW_LOCKS: L O C K S;
KW_UNLOCK: U N L O C K;
KW_SHARED: S H A R E D;
KW_EXCLUSIVE: E X C L U S I V E;
KW_PROCEDURE: P R O C E D U R E;
KW_UNSIGNED: U N S I G N E D;
KW_WHILE: W H I L E;
KW_READ: R E A D;
KW_READS: R E A D S;
KW_PURGE: P U R G E;
KW_RANGE: R A N G E;
KW_ANALYZE: A N A L Y Z E;
KW_BEFORE: B E F O R E;
KW_BETWEEN: B E T W E E N;
KW_BOTH: B O T H;
KW_BINARY: B I N A R Y;
KW_CROSS: C R O S S;
KW_CONTINUE: C O N T I N U E;
KW_CURSOR: C U R S O R;
KW_TRIGGER: T R I G G E R;
KW_RECORDREADER: R E C O R D R E A D E R;
KW_RECORDWRITER: R E C O R D W R I T E R;
KW_SEMI: S E M I;
KW_ANTI: A N T I;
KW_LATERAL: L A T E R A L;
KW_TOUCH: T O U C H;
KW_ARCHIVE: A R C H I V E;
KW_UNARCHIVE: U N A R C H I V E;
KW_COMPUTE: C O M P U T E;
KW_STATISTICS: S T A T I S T I C S;
KW_NULL_VALUE: N U L L '_' V A L U E;
KW_DISTINCT_VALUE: D I S T I N C T '_' V A L U E;
KW_TABLE_COUNT: T A B L E '_' C O U N T;
KW_COLUMN_SUM: C O L U M N '_' S U M;
KW_COLUMN_MAX: C O L U M N '_' M A X;
KW_COLUMN_MIN: C O L U M N '_' M I N;
KW_EXPRESSION_CONDITION: E X P R E S S I O N '_' C O N D I T I O N;
KW_USE: U S E;
KW_OPTION: O P T I O N;
KW_CONCATENATE: C O N C A T E N A T E;
KW_SHOW_DATABASE: S H O W '_' D A T A B A S E;
KW_UPDATE: U P D A T E;
KW_MATCHED: M A T C H E D;
KW_RESTRICT: R E S T R I C T;
KW_CASCADE: C A S C A D E;
KW_SKEWED: S K E W E D;
KW_ROLLUP: R O L L U P;
KW_CUBE: C U B E;
KW_DIRECTORIES: D I R E C T O R I E S;
KW_FOR: F O R;
KW_WINDOW: W I N D O W;
KW_UNBOUNDED: U N B O U N D E D;
KW_PRECEDING: P R E C E D I N G;
KW_FOLLOWING: F O L L O W I N G;
KW_CURRENT: C U R R E N T;
KW_LOCALTIMESTAMP: L O C A L T I M E S T A M P;
KW_CURRENT_DATE: C U R R E N T '_' D A T E;
KW_CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P;
KW_LESS: L E S S;
KW_MORE: M O R E;
KW_OVER: O V E R;
KW_GROUPING: G R O U P I N G;
KW_SETS: S E T S;
KW_TRUNCATE: T R U N C A T E;
KW_NOSCAN: N O S C A N;
KW_PARTIALSCAN: P A R T I A L S C A N;
KW_USER: U S E R;
KW_ROLE: R O L E;
KW_ROLES: R O L E S;
KW_INNER: I N N E R;
KW_EXCHANGE: E X C H A N G E;
KW_URI: U R I;
KW_SERVER : S E R V E R;
KW_ADMIN: A D M I N;
KW_OWNER: O W N E R;
KW_PRINCIPALS: P R I N C I P A L S;
KW_COMPACT: C O M P A C T;
KW_COMPACTIONS: C O M P A C T I O N S;
KW_TRANSACTIONS: T R A N S A C T I O N S;
KW_REWRITE : R E W R I T E;
KW_AUTHORIZATION: A U T H O R I Z A T I O N;
KW_CONF: C O N F;
KW_VALUES: V A L U E S;
KW_RELOAD: R E L O A D;
KW_YEAR: Y E A R;
KW_MONTH: M O N T H;
KW_DAY: D A Y;
KW_HOUR: H O U R;
KW_MINUTE: M I N U T E;
KW_SECOND: S E C O N D;
KW_YEARS: Y E A R S;
KW_MONTHS: M O N T H S;
KW_DAYS: D A Y S;
KW_HOURS: H O U R S;
KW_MINUTES: M I N U T E S;
KW_SECONDS: S E C O N D S;
KW_UDFPROPERTIES: U D F P R O P E R T I E S;
KW_EXCLUDE : E X C L U D E;
KW_TIES: T I E S;
KW_NO: N O;
KW_OTHERS: O T H E R S;
// odps extensions

KW_BEGIN: B E G I N;
KW_RETURNS : R E T U R N S;
KW_SQL : S Q L;
KW_LOOP: L O O P;
KW_NEW: 'new';
KW_LIFECYCLE: L I F E C Y C L E;
KW_REMOVE: R E M O V E;
KW_GRANTS: G R A N T S;
KW_ACL: A C L;
KW_TYPE: T Y P E;
KW_LIST: L I S T;
KW_USERS: U S E R S;
KW_WHOAMI: W H O A M I;
KW_TRUSTEDPROJECTS: T R U S T E D P R O J E C T S;
KW_TRUSTEDPROJECT: T R U S T E D P R O J E C T;
KW_SECURITYCONFIGURATION: S E C U R I T Y C O N F I G U R A T I O N;
KW_PRIVILEGES: P R I V (I L E G E)? S;
KW_PROJECT: P R O J E C T;
KW_PROJECTS: P R O J E C T S;
KW_LABEL: L A B E L;
KW_ALLOW: A L L O W;
KW_DISALLOW: D I S A L L O W;
KW_PACKAGE: P A C K A G E;
KW_PACKAGES: P A C K A G E S;
KW_INSTALL: I N S T A L L;
KW_UNINSTALL: U N I N S T A L L;
KW_P: P;
KW_JOB: J O B;
KW_JOBS: J O B S;
KW_ACCOUNTPROVIDERS: A C C O U N T P R O V I D E R S;
KW_RESOURCES: R E S O U R C E S;
KW_FLAGS: F L A G S;
KW_COUNT: C O U N T;
KW_STATISTIC: S T A T I S T I C;
KW_STATISTIC_LIST: S T A T I S T I C '_' L I S T;
KW_GET: G E T;
KW_PUT: P U T;
KW_POLICY: P O L I C Y;
KW_PROJECTPROTECTION: P R O J E C T P R O T E C T I O N;
KW_EXCEPTION: E X C E P T I O N;
KW_CLEAR: C L E A R;
KW_EXPIRED: E X P I R E D;
KW_EXP: E X P;
KW_ACCOUNTPROVIDER: A C C O U N T P R O V I D E R;
KW_SUPER: S U P E R;
KW_VOLUMEFILE: V O L U M E F I L E;
KW_VOLUMEARCHIVE: V O L U M E A R C H I V E;
KW_OFFLINEMODEL: O F F L I N E M O D E L;
KW_PY: P Y;
KW_RESOURCE: R E S O U R C E;
KW_KILL: K I L L;
KW_STATUS: S T A T U S;
KW_SETPROJECT: S E T P R O J E C T;
KW_MERGE: M E R G E;
KW_SMALLFILES: S M A L L F I L E S;
KW_PARTITIONPROPERTIES: P A R T I T I O N P R O P E R T I E S;
KW_EXSTORE: E X S T O R E;
KW_CHANGELOGS: C H A N G E L O G S;
KW_REDO: R E D O;
KW_CHANGEOWNER: C H A N G E O W N E R;
KW_RECYCLEBIN: R E C Y C L E B I N;
KW_PRIVILEGEPROPERTIES: P R I V I L E G E P R O P E R T I E S;
KW_CACHE: C A C H E;
KW_CACHEPROPERTIES: C A C H E P R O P E R T I E S;
KW_VARIABLES: V A R I A B L E S;
KW_EXCEPT: E X C E P T;
KW_SELECTIVITY: S E L E C T I V I T Y;
KW_EXTRACT: E X T R A C T;
KW_SUBSTRING: S U B S T R I N G;
KW_DEFAULT: D E F A U L T;
KW_ANY: A N Y;
KW_NATURAL: N A T U R A L;
KW_CONSTRAINT: C O N S T R A I N T;
KW_PRIMARY: P R I M A R Y;
KW_KEY: K E Y;
KW_VALIDATE: V A L I D A T E;
KW_NOVALIDATE: N O V A L I D A T E;
KW_RELY: R E L Y;
KW_NORELY: N O R E L Y;
KW_CLONE: C L O N E;
KW_HISTORY: H I S T O R Y;
KW_RESTORE: R E S T O R E;
KW_LSN: L S N;
KW_WITHIN: W I T H I N;
KW_FILTER: F I L T E R;
KW_TENANT: T E N A N T;
// stream sql
KW_SHARDS: S H A R D S;
KW_HUBLIFECYCLE: H U B L I F E C Y C L E;
KW_HUBTABLE: H U B T A B L E;
KW_OUTPUT: O U T P U T;
KW_CODE_BEGIN: SHARP C O D E;
KW_CODE_END: SHARP E N D WS C O D E;
KW_MODEL: M O D E L;
KW_PROPERTIES: P R O P E R T I E S;

// Operators
// NOTE: if you add a new function/operator, add it to sysFuncNames so that describe function _FUNC_ will work.

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' | '\uFF0C' ;
SEMICOLON : ';' | '\uFF1B' ;

LPAREN : '(' | '\uFF08' ;
RPAREN : ')' | '\uFF09' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : D I V;

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
CONCATENATE : '||';
BITWISEXOR : '^';
QUESTION : '?';
DOLLAR : '$';

SHARP : '#';

ASSIGN: ':=';

LAMBDA_IMPLEMENT: '->';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    E ( PLUS|MINUS )? (Digit)+
    ;

//fragment
//RegexComponent
//    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
//    | PLUS | STAR | QUESTION | MINUS | DOT
//    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
//    | BITWISEXOR | BITWISEOR | DOLLAR
//    ;

StringLiteral
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    ;

DoubleQuoteStringLiteral
    : '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

    // Verbatium String Literal
//    | '@\'' (~'\'' | '\'\'')* '\''
//    | '@"' (~'"' | '""')* '"'
//    ;

BigintLiteral
    :
    (Digit)+ L
    ;

SmallintLiteral
    :
    (Digit)+ S
    ;

TinyintLiteral
    :
    (Digit)+ Y
    ;

DecimalLiteral
    :
    Number B D
    ;

ByteLengthLiteral
    :
    (Digit)+ (B | K | M | G)
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;

Variable
    : '@' (Letter | Digit | '_')+;

/*
An Identifier can be:
- tableName
- columnName
- select expr alias
- lateral view aliases
- database name
- view name
- subquery alias
- function name
- ptf argument identifier
- index name
- property name for: db,tbl,partition...
- fileFormat
- role name
- privilege name
- principal name
- macro name
- hint name
- window name
*/

// Ported from Java.g
fragment
IDLetter
    :  '\u0024' |
       '\u0041'..'\u005a' |
       '\u005f' |
       '\u0061'..'\u007a' |
       '\u00c0'..'\u00d6' |
       '\u00d8'..'\u00f6' |
       '\u00f8'..'\u00ff' |
       '\u0100'..'\u1fff' |
       '\u3040'..'\u318f' |
       '\u3300'..'\u337f' |
       '\u3400'..'\u3d2d' |
       '\u4e00'..'\u9fff' |
       '\uf900'..'\ufaff'
    ;

fragment
IDDigit
    :  '\u0030'..'\u0039' |
       '\u0660'..'\u0669' |
       '\u06f0'..'\u06f9' |
       '\u0966'..'\u096f' |
       '\u09e6'..'\u09ef' |
       '\u0a66'..'\u0a6f' |
       '\u0ae6'..'\u0aef' |
       '\u0b66'..'\u0b6f' |
       '\u0be7'..'\u0bef' |
       '\u0c66'..'\u0c6f' |
       '\u0ce6'..'\u0cef' |
       '\u0d66'..'\u0d6f' |
       '\u0e50'..'\u0e59' |
       '\u0ed0'..'\u0ed9' |
       '\u1040'..'\u1049'
   ;

fragment
Substitution
    :
    DOLLAR LCURLY (IDLetter | IDDigit) (IDLetter | IDDigit | '_')* RCURLY
    ;

Identifier
    :
    (IDLetter | IDDigit) (IDLetter | IDDigit | '_' | Substitution)*
    | QuotedIdentifier
//    | '`' RegexComponent+ '`'
    ;

QuotedIdentifier
    :
    '`'  ( '``' | ~('`') )* '`'
    ;

fragment
CharSetName
    :
    '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

fragment
CharSetLiteral
    :
    StringLiteral
    | DoubleQuoteStringLiteral
    | '0' X (HexDigit|Digit)+
    ;

CharSetStringLiteral
    : CharSetName WS* CharSetLiteral;

// move whitespaces and comments to separate hidden channels, ODPS Studio will use these tokens
WS  :  (' '|'\r'|'\t'|'\n') -> channel(WS_CHANNEL)
    ;

COMMENT
  : '--' (~('\n'|'\r'))* -> channel(COMMENT_CHANNEL)
  ;

HintStart:
    '/' [ \r\t]* '*' [ \r\t]* '+';

//
//BlockComment:
//    ('/*' [ \r\t]* '*/' | '/*' [ \r\t]* ~[ \r\t+] .*? '*/') -> channel(COMMENT);

ESCAPE:
    '\\';

AT:
    '@';

UNDERLINE:
    '_';

ANY_CHAR:
    .;