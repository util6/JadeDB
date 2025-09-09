package lexer

import "strings"

// KeywordTable 关键字映射表 - 参考TiDB设计
var KeywordTable = map[string]TokenType{
	// SQL关键字
	"ADD":        ADD,
	"ALL":        ALL,
	"ALTER":      ALTER,
	"AND":        AND,
	"AS":         AS,
	"ASC":        ASC,
	"BEGIN":      BEGIN,
	"BETWEEN":    BETWEEN,
	"BY":         BY,
	"CASE":       CASE,
	"CHECK":      CHECK,
	"COLUMN":     COLUMN,
	"COMMIT":     COMMIT,
	"CONSTRAINT": CONSTRAINT,
	"CREATE":     CREATE,
	"CROSS":      CROSS,
	"DATABASE":   DATABASE,
	"DEFAULT":    DEFAULT,
	"DELETE":     DELETE,
	"DESC":       DESC,
	"DISTINCT":   DISTINCT,
	"DROP":       DROP,
	"ELSE":       ELSE,
	"END":        END,
	"EXCEPT":     EXCEPT,
	"EXISTS":     EXISTS,
	"FALSE":      FALSE_LIT,
	"FOREIGN":    FOREIGN,
	"FROM":       FROM,
	"FULL":       FULL,
	"GROUP":      GROUP,
	"HAVING":     HAVING,
	"IF":         IF,
	"IN":         IN,
	"INDEX":      INDEX,
	"INNER":      INNER,
	"INSERT":     INSERT,
	"INTERSECT":  INTERSECT,
	"INTO":       INTO,
	"IS":         IS,
	"JOIN":       JOIN,
	"KEY":        KEY,
	"LEFT":       LEFT,
	"LIKE":       LIKE,
	"LIMIT":      LIMIT,
	"NOT":        NOT,
	"NULL":       NULL_LIT,
	"OFFSET":     OFFSET,
	"ON":         ON,
	"OR":         OR,
	"ORDER":      ORDER,
	"OUTER":      OUTER,
	"PRIMARY":    PRIMARY,
	"REFERENCES": REFERENCES,
	"RIGHT":      RIGHT,
	"ROLLBACK":   ROLLBACK,
	"SELECT":     SELECT,
	"SET":        SET,
	"TABLE":      TABLE,
	"THEN":       THEN,
	"TRUE":       TRUE_LIT,
	"UNION":      UNION,
	"UNIQUE":     UNIQUE,
	"UPDATE":     UPDATE,
	"VALUES":     VALUES,
	"WHEN":       WHEN,
	"WHERE":      WHERE,

	// 数据类型关键字
	"BIGINT":     BIGINT,
	"BINARY":     BINARY,
	"BIT":        BIT,
	"BLOB":       BLOB,
	"BOOLEAN":    BOOLEAN,
	"CHAR":       CHAR,
	"DATE":       DATE,
	"DATETIME":   DATETIME,
	"DECIMAL":    DECIMAL,
	"DOUBLE":     DOUBLE,
	"ENUM":       ENUM,
	"FLOAT":      FLOAT,
	"GEOMETRY":   GEOMETRY,
	"INT":        INT,
	"INTEGER":    INTEGER,
	"JSON":       JSON,
	"LONGBLOB":   LONGBLOB,
	"LONGTEXT":   LONGTEXT,
	"MEDIUMBLOB": MEDIUMBLOB,
	"MEDIUMINT":  MEDIUMINT,
	"MEDIUMTEXT": MEDIUMTEXT,
	"NUMERIC":    NUMERIC,
	"REAL":       REAL,
	"SMALLINT":   SMALLINT,
	"TEXT":       TEXT,
	"TIME":       TIME,
	"TIMESTAMP":  TIMESTAMP,
	"TINYBLOB":   TINYBLOB,
	"TINYINT":    TINYINT,
	"TINYTEXT":   TINYTEXT,
	"VARBINARY":  VARBINARY,
	"VARCHAR":    VARCHAR,
	"YEAR":       YEAR,

	// 函数名关键字
	"AVG":   AVG,
	"COUNT": COUNT,
	"MAX":   MAX,
	"MIN":   MIN,
	"SUM":   SUM,
}

// LookupKeyword 查找关键字，大小写不敏感
func LookupKeyword(ident string) TokenType {
	if tok, exists := KeywordTable[strings.ToUpper(ident)]; exists {
		return tok
	}
	return IDENTIFIER
}

// IsKeyword 检查是否为关键字
func IsKeyword(tokenType TokenType) bool {
	return tokenType > WHITESPACE && tokenType < MAX_TOKEN
}

// IsReservedKeyword 检查是否为保留关键字
func IsReservedKeyword(tokenType TokenType) bool {
	switch tokenType {
	case SELECT, FROM, WHERE, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER:
		return true
	case TABLE, INDEX, DATABASE, COLUMN, CONSTRAINT:
		return true
	case PRIMARY, FOREIGN, UNIQUE, NOT, NULL_KW:
		return true
	case AND, OR, IN, LIKE, BETWEEN, EXISTS:
		return true
	default:
		return false
	}
}

// TokenName 返回Token类型的名称
func TokenName(tokenType TokenType) string {
	switch tokenType {
	case INVALID:
		return "INVALID"
	case EOF:
		return "EOF"
	case IDENTIFIER:
		return "IDENTIFIER"
	case INTEGER_LIT:
		return "INTEGER"
	case FLOAT_LIT:
		return "FLOAT"
	case STRING_LIT:
		return "STRING"
	case HEX_LIT:
		return "HEX"
	case BIT_LIT:
		return "BIT"
	case NULL_LIT:
		return "NULL"
	case TRUE_LIT:
		return "TRUE"
	case FALSE_LIT:
		return "FALSE"
	case COMMENT:
		return "COMMENT"
	case WHITESPACE:
		return "WHITESPACE"
	case PLUS:
		return "+"
	case MINUS:
		return "-"
	case MULTIPLY:
		return "*"
	case DIVIDE:
		return "/"
	case MODULO:
		return "%"
	case EQUAL:
		return "="
	case NOT_EQUAL:
		return "!="
	case LESS:
		return "<"
	case LESS_EQUAL:
		return "<="
	case GREATER:
		return ">"
	case GREATER_EQUAL:
		return ">="
	case NULL_SAFE_EQUAL:
		return "<=>"
	case AND_OP:
		return "&&"
	case OR_OP:
		return "||"
	case NOT_OP:
		return "!"
	case BIT_AND:
		return "&"
	case BIT_OR:
		return "|"
	case BIT_XOR:
		return "^"
	case BIT_NOT:
		return "~"
	case LEFT_SHIFT:
		return "<<"
	case RIGHT_SHIFT:
		return ">>"
	case ASSIGN:
		return ":="
	case COMMA:
		return ","
	case SEMICOLON:
		return ";"
	case LEFT_PAREN:
		return "("
	case RIGHT_PAREN:
		return ")"
	case LEFT_BRACE:
		return "{"
	case RIGHT_BRACE:
		return "}"
	case LEFT_BRACKET:
		return "["
	case RIGHT_BRACKET:
		return "]"
	case DOT:
		return "."
	case QUESTION:
		return "?"
	case COLON:
		return ":"
	case DOUBLE_COLON:
		return "::"
	default:
		// 对于关键字，查找反向映射
		for keyword, keywordTokenType := range KeywordTable {
			if keywordTokenType == tokenType {
				return keyword
			}
		}
		return "UNKNOWN"
	}
}
