package lexer

import (
	"testing"
)

func TestTokenCreation(t *testing.T) {
	pos := NewPosition(1, 1, 0)
	token := NewToken(SELECT, "SELECT", "SELECT", pos)

	if token.Type != SELECT {
		t.Errorf("Expected token type SELECT, got %v", token.Type)
	}

	if token.Value != "SELECT" {
		t.Errorf("Expected token value 'SELECT', got %s", token.Value)
	}

	if !token.IsKeyword() {
		t.Error("Expected SELECT to be a keyword")
	}

	if token.IsLiteral() {
		t.Error("Expected SELECT not to be a literal")
	}
}

func TestKeywordLookup(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"SELECT", SELECT},
		{"select", SELECT}, // 大小写不敏感
		{"Select", SELECT},
		{"FROM", FROM},
		{"WHERE", WHERE},
		{"unknown", IDENTIFIER},
		{"", IDENTIFIER},
	}

	for _, test := range tests {
		result := LookupKeyword(test.input)
		if result != test.expected {
			t.Errorf("LookupKeyword(%s) = %v, expected %v", test.input, result, test.expected)
		}
	}
}

func TestTokenName(t *testing.T) {
	tests := []struct {
		tokenType TokenType
		expected  string
	}{
		{SELECT, "SELECT"},
		{IDENTIFIER, "IDENTIFIER"},
		{INTEGER_LIT, "INTEGER"},
		{PLUS, "+"},
		{EQUAL, "="},
		{LEFT_PAREN, "("},
		{EOF, "EOF"},
	}

	for _, test := range tests {
		result := TokenName(test.tokenType)
		if result != test.expected {
			t.Errorf("TokenName(%v) = %s, expected %s", test.tokenType, result, test.expected)
		}
	}
}

func TestPositionMethods(t *testing.T) {
	pos1 := NewPosition(1, 5, 4)
	pos2 := NewPosition(1, 10, 9)

	if !pos1.Before(pos2) {
		t.Error("pos1 should be before pos2")
	}

	if !pos2.After(pos1) {
		t.Error("pos2 should be after pos1")
	}

	if pos1.Equal(pos2) {
		t.Error("pos1 should not equal pos2")
	}

	// 测试位置范围
	range1 := NewPositionRange(pos1, pos2)
	if !range1.IsValid() {
		t.Error("range1 should be valid")
	}

	midPos := NewPosition(1, 7, 6)
	if !range1.Contains(midPos) {
		t.Error("range1 should contain midPos")
	}
}

func TestTokenList(t *testing.T) {
	pos := NewPosition(1, 1, 0)
	tokens := TokenList{
		NewToken(SELECT, "SELECT", "SELECT", pos),
		NewToken(MULTIPLY, "*", "*", pos),
		NewToken(FROM, "FROM", "FROM", pos),
	}

	if tokens.Length() != 3 {
		t.Errorf("Expected token list length 3, got %d", tokens.Length())
	}

	first := tokens.First()
	if first == nil || first.Type != SELECT {
		t.Error("Expected first token to be SELECT")
	}

	last := tokens.Last()
	if last == nil || last.Type != FROM {
		t.Error("Expected last token to be FROM")
	}
}