package lexer

import "fmt"

// Position 位置信息 - 参考TiDB设计，提供精确的位置跟踪
type Position struct {
	Line   int // 行号，从1开始
	Column int // 列号，从1开始
	Offset int // 字符偏移量，从0开始
}

// NewPosition 创建新的位置信息
func NewPosition(line, column, offset int) Position {
	return Position{
		Line:   line,
		Column: column,
		Offset: offset,
	}
}

// String 返回位置的字符串表示
func (p Position) String() string {
	return fmt.Sprintf("line %d, column %d", p.Line, p.Column)
}

// IsValid 检查位置是否有效
func (p Position) IsValid() bool {
	return p.Line > 0 && p.Column > 0 && p.Offset >= 0
}

// Before 检查当前位置是否在另一个位置之前
func (p Position) Before(other Position) bool {
	return p.Offset < other.Offset
}

// After 检查当前位置是否在另一个位置之后
func (p Position) After(other Position) bool {
	return p.Offset > other.Offset
}

// Equal 检查两个位置是否相等
func (p Position) Equal(other Position) bool {
	return p.Line == other.Line && p.Column == other.Column && p.Offset == other.Offset
}

// PositionRange 位置范围
type PositionRange struct {
	Start Position
	End   Position
}

// NewPositionRange 创建新的位置范围
func NewPositionRange(start, end Position) PositionRange {
	return PositionRange{
		Start: start,
		End:   end,
	}
}

// String 返回位置范围的字符串表示
func (pr PositionRange) String() string {
	if pr.Start.Line == pr.End.Line {
		return fmt.Sprintf("line %d, columns %d-%d", pr.Start.Line, pr.Start.Column, pr.End.Column)
	}
	return fmt.Sprintf("lines %d-%d", pr.Start.Line, pr.End.Line)
}

// IsValid 检查位置范围是否有效
func (pr PositionRange) IsValid() bool {
	return pr.Start.IsValid() && pr.End.IsValid() && !pr.Start.After(pr.End)
}

// Contains 检查位置范围是否包含指定位置
func (pr PositionRange) Contains(pos Position) bool {
	return !pos.Before(pr.Start) && !pos.After(pr.End)
}

// Length 返回位置范围的长度
func (pr PositionRange) Length() int {
	if !pr.IsValid() {
		return 0
	}
	return pr.End.Offset - pr.Start.Offset
}