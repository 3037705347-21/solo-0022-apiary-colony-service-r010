package store

import (
	"fmt"
	"sync"
	"time"

	"example.com/apiary-colony-service/internal/domain"
)

// Memory 是并发安全的内存仓储。所有写入都在写锁内完成状态校验，
// 读取则通过 Snapshot 得到一致视图。
type Memory struct {
	mu sync.RWMutex

	apiaries         map[string]domain.Apiary
	hives            map[string]domain.Hive
	inspectionLedger map[string]*inspectionLedger
	queens           map[string][]domain.Queen

	inspectionSeq int
	queenSeq      int
}

func NewMemory() *Memory {
	return &Memory{
		apiaries:         make(map[string]domain.Apiary),
		hives:            make(map[string]domain.Hive),
		inspectionLedger: make(map[string]*inspectionLedger),
		queens:           make(map[string][]domain.Queen),
	}
}

func (m *Memory) CreateApiary(apiary domain.Apiary) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.apiaries[apiary.ID]; ok {
		return domain.Conflict("蜂场 %q 已存在", apiary.ID)
	}
	m.apiaries[apiary.ID] = apiary
	return nil
}

func (m *Memory) GetApiary(id string) (domain.Apiary, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	apiary, ok := m.apiaries[id]
	return apiary, ok
}

func (m *Memory) CreateHive(hive domain.Hive) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.apiaries[hive.ApiaryID]; !ok {
		return domain.NotFound("蜂场", hive.ApiaryID)
	}
	if _, ok := m.hives[hive.ID]; ok {
		return domain.Conflict("蜂箱 %q 已存在", hive.ID)
	}
	m.hives[hive.ID] = hive
	return nil
}

func (m *Memory) GetHive(id string) (domain.Hive, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hive, ok := m.hives[id]
	return hive, ok
}

// AdvanceHive 在同一把写锁内完成状态读取、迁移校验与写入，避免并发迁移产生交错结果。
func (m *Memory) AdvanceHive(hiveID string, target domain.HiveState, _ time.Time) (domain.Transition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hive, ok := m.hives[hiveID]
	if !ok {
		return domain.Transition{}, domain.NotFound("蜂箱", hiveID)
	}
	if hive.State == target {
		return domain.Transition{}, domain.InvalidState("蜂箱 %q 已经处于状态 %q", hiveID, target)
	}
	if !domain.CanTransitionHive(hive.State, target) {
		return domain.Transition{}, domain.InvalidState(
			"蜂箱 %q 不允许从 %q 迁移到 %q", hiveID, hive.State, target)
	}
	from := hive.State
	hive.State = target
	m.hives[hiveID] = hive
	return domain.Transition{HiveID: hiveID, From: from, To: target}, nil
}

// inspectionLedger 是一只蜂箱已经落下的巡检行，以及这份台账记下的
// 最近一次巡检编号。
type inspectionLedger struct {
	order    []string
	rows     map[string]domain.Inspection
	latestID string
}

func newInspectionLedger() *inspectionLedger {
	return &inspectionLedger{rows: make(map[string]domain.Inspection)}
}

// append 把巡检追加到台账末尾，并把它登记为最近一次巡检。
func (l *inspectionLedger) append(row domain.Inspection) {
	l.order = append(l.order, row.ID)
	l.rows[row.ID] = row
	l.latestID = row.ID
}

// list 按台账顺序读出所有已经落下的巡检行。
func (l *inspectionLedger) list() []domain.Inspection {
	items := make([]domain.Inspection, 0, len(l.order))
	for _, id := range l.order {
		if id == "" {
			continue
		}
		row, ok := l.rows[id]
		if !ok {
			continue
		}
		items = append(items, row)
	}
	return items
}

// PrecheckInspection 在校验量测之前确认蜂箱存在且接受巡检，
// 让未找到与状态错误优先于量测校验错误返回；提交时仍会在写锁内复核。
func (m *Memory) PrecheckInspection(hiveID string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hive, ok := m.hives[hiveID]
	if !ok {
		return domain.NotFound("蜂箱", hiveID)
	}
	if !hive.AcceptsInspection() {
		return domain.InvalidState("蜂箱 %q 处于 %q 状态，不接受巡检", hiveID, hive.State)
	}
	return nil
}

// CommitInspection 在同一把写锁内复核蜂箱状态、分配巡检编号、把巡检追加到
// 台账并推进最近巡检时间。每一次被接受的提交都落下唯一且完整的一行；
// 未被接受的提交不会分配编号，也不会扰动已经落下的记录。
func (m *Memory) CommitInspection(hiveID string, row domain.Inspection) (domain.Inspection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hive, ok := m.hives[hiveID]
	if !ok {
		return domain.Inspection{}, domain.NotFound("蜂箱", hiveID)
	}
	if !hive.AcceptsInspection() {
		return domain.Inspection{}, domain.InvalidState(
			"蜂箱 %q 处于 %q 状态，不接受巡检", hiveID, hive.State)
	}
	ledger, ok := m.inspectionLedger[hiveID]
	if !ok {
		ledger = newInspectionLedger()
		m.inspectionLedger[hiveID] = ledger
	}
	m.inspectionSeq++
	row.ID = fmt.Sprintf("inspection-%06d", m.inspectionSeq)
	row.HiveID = hiveID
	ledger.append(row)

	observedAt := row.ObservedAt
	hive.LastInspectionAt = &observedAt
	m.hives[hiveID] = hive
	return row, nil
}

// AddQueen 关闭原有产卵蜂王并写入新蜂王，两件事在同一把锁内完成，
// 保证任意时刻同一蜂箱至多只有一只 laying 蜂王。
func (m *Memory) AddQueen(
	hiveID string,
	queen domain.Queen,
	closeQueenID string,
	closeState domain.QueenState,
	now time.Time,
) (domain.Queen, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hive, ok := m.hives[hiveID]
	if !ok {
		return domain.Queen{}, domain.NotFound("蜂箱", hiveID)
	}
	if !hive.AcceptsQueenChange() {
		return domain.Queen{}, domain.InvalidState("蜂箱 %q 处于 %q 状态，不接受换王", hiveID, hive.State)
	}
	if closeQueenID != "" {
		if err := m.closeQueenLocked(hiveID, closeQueenID, closeState, now); err != nil {
			return domain.Queen{}, err
		}
	}
	m.queenSeq++
	queen.ID = fmt.Sprintf("queen-%06d", m.queenSeq)
	queen.HiveID = hiveID
	queen.State = domain.QueenLaying
	m.queens[hiveID] = append(m.queens[hiveID], queen)

	hive.ActiveQueenID = queen.ID
	m.hives[hiveID] = hive
	return queen, nil
}

func (m *Memory) closeQueenLocked(hiveID, queenID string, state domain.QueenState, now time.Time) error {
	items := m.queens[hiveID]
	for index, item := range items {
		if item.ID != queenID {
			continue
		}
		if !item.IsLaying() {
			return domain.Conflict("蜂王 %q 已经关闭", queenID)
		}
		closedAt := now.UTC()
		item.State = state
		item.ClosedAt = &closedAt
		items[index] = item
		m.queens[hiveID] = items
		return nil
	}
	return domain.Storage("蜂箱 %q 上的蜂王 %q 记录缺失", hiveID, queenID)
}

func (m *Memory) ListInspections(hiveID string) ([]domain.Inspection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.hives[hiveID]; !ok {
		return nil, domain.NotFound("蜂箱", hiveID)
	}
	ledger, ok := m.inspectionLedger[hiveID]
	if !ok {
		return nil, nil
	}
	return ledger.list(), nil
}

func (m *Memory) ListQueens(hiveID string) ([]domain.Queen, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.hives[hiveID]; !ok {
		return nil, domain.NotFound("蜂箱", hiveID)
	}
	items := m.queens[hiveID]
	out := make([]domain.Queen, len(items))
	copy(out, items)
	return out, nil
}

// Snapshot 返回深拷贝的一致视图，供汇总类读取使用。
func (m *Memory) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snapshot := Snapshot{
		Apiaries:               make(map[string]domain.Apiary, len(m.apiaries)),
		Hives:                  make(map[string]domain.Hive, len(m.hives)),
		InspectionsByHive:      make(map[string][]domain.Inspection, len(m.inspectionLedger)),
		LatestInspectionByHive: make(map[string]string, len(m.inspectionLedger)),
		QueensByHive:           make(map[string][]domain.Queen, len(m.queens)),
	}
	for id, apiary := range m.apiaries {
		snapshot.Apiaries[id] = apiary
	}
	for id, hive := range m.hives {
		snapshot.Hives[id] = cloneHive(hive)
	}
	for id, ledger := range m.inspectionLedger {
		snapshot.InspectionsByHive[id] = cloneInspections(ledger.list())
		if ledger.latestID != "" {
			snapshot.LatestInspectionByHive[id] = ledger.latestID
		}
	}
	for id, items := range m.queens {
		copied := make([]domain.Queen, len(items))
		copy(copied, items)
		for index := range copied {
			if items[index].ClosedAt != nil {
				closedAt := *items[index].ClosedAt
				copied[index].ClosedAt = &closedAt
			}
		}
		snapshot.QueensByHive[id] = copied
	}
	return snapshot
}

func cloneHive(hive domain.Hive) domain.Hive {
	if hive.LastInspectionAt != nil {
		observedAt := *hive.LastInspectionAt
		hive.LastInspectionAt = &observedAt
	}
	return hive
}

func cloneInspections(items []domain.Inspection) []domain.Inspection {
	copied := make([]domain.Inspection, len(items))
	copy(copied, items)
	for index := range copied {
		copied[index].Concerns = append([]string{}, items[index].Concerns...)
	}
	return copied
}
