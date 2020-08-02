package packet

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/shopspring/decimal"
)

type Mode int

const (
	_ Mode = iota
	// 普通红包，平均分配
	Normal
	// 手气红包
	Luck
)

var (
	// 红包已抢光
	ErrExhausted = errors.New("packet: exhausted")

	// 单个红包最小金额
	// 创建红包的时候需要检查平均金额不能小于这个数
	minimumRecordAmount, _ = decimal.NewFromString("0.01")
)

// 红包
type Packet struct {
	ID        int64     `sql:"PRIMARY_KEY" json:"id,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	UserID    int64     `json:"user_id,omitempty"`
	Message   string    `sql:"size:256" json:"message,omitempty"`
	Mode      Mode      `json:"mode,omitempty"`
	// 红包个数
	TotalCount int64 `json:"total_count,omitempty"`
	// 剩余个数
	RemainCount int64 `json:"remain_count,omitempty"`
	// 红包金额
	TotalAmount decimal.Decimal `sql:"type:decimal(10,2)" json:"total_amount,omitempty"`
	// 剩余金额
	RemainAmount decimal.Decimal `sql:"type:decimal(10,2)" json:"remain_amount,omitempty"`
}

// 领取红包记录
type Record struct {
	ID        int64     `sql:"PRIMARY_KEY" json:"id,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	// user_id + packet_id 需要加 unique 组合索引
	UserID   int64 `json:"user_id,omitempty"`
	PacketID int64 `json:"packet_id,omitempty"`
	// 抢到的金额
	Amount decimal.Decimal `sql:"type:decimal(10,2)" json:"amount,omitempty"`
}

func FindPacket(db *gorm.DB, id int64) (*Packet, error) {
	var packet Packet
	if err := db.Where("id = ?", id).First(&packet).Error; err != nil {
		return nil, err
	}

	return &packet, nil
}

func FindUserRecord(db *gorm.DB, userID, packetID int64) (*Record, error) {
	var record Record
	if err := db.Where("user_id = ? AND packet_id = ?", userID, packetID).First(&record).Error; err != nil {
		return nil, err
	}

	return &record, nil
}

func Claim(ctx context.Context, db *gorm.DB, packet *Packet, userID int64) (*Record, error) {
	// 检查剩余个数
	if packet.RemainCount == 0 {
		return nil, ErrExhausted
	}

	// 检查是否已经抢过了
	if r, err := FindUserRecord(db, userID, packet.ID); err == nil {
		return r, nil
	}

	r := &Record{
		UserID:   userID,
		PacketID: packet.ID,
	}

	switch {
	case packet.RemainCount == 1: // 最后一个包
		r.Amount = packet.RemainAmount
	case packet.Mode == Normal: // 平均分配
		r.Amount = packet.RemainAmount.Div(decimal.NewFromInt(packet.RemainCount))
	case packet.Mode == Luck:
		// 手气红包，在最小值和剩余平均值 * 2 之间随机选取
		// 要注意最大值，需要至少给剩下的人留最小值
		min := minimumRecordAmount
		max := packet.RemainAmount.Sub(decimal.NewFromInt(packet.RemainCount - 1).Mul(min))
		if avg := packet.RemainAmount.Div(decimal.NewFromInt(packet.RemainCount)); avg.Add(avg).LessThan(max) {
			max = avg.Add(avg)
		}

		random := decimal.NewFromFloat(rand.Float64())
		r.Amount = max.Sub(min).Mul(random).Add(min).Truncate(min.Exponent())
	}

	packet.RemainAmount = packet.RemainAmount.Sub(r.Amount)
	if err := transaction(db, func(tx *gorm.DB) error {
		updates := map[string]interface{}{
			"remain_count":  packet.RemainCount - 1,
			"remain_amount": packet.RemainAmount,
		}

		// 这里在更新 packet 的时候在 Where 加了剩余个数的判断
		// 如果这个个数的红包已经被别人抢了，这里会更新失败, RowsAffected 返回 0
		if tx := tx.Model(packet).Where("id = ? AND remain_count = ?", packet.ID, packet.RemainCount).Updates(updates); tx.Error != nil {
			return tx.Error
		} else if tx.RowsAffected == 0 {
			return ErrOptimisticLock
		}

		// packet 更新成功，将记录入库
		return tx.Create(r).Error
	}); err != nil {
		// 被别人抢了，等待 50ms 继续抢
		if err == ErrOptimisticLock {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
				// 获取最新的 packet
				packet, err := FindPacket(db, packet.ID)
				if err != nil {
					return nil, err
				}

				// 继续抢
				return Claim(ctx, db, packet, userID)
			}
		}

		return nil, err
	}

	return r, nil
}

var ErrOptimisticLock = errors.New("optimistic Lock Error")

func transaction(db *gorm.DB, fn func(tx *gorm.DB) error) error {
	tx := db.Begin()
	defer tx.RollbackUnlessCommitted()

	return fn(tx)
}
