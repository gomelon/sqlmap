package testdata

import (
	"context"
	"time"
)

//Gender 性别
type Gender uint8

//User 用户信息
type User struct {
	Id        int64
	Name      string
	Gender    Gender
	Birthday  time.Time
	CreatedAt time.Time
}

//UserDao
//+sqlmap.Mapper Table="user" Dialect="mysql"
type UserDao interface {
	FindById(ctx context.Context, id int64) (*User, error)

	FindByBirthdayGTE(ctx context.Context, time time.Time) ([]*User, error)

	ExistsById(ctx context.Context, id int64) (bool, error)

	CountByBirthdayGTE(ctx context.Context, time time.Time) (int, error)

	//FindById2
	/*+sqlmap.Select Query="select * from `user` where id = :id" Master*/
	FindById2(ctx context.Context, id int64) (*User, error)

	//FindByBirthdayGTE2
	/*+sqlmap.Select Query="select * from `user` where birthday >= :time"*/
	FindByBirthdayGTE2(ctx context.Context, time time.Time) ([]*User, error)

	//ExistsById2
	/*+sqlmap.Select Query="select 1 as X from `user` WHERE id = :id limit 1"*/
	ExistsById2(ctx context.Context, id int64) (bool, error)

	//CountByBirthdayGTE2
	/*+sqlmap.Select Query="select count(*) as count from `user` where birthday >= :time"*/
	CountByBirthdayGTE2(ctx context.Context, time time.Time) (int, error)

	//Insert
	//+sqlmap.None
	Insert(ctx context.Context, user *User) (*User, error)

	//UpdateById
	//+sqlmap.None
	UpdateById(ctx context.Context, id int64, user *User) (int64, error)

	DeleteById(ctx context.Context, id int64) (int64, error)

	//DeleteById2
	/*+sqlmap.Delete Query="delete from `user` where id = :id"*/
	DeleteById2(ctx context.Context, id int64) (int64, error)
}
