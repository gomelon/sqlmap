package testdata

import (
	"context"
	"time"
)

//Gender 性别
type Gender uint8

const (
	//GenderUnknown 性别,未知
	GenderUnknown Gender = iota
	//GenderFemale 性别,女
	GenderFemale
	//GenderMale 性别,男
	GenderMale
)

//User 用户信息
type User struct {
	Id        int64
	Name      string
	Gender    Gender
	Birthday  time.Time
	CreatedAt time.Time
}

//UserDao
//sql:table name=`user` dialect="mysql"
type UserDao interface {
	FindById(ctx context.Context, id int64) (*User, error)

	FindByBirthdayGTE(ctx context.Context, time time.Time) ([]*User, error)

	ExistsById(ctx context.Context, id int64) (bool, error)

	CountByBirthdayGTE(ctx context.Context, time time.Time) (int, error)

	/*sql:select query="select * from `user` where id = :id" master*/
	FindById2(ctx context.Context, id int64) (*User, error)

	/*sql:select query="select * from `user` where birthday >= :time"*/
	FindByBirthdayGTE2(ctx context.Context, time time.Time) ([]*User, error)

	/*sql:select query="select 1 as X from `user` WHERE id = :id limit 1"*/
	ExistsById2(ctx context.Context, id int64) (bool, error)

	/*sql:select query="select count(*) as count from `user` where birthday >= :time"*/
	CountByBirthdayGTE2(ctx context.Context, time time.Time) (int, error)

	//Insert
	//sql:none
	Insert(ctx context.Context, user *User) (*User, error)

	//UpdateById
	//sql:none
	UpdateById(ctx context.Context, id int64, user *User) (int64, error)

	DeleteById(ctx context.Context, id int64) (int64, error)

	/*sql:delete query="delete from `user` where id = :id"*/
	DeleteById2(ctx context.Context, id int64) (int64, error)
}

//AToGender 字符转Gender
func AToGender(a string) Gender {
	switch a {
	case "FEMALE":
		//Don't parse comment
		return GenderFemale
	case "MALE":
		return GenderMale
	default:
		return GenderUnknown
	}
}
