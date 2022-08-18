package testdata

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomelon/melon/data"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

func TestCRUD(t *testing.T) {
	a := assert.New(t)
	// prepare
	tm, closeFunc := tm()
	defer closeFunc()
	var userDao UserDao = NewUserDaoSQLImpl(tm)

	// execute
	var err error
	ctx := context.Background()
	user1 := &User{
		Name:     "GoMelon1",
		Gender:   0,
		Birthday: time.Now(),
	}

	user1, err = userDao.Insert(ctx, user1)

	user2 := &User{
		Name:     "GoMelon2",
		Gender:   0,
		Birthday: time.Now(),
	}
	user2, err = userDao.Insert(ctx, user2)
	a.NoError(err, "Insert fail")
	a.Greater(user1.Id, int64(0), "insert then get id fail")

	// -------------------------- Find Single Start --------------------------

	foundUser, err := userDao.FindById(ctx, user1.Id)
	a.NoError(err, "FindById fail")
	a.NotNil(foundUser, "FindById fail")
	a.NotEmptyf(foundUser.Name, "FindById fail,Name is Empty")

	foundUser, err = userDao.FindById(ctx, math.MaxInt64)
	a.NoError(err, "FindById fail")
	a.Nil(foundUser, "FindById fail")

	foundUser, err = userDao.FindById2(ctx, user1.Id)
	a.NoError(err, "FindById2 fail")
	a.NotNil(foundUser, "FindById2 fail")
	a.NotEmptyf(foundUser.Name, "FindById2 fail,Name is Empty")

	foundUser, err = userDao.FindById2(ctx, math.MaxInt64)
	a.NoError(err, "FindById2 fail")
	a.Nil(foundUser, "FindById2 fail")

	// -------------------------- Find Single End   --------------------------

	// -------------------------- Find Slice Start --------------------------

	foundUsers, err := userDao.FindByBirthdayGTE(ctx, time.Now().Add(-1*time.Hour))
	a.NoError(err, "FindByBirthdayGTE fail")
	a.NotEmpty(foundUsers, "FindByBirthdayGTE fail")
	a.NotEmptyf(foundUsers[0].Name, "FindByBirthdayGTE fail,Name is Empty")

	foundUsers, err = userDao.FindByBirthdayGTE(ctx, time.Now().Add(1*time.Hour))
	a.NoError(err, "FindByBirthdayGTE fail")
	a.Empty(foundUser, "FindByBirthdayGTE fail")

	foundUsers, err = userDao.FindByBirthdayGTE2(ctx, time.Now().Add(-1*time.Hour))
	a.NoError(err, "FindByBirthdayGTE2 fail")
	a.NotEmpty(foundUsers, "FindByBirthdayGTE2 fail")
	a.NotEmptyf(foundUsers[0].Name, "FindByBirthdayGTE2 fail,Name is Empty")

	foundUsers, err = userDao.FindByBirthdayGTE2(ctx, time.Now().Add(1*time.Hour))
	a.NoError(err, "FindByBirthdayGTE2 fail")
	a.Empty(foundUsers, "FindByBirthdayGTE2 fail")

	// -------------------------- Find Slice End   --------------------------

	// -------------------------- Exists Start --------------------------

	found, err := userDao.ExistsById(ctx, user1.Id)
	a.NoError(err, "ExistsById fail")
	a.True(found, "ExistsById fail")

	found, err = userDao.ExistsById(ctx, math.MaxInt64)
	a.NoError(err, "ExistsById fail")
	a.False(found, "ExistsById fail")

	found, err = userDao.ExistsById2(ctx, user1.Id)
	a.NoError(err, "ExistsById2 fail")
	a.True(found, "ExistsById2 fail")

	found, err = userDao.ExistsById2(ctx, math.MaxInt64)
	a.NoError(err, "ExistsById2 fail")
	a.False(found, "ExistsById2 fail")

	// -------------------------- Exists End   --------------------------

	// -------------------------- Count Start --------------------------

	count, err := userDao.CountByBirthdayGTE(ctx, time.Now().Add(-1*time.Hour))
	a.NoError(err, "CountByBirthdayGTE fail")
	a.Greater(count, 0, "CountByBirthdayGTE fail")

	count, err = userDao.CountByBirthdayGTE(ctx, time.Now().Add(1*time.Hour))
	a.NoError(err, "CountByBirthdayGTE fail")
	a.Equal(count, 0, "CountByBirthdayGTE fail")

	count, err = userDao.CountByBirthdayGTE2(ctx, time.Now().Add(-1*time.Hour))
	a.NoError(err, "CountByBirthdayGTE2 fail")
	a.Greater(count, 0, "CountByBirthdayGTE2 fail")

	count, err = userDao.CountByBirthdayGTE2(ctx, time.Now().Add(1*time.Hour))
	a.NoError(err, "CountByBirthdayGTE2 fail")
	a.Equal(count, 0, "CountByBirthdayGTE2 fail")

	// -------------------------- Count End   --------------------------

	// -------------------------- Delete Start --------------------------
	deleteCount, err := userDao.DeleteById(ctx, user1.Id)
	a.NoError(err, "DeleteById fail")
	a.Greater(deleteCount, int64(0), "DeleteById fail")

	deleteCount, err = userDao.DeleteById(ctx, math.MaxInt64)
	a.NoError(err, "DeleteById fail")
	a.Equal(deleteCount, int64(0), "DeleteById fail")

	deleteCount, err = userDao.DeleteById2(ctx, user2.Id)
	a.NoError(err, "DeleteById2 fail")
	a.Greater(deleteCount, int64(0), "DeleteById2 fail")

	deleteCount, err = userDao.DeleteById2(ctx, math.MaxInt64)
	a.NoError(err, "DeleteById2 fail")
	a.Equal(deleteCount, int64(0), "DeleteById2 fail")
	// -------------------------- Delete End   --------------------------
}

func TestTransactionRollback(t *testing.T) {
	// prepare
	tm, closeFunc := tm()
	defer closeFunc()
	var userDao UserDao = NewUserDaoSQLImpl(tm)

	// execute
	ctx := context.Background()
	newCtx, err := tm.Begin(ctx, nil)
	if err != nil {
		panic(err)
	}
	user := &User{
		Name:     "GoMelon",
		Gender:   0,
		Birthday: time.Now(),
	}
	user, err = userDao.Insert(newCtx, user)
	if err != nil {
		panic(err)
	}
	if err != nil {
		return
	}
	tm.Rollback(newCtx)

	user, err = userDao.FindById(ctx, user.Id)
	if err != nil {
		panic(err)
	}

	if user == nil {
		fmt.Println("User Not Found")
	} else {
		fmt.Println("Find User ID:", user.Id, ",Name:", user.Name)
		userDao.DeleteById(ctx, user.Id)
	}
}

func tm() (tm *data.SQLTXManager, closeFunc func()) {
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/user?charset=utf8&parseTime=True")
	if err != nil {
		panic(err)
	}

	tm = data.NewSqlTxManager("user", db)
	closeFunc = func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}
	return
}

func (_impl *UserDaoSQLImpl) Insert(ctx context.Context, user *User) (*User, error) {
	query := "INSERT INTO `user`(`name`,`gender`,`birthday`)" +
		"VALUES (?, ?, ?)"
	db := _impl._tm.OriginTXOrDB(ctx)
	result, err := db.Exec(query, user.Name, user.Gender, user.Birthday)
	if err != nil {
		return nil, err
	}
	id, err := result.LastInsertId()
	user.Id = id
	return user, err
}

func (_impl *UserDaoSQLImpl) UpdateById(ctx context.Context, id int64, user *User) (int64, error) {
	query := "UPDATE `user` SET `name`= ?, `gender` = ?, `birthday` = ?, `created_at` = ? " +
		"WHERE `id` = ?"
	db := _impl._tm.OriginTXOrDB(ctx)
	result, _ := db.Exec(query, user.Name, user.Gender, user.Birthday, id)
	return result.RowsAffected()
}
