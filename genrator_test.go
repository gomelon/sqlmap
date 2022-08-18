package sqlmap

import (
	"fmt"
	"github.com/gomelon/melon/data/engine"
	"github.com/gomelon/meta"
	"os"
	"testing"
	"text/template"
)

func TestTemplateGen(t *testing.T) {

	workdir, _ := os.Getwd()
	path := workdir + "/testdata"
	generator, err := meta.NewTmplPkgGen(path, TmplSQL, meta.WithOutputFilename("sql_dao"),
		meta.WithFuncMapFactory(func(generator *meta.TmplPkgGen) template.FuncMap {
			return NewFunctions(generator, engine.NewMySQL()).FuncMap()
		}))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	//err = generator.Print()
	err = generator.Generate()
	//err = generator.Generate()
	if err != nil {
		fmt.Println(err.Error())
	}
}
