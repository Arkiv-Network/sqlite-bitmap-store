package sqlitebitmapstore_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
)

func TestSqlitestore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sqlitestore Suite", types.ReporterConfig{NoColor: true})
}
