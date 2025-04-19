package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"

	"github.com/abstratium-informatique-sarl/abstrastore/pkg/minio"
	"github.com/abstratium-informatique-sarl/abstrastore/pkg/reader"
	"github.com/abstratium-informatique-sarl/abstrastore/pkg/writer"
)

func TestReadWrite(t *testing.T) {
	assert := assert.New(t)
	godotenv.Load("/w/abstratium-abstrastore.env")
	minio.Setup()
	id := uuid.New().String()
	schema := "integration-tests"
	start := time.Now()
	table := fmt.Sprintf("table-%d", start.UnixMilli())

	err := minio.GetRepository().DeleteFolder(context.Background(), schema)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Write", func(t *testing.T) {
		entity := TestEntity{
			Id:   id,
			Name: "John Smith",
			Height: 180,
			Address: &TestEntityAddress{
				Street: "123 Main St",
				City: "Anytown",
				ZipCode: "12345",
			},
		}
		if err := writer.Append(context.Background(), schema, table, id, entity); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("Write after %v\n", time.Since(start))
	})

	t.Run("Write again", func(t *testing.T) {
		entity := TestEntity{
			Id:   id,
			Name: "John Doe",
			Height: 180,
			Address: &TestEntityAddress{
				Street: "124 Main St",
				City: "Anytown",
				ZipCode: "12346",
			},
		}
		if err := writer.Append(context.Background(), schema, table, id, entity); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("Second write after %v\n", time.Since(start))
	})

	t.Run("Read", func(t *testing.T) {
		entity := TestEntity{}
		if err := reader.ReadById(context.Background(), schema, table, id, &entity); err != nil {
			t.Fatal(err)
		}

		fmt.Printf("Read after %v\n", time.Since(start))

		assert.Equal("John Doe", entity.Name)
	})
}

type TestEntity struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Height int `json:"value"`
	Address *TestEntityAddress `json:"address"`
}

type TestEntityAddress struct {
	Street string `json:"street"`
	City string `json:"city"`
	ZipCode string `json:"zipCode"`
}
