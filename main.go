package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/kpgx"
)

var UsersTable = ksql.NewTable("users", "id")

type User struct {
	ID        int       `ksql:"id"`
	Name      string    `ksql:"name"`
	Age       int       `ksql:"age"`
	CreatedAt time.Time `ksql:"created_at"`
}

func main() {
	ctx := context.Background()

	db, err := kpgx.New(ctx, os.Getenv("POSTGRES_URL"), ksql.Config{})
	if err != nil {
		log.Fatalf("unable to connect to database using URL: %s, error: %s", os.Getenv("POSTGRES_URL"), err)
	}

	// Create any necessary tables if they don't exist yet
	setupDB(ctx, db)

	var user User
	err = db.QueryOne(ctx, &user, "FROM users WHERE name='Mary'")
	if err == ksql.ErrRecordNotFound {
		user = User{
			Name:      "Mary",
			Age:       17,
			CreatedAt: time.Now().UTC(),
		}
		err := db.Insert(ctx, UsersTable, &user)
		if err != nil {
			log.Fatalf("unable to insert a new user into the database: %s", err)
		}
	} else if err != nil {
		log.Fatalf("unexpected error when searching for user 'Mary': %s", err)
	}

	fmt.Printf("Mary's ID is: %d\n", user.ID)
	fmt.Printf("Mary's age is: %d\n", user.Age)

	// Mary got older:
	user.Age++
	err = db.Update(ctx, UsersTable, &user)
	if err != nil {
		log.Fatalf("unable to update Mary's age: %s", err)
	}
}

func setupDB(ctx context.Context, db ksql.Provider) {
	db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users(
		id serial PRIMARY KEY,
		name varchar,
		age integer,
		created_at timestamptz NOT NULL DEFAULT NOW()
	)`)
}
