package main

// User represents user info.
type User struct {
	Login      string `json:"login"`
	Email      string `json:"email"`
	Password   string `json:"password"`
	FirstName  string `json:"firstName"`
	LastName   string `json:"lastName"`
	Phone      string `json:"phone"`
	Registered int64  `json:"registered"`
}
