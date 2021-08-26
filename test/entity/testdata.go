package entity

type RecordA struct {
	ID          string `gorm:"type:varchar(50);PRIMARY_KEY"`
	Name        string `gorm:"type:varchar(255);NOT NULL;column:name"`
	Keywords    string `gorm:"type:text;NOT NULL;column:keywords"`
	Description string `gorm:"type:text;NOT NULL;column:description"`

	BID string `gorm:"type:varchar(50); column:bid"`
	CID string `gorm:"type:varchar(50); column:cid"`
	DID string `gorm:"type:varchar(50); column:did"`
}

func (a RecordA) TableName() string {
	return "record_a"
}

type RecordB struct {
	ID   string `gorm:"type:varchar(50);PRIMARY_KEY"`
	Name string `gorm:"type:varchar(255);NOT NULL;column:name"`
	DID  string `gorm:"type:varchar(50); column:did"`
}

func (a RecordB) TableName() string {
	return "record_b"
}

type RecordC struct {
	ID       string `gorm:"type:varchar(50);PRIMARY_KEY"`
	Name     string `gorm:"type:varchar(255);NOT NULL;"`
	RealName string `gorm:"type:varchar(255);NOT NULL;"`
}

func (a RecordC) TableName() string {
	return "record_c"
}

type RecordD struct {
	ID      string `gorm:"type:varchar(50);PRIMARY_KEY"`
	Title   string `gorm:"type:varchar(255);NOT NULL;"`
	Content string `gorm:"type:varchar(255);NOT NULL;"`
	EID     string `gorm:"type:varchar(50); column:eid"`
}

func (a RecordD) TableName() string {
	return "record_d"
}

type RecordE struct {
	ID      string `gorm:"type:varchar(50);PRIMARY_KEY"`
	Title   string `gorm:"type:varchar(255);NOT NULL;"`
	Content string `gorm:"type:varchar(255);NOT NULL;"`
}

func (a RecordE) TableName() string {
	return "record_e"
}
