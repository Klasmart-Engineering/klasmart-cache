module gitlab.badanamu.com.cn/calmisland/kidsloop-cache

go 1.16

require (
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/jinzhu/gorm v1.9.12
	gitlab.badanamu.com.cn/calmisland/common-log v0.1.4
	gitlab.badanamu.com.cn/calmisland/dbo v0.1.10
	gitlab.badanamu.com.cn/calmisland/ro v0.0.0-20200819092854-7b96095e0678
	gorm.io/driver/mysql v1.1.2
)

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4
	google.golang.org/grpc => google.golang.org/grpc v1.26.0

)
