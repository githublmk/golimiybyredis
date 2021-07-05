package util

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	gouuid "github.com/satori/go.uuid"
	"strconv"
	"time"
)

//redis key

var RuleType = ruleType{
	QPS:             1,
	AbnormalRatio:   2,
	ContinuousError: 3,
}

//规则结构体
type ruleType struct {
	QPS             int //qps
	AbnormalRatio   int //异常比例
	ContinuousError int //连续错误
}

//规则是否是结构体中的值
func (r ruleType) isRuleTypeMember(member int) bool {
	switch member {
	case 1, 2, 3:
		return true
	default:
		return false
	}
}

const (
	EXPIRETIME   = "expiretime"   //统计时间
	LIMITTYPE    = "limittype"    //规则方式，2 异常比例、1 QPS、3 单位时间连续异常
	CRITICAL     = "critical"     //临界值
	SLEEPTIME    = "sleeptime"    //恢复时间（窗口期）
	STARTCOUNT   = "startcount"   //达到多少请求开始统计
	RECOVERCOUNT = "recovercount" //半开可以进入的请求数
	HALFTIME     = "halftime"     //半开的窗口时间
	HALFCRITICAL = "halfcritical" //恢复比例

	ISPASS   = "ispass"   //是否达到统计值
	ERRCOUNT = "errcount" //错误数
	COUNT    = "count"    //总数

	HALFERRORCOUNT   = "halferrorcount"   //请求错误的数目
	HALFCORRECTCOUNT = "halfcorrectcount" //请求正确数目
	HALFCOUNT        = "halfcount"        //当前请求数目

)

//服务结构体
type Service struct {
	prefix         string //前缀
	suffix         string //后缀
	halfOpenSetKey string //半开集合key
	redisClient    *redis.ClusterClient
	hashKey        string // 存取资源key
}

//限流处理结构体
type Pass struct {
	service   *Service
	resource  string //资源
	limitType int    //规则类型
	count     int    //可以调用次数
	sid       string //唯一标识

}

//创建限流服务
func NewService(prefix string, suffix string, setKey string, hashKey string, redisClient *redis.ClusterClient) (*Service, error) {
	if redisClient == nil {
		return nil, errors.New("redisClusterClient is nil")
	}
	err := redisClient.Ping().Err()

	if err != nil {
		return nil, err
	}
	s := &Service{
		prefix:         prefix,
		suffix:         suffix,
		halfOpenSetKey: setKey,
		redisClient:    redisClient,
		hashKey:        hashKey,
	}
	return s, nil

}

//注册资源
func (s *Service) AddResource(resource string, expireTime int, limitType int, critical float64, sleepTime int, startCount int, recoverCount int, halfTime int, halfCritical float64) error {
	if !RuleType.isRuleTypeMember(limitType) {
		return errors.New("该规则类型不存在")
	}

	resourceHashKey := s.resourceHashKey(resource)
	info := make(map[string]interface{}, 10)
	info[EXPIRETIME] = expireTime
	info[LIMITTYPE] = limitType
	info[CRITICAL] = critical
	info[SLEEPTIME] = sleepTime
	info[STARTCOUNT] = startCount
	info[RECOVERCOUNT] = recoverCount
	info[HALFTIME] = halfTime
	info[HALFCRITICAL] = halfCritical

	set := s.redisClient.HMSet(resourceHashKey, info)
	result, err := set.Result()
	fmt.Println(result)
	fmt.Println(err)
	return err

}

//注册QPS资源
func (s *Service) AddQPS(resource string, expireTime int, critical int) error {
	return s.AddResource(resource, expireTime, RuleType.QPS, float64(critical), 0, 0, 0, 0, 0)
}

//注册异常比例资源
func (s *Service) AddAbnormalRatio(resource string, expireTime int, critical float64, sleepTime int, startCount int, recoverCount int, halfTime int, halfCritical float64) error {
	return s.AddResource(resource, expireTime, RuleType.AbnormalRatio, critical, sleepTime, startCount, recoverCount, halfTime, halfCritical)
}

//注册连续错误资源
func (s *Service) AddContinuousError(resource string, expireTime int, critical int, sleepTime int, recoverCount int, halfTime int, halfCritical float64) error {
	return s.AddResource(resource, expireTime, RuleType.ContinuousError, float64(critical), sleepTime, 1, recoverCount, halfTime, halfCritical)
}

//是否限流，true 为限流，false 没有限流
func (s *Service) Take(resource string) (*Pass, int, string, bool, error) {
	sid := uuid()
	limitType, isLimit, err := s.isLimit(resource)
	if err != nil {
		return nil, limitType, sid, false, err
	}

	switch limitType {
	case 1:
		if isLimit {
			return nil, limitType, sid, true, nil
		} else {
			return nil, limitType, sid, false, nil
		}
	default:
		if isLimit {
			return nil, limitType, sid, true, nil
		} else {
			p := &Pass{
				service:   s,
				resource:  resource,
				limitType: limitType,
				count:     1,
				sid:       sid,
			}
			return p, limitType, sid, false, nil
		}

	}

}

//加锁 放几个请求，其他的按熔断处理
//判断资源是否限流
//判断资源的限流方式
//1.限流 2.异常比例 3 连续错误
func (s *Service) isLimit(resource string) (int, bool, error) {

	var err error
	resourceKey := s.resourceKey(resource)
	resourceHashKey := s.resourceHashKey(resource)
	halfOpenKey := s.halfOpenKey()
	halfRecoverKey := s.resourceHalfRecoverKey(resource)

	lockKey := s.resourceLockKey(resource)
	passLockKey:=s.resourcePassLockKey(resource)
	i:=0
	for {
		i=1
		exists ,err1:= s.redisClient.Exists(passLockKey).Result()
		if exists==0&&err1==nil{
			nx := s.redisClient.SetNX(lockKey, "1", 100*time.Millisecond).Val()
			if nx{
				fmt.Println("take lock")
				break
			}
		}

		if i>100{
			return 0, true, errors.New("无法抢到锁")
		}
		i++
		time.Sleep(time.Millisecond)
	}
	defer s.redisClient.Del(lockKey)


	//获取资源的相关设置
	info, err := s.redisClient.HGetAll(resourceHashKey).Result()
	if len(info) != 8 {
		errMes:=""
		fmt.Println(info)
		fmt.Println(err)
		if err!=nil{
			errMes=","+err.Error()
		}
		return 0, false, errors.New(resourceHashKey + "值缺失"+errMes)
	}
	//获取资源信息
	resourceInfo := s.redisClient.HGetAll(resourceKey).Val()
	expireTime, err := strconv.Atoi(info[EXPIRETIME])
	halfTime, err := strconv.Atoi(info[HALFTIME])
	limitType, err := strconv.Atoi(info[LIMITTYPE])
	critical, err := strconv.Atoi(info[CRITICAL])
	recoverCount, err := strconv.Atoi(info[RECOVERCOUNT])
	count := 0
	//是否限流
	isPass := "false"
	if len(resourceInfo) != 0 {
		count, err = strconv.Atoi(resourceInfo[COUNT])
		fmt.Println("count",count)
		isPass = resourceInfo[ISPASS]
	}
	//处理转换错误
	if err != nil {
		return 0, false, err
	}

	//处理不同的限流方式
	if limitType == 1 {
		// QPS 方式
		// 资源不存在，设置
		if len(resourceInfo) == 0 {
			s.redisClient.HMSet(resourceKey, map[string]interface{}{
				ISPASS: "false",
				COUNT:1,
				ERRCOUNT: 0,
			})
			s.redisClient.Expire(resourceKey, time.Duration(expireTime)*time.Second)
			return limitType, false, err
		} else {
			if (count+1) > critical {
				return limitType, true, err
			} else {
				s.redisClient.HIncrBy(resourceKey, COUNT, 1)
				return limitType, false, err
			}
		}
	} else {
		//处理 异常比例、连续错误方式
		//ispass 标志为 true 表示资源正处于限流状态
		if isPass == "true" {
			return limitType, true, err
		}
		// 资源是否在半开集合
		isMember := s.redisClient.SIsMember(halfOpenKey, resourceKey).Val()
		// 获取资源的探测信息
		recoverInfo := s.redisClient.HGetAll(halfRecoverKey).Val()
		//在半开集合中，进行探测
		if isMember {
			//没有探测信息
			fmt.Println("take  --------------------")
			if len(recoverInfo) == 0 {
				//探测数量设置为零 限流
				if 1 > recoverCount {
					return limitType, true, err
				} else {
					//设置探测信息
					fmt.Println("take  ----------Mset----------")
					s.redisClient.HMSet(halfRecoverKey, map[string]interface{}{
						HALFERRORCOUNT:   0,
						HALFCORRECTCOUNT: 0,
						HALFCOUNT:        1,
					})
					s.redisClient.Expire(halfRecoverKey, time.Duration(halfTime)*time.Second)
					return limitType, false, err
				}
			} else {
				//有探测信息
				halfCount, err := strconv.Atoi(recoverInfo[HALFCOUNT])
				//探测数目大于设置数目 限流
				if (halfCount+1) > recoverCount {
					return limitType, true, err
				} else {
					fmt.Println("take  ----------Mset--+111--------")
					//探测数目+1
					s.redisClient.HIncrBy(halfRecoverKey, HALFCOUNT, 1)
					return limitType, false, err
				}
			}

		} else {
			//不在半开集合 一定不会处于限流状态
			return limitType, false, err
		}

	}

}

//获取资源的redis key
func (s *Service) resourceKey(resource string) string {
	return s.prefix + "_" + resource + "_" + s.suffix
}

//获取保存资源的过期时间、熔断值、熔断方式的rediskey
func (s *Service) resourceHashKey(resource string) string {
	return s.prefix + "_" + s.hashKey + "_" + resource + "_" + s.suffix
}

//获取半开集合在redis中key
func (s *Service) halfOpenKey() string {
	return s.prefix + "_" + s.halfOpenSetKey + "_" + s.suffix
}


//资源探测key
func (s *Service) resourceHalfRecoverKey(resource string) string {
	return s.prefix + "_" + "halfrecover" + "_" + resource + "_" + s.suffix
}

//保存资源信息的hash是否存在
func (s *Service) IsExistsResourceHashKey(resource string) (bool, error) {
	key := s.resourceHashKey(resource)

	result, err := s.redisClient.Exists(key).Result()
	if err != nil {
		return false, err
	}
	if result == 1 {
		return true, err
	} else {
		return false, err
	}

}

//生成uuid
func uuid() string {
	var err error
	s := gouuid.Must(gouuid.NewV4(), err).String()
	return fmt.Sprint(time.Now().Local().UnixNano(), "-", s)
}

//生成资源的分布式锁
func (s *Service) resourceLockKey(resource string) string {
	return s.prefix + "_" + "lock" + "_" + resource + "_" + s.suffix
}

func (s *Service) resourcePassLockKey(resource string) string {
	return s.prefix + "_" + "passlock" + "_" + resource + "_" + s.suffix
}

//对资源进行熔断或恢复处理
func (p *Pass) Pass(isPass bool) error {
	var err error
	//该方法只能调用一次
	if p.count != 1 {
		return errors.New("请勿重复反馈")
	}
	p.count = p.count - 1
	//通过，考虑是否恢复
	resourceKey := p.service.resourceKey(p.resource)
	resourceHashKey := p.service.resourceHashKey(p.resource)
	halfOpenKey := p.service.halfOpenKey()
	resourceHalfRecoverKey := p.service.resourceHalfRecoverKey(p.resource)
	//不处理QPS方式
	if p.limitType == 1 {
		return nil
	}

	lockKey := p.service.resourcePassLockKey(p.resource)
	i:=0
	for {
		i=1
		nx := p.service.redisClient.SetNX(lockKey, "1", 500*time.Millisecond).Val()
		if nx{
			fmt.Println("pass lock")
			break
		}
		if i>30{
			return  errors.New("无法抢到锁")
		}
		i++
		time.Sleep(time.Millisecond)
	}
	defer p.service.redisClient.Del(lockKey)



	//资源设置信息
	info, err := p.service.redisClient.HGetAll(resourceHashKey).Result()
	if len(info) != 8 {
		errMes:=""
		if err!=nil{
			errMes=err.Error()
		}
		return  errors.New(resourceHashKey + "值缺失,"+errMes)
	}


	//探测信息
	recoverInfo ,err:= p.service.redisClient.HGetAll(resourceHalfRecoverKey).Result()
	fmt.Println("recoverInfo:",recoverInfo,"   err:",err)
	expireTime, err := strconv.Atoi(info[EXPIRETIME])
	startCount, err := strconv.Atoi(info[STARTCOUNT])
	criticalInt, err := strconv.Atoi(info[CRITICAL])
	sleepTime, err := strconv.Atoi(info[SLEEPTIME])
	criticalFloat, err := strconv.ParseFloat(info[CRITICAL], 64)
	halfCritical, err := strconv.ParseFloat(info[HALFCRITICAL], 64)
	recoverCountFloat, err := strconv.ParseFloat(info[RECOVERCOUNT], 64)


	//资源信息
	resourceInfo := p.service.redisClient.HGetAll(resourceKey).Val()
	fmt.Println("------------------",resourceInfo)
	if len(resourceInfo) >0 && resourceInfo[ISPASS]=="true"{
		return nil

	}
	count := 0
	if len(resourceInfo) != 0 {
		count, err = strconv.Atoi(resourceInfo[COUNT])
		if err !=nil{
			fmt.Println("count",err)
		}

	}


	//处理转换错误
	if err != nil {
		return err
	}



	//业务成功
	if isPass {
		//没有探测信息
		if len(recoverInfo) == 0 {
			//没有资源信息
			if len(resourceInfo) == 0 {
				//设置资源信息
				p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
					ERRCOUNT: 0,
					COUNT:    1,
					ISPASS:   "false",
				})
				p.service.redisClient.Expire(resourceKey, time.Duration(expireTime)*time.Second)
			} else {
				//有资源信息
				//总数+1
				p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
				//连续异常次数置为0
				if p.limitType == 3 {
					p.service.redisClient.HSet(resourceKey, ERRCOUNT, 0)
				}

			}
		} else {
			//有探测的信息
			//判断是否达到恢复的条件
			correctCount, err := strconv.ParseFloat(recoverInfo[HALFCORRECTCOUNT], 64)
			if err != nil {
				return err
			}
			//halfCount,err := strconv.ParseFloat(recoverInfo[HALFCOUNT],64)
			if ((correctCount + 1)/recoverCountFloat) >= halfCritical {

				p.service.redisClient.Del(resourceHalfRecoverKey)
				p.service.redisClient.SRem(halfOpenKey, resourceKey)
			} else {
				p.service.redisClient.HIncrBy(resourceHalfRecoverKey, HALFCORRECTCOUNT, 1)
			}
		}

	} else {

		//业务失败
		//有探测信息
		fmt.Println("recoverinfo  len",len(recoverInfo))
		if len(recoverInfo) != 0 {
			halfErrorCount, err := strconv.ParseFloat(recoverInfo[HALFERRORCOUNT], 64)
			if err != nil {
				return err
			}
			//判断是否继续熔断
			if ((halfErrorCount + 1)/recoverCountFloat) > (1 - halfCritical) {
				p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
					ERRCOUNT: 0,
					COUNT:    0,
					ISPASS:   "true",
				})
				p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
				p.service.redisClient.Del(resourceHalfRecoverKey)
			} else {
				p.service.redisClient.HIncrBy(resourceHalfRecoverKey, HALFERRORCOUNT, 1)
			}

		} else {
			fmt.Println("-------------------------ywf----------------------------")
			//没有探测信息
			//达到计算数目
			if startCount <= (count+1) {
				//没有资源信息，设值资源信息
				if len(resourceInfo) == 0 {
					if p.limitType == 2 {
						if 1 > criticalFloat {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 0,
								COUNT:    0,
								ISPASS:   "true",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 1,
								COUNT:    1,
								ISPASS:   "false",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
						}
					}
					if p.limitType == 3 {
						fmt.Println("======================================")
						fmt.Println(resourceInfo)
						fmt.Println(1 > criticalInt)
						if 1 > criticalInt {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 0,
								COUNT:    0,
								ISPASS:   "true",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
								ERRCOUNT: 1,
								COUNT:    1,
								ISPASS:   "false",
							})
							p.service.redisClient.Expire(resourceKey, time.Duration(expireTime)*time.Second)
						}
					}

				} else {
					fmt.Println("柚子源信息",resourceInfo)
					//有资源信息
					errorCount, err := strconv.ParseFloat(resourceInfo[ERRCOUNT], 64)
					num, err := strconv.ParseFloat(resourceInfo[COUNT], 64)
					if err != nil {
						return err
					}
					//判断是否达到限流条件
					if p.limitType == 2 {
						if ((errorCount + 1)/(num + 1)) > criticalFloat {
							p.service.redisClient.HSet(resourceKey, ISPASS, "true")
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HIncrBy(resourceKey, ERRCOUNT, 1)
							p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
						}
					}
					if p.limitType == 3 {
						fmt.Println("有资源",resourceInfo)
						if (errorCount) > criticalFloat {
							p.service.redisClient.HSet(resourceKey, ISPASS, "true")
							p.service.redisClient.Expire(resourceKey, time.Duration(sleepTime)*time.Second)
							p.service.redisClient.SAdd(halfOpenKey, resourceKey)
						} else {
							p.service.redisClient.HIncrBy(resourceKey, ERRCOUNT, 1)
							p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
						}
					}

				}

			} else {
				//没有达到计算数目
				//没有资源信息，设置资源信息
				fmt.Println("统计")
				if len(resourceInfo) == 0 {
					fmt.Println("统计1")
					p.service.redisClient.HMSet(resourceKey, map[string]interface{}{
						ERRCOUNT: 1,
						COUNT:    1,
						ISPASS:   "false",
					})
					p.service.redisClient.Expire(resourceKey, time.Duration(expireTime)*time.Second)
				} else {
					fmt.Println("统计1、2+1")
					//资源信息的错误数和总数+1
					p.service.redisClient.HIncrBy(resourceKey, ERRCOUNT, 1)
					p.service.redisClient.HIncrBy(resourceKey, COUNT, 1)
				}

			}

		}

	}
	return err
}
func (p *Pass) SID() string {
	return p.sid
}
